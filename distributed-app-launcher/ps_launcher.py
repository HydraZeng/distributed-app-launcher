import os
import sys
import uuid
import warnings
from argparse import REMAINDER, ArgumentParser

import concurrent.futures

import paramiko

servers = []
workers = []
client_dict = {}
training_script = ""

def run_command(instance, client, cmd, environment=None, inputs=None):
    stdin, stdout, stderr = client.exec_command(
        cmd, get_pty=True, environment=environment
    )
    if inputs:
        for inp in inputs:
            stdin.write(inp)
    
    def read_lines(fin, fout, line_head):
        line = ""
        while not fin.channel.exit_status_ready():
            line += fin.read(1).decode("utf8")
            if line.endswith("\n"):
                print(f"{line_head}{line[:-1]}", file=fout)
                line = ""
        if line:
            # print what remains in line buffer, in case fout does not
            # end with '\n'
            print(f"{line_head}{line[:-1]}", file=fout)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as printer:
        printer.submit(read_lines, stdout, sys.stdout, f"[{instance} STDOUT] ")
        printer.submit(read_lines, stderr, sys.stderr, f"[{instance} STDERR] ")

def connect_to_instance(instance, keypath, username, http_proxy=None):
    print(f"Connecting to {instance}...")
    if http_proxy:
        # paramiko.ProxyCommand does not do string substitution for %h %p,
        # so 'nc --proxy-type http --proxy fwdproxy:8080 %h %p' would not work!
        proxy = paramiko.ProxyCommand(
            f"nc --proxy-type http --proxy {http_proxy} {instance} {22}"
        )
        proxy.settimeout(300)
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    retries = 20
    while retries > 0:
        try:
            client.connect(
                instance,
                username=username,
                key_filename=keypath,
                timeout=10,
                sock=proxy if http_proxy else None,
            )
            print(f"Connected to {instance}")
            break
        except Exception as e:
            print(f"Exception: {e} Retrying...")
            retries -= 1
            time.sleep(10)
    return client

def upload_file(instance_id, client, localpath, remotepath):
    ftp_client = client.open_sftp()
    print(f"Uploading `{localpath}` to {instance_id}...")
    ftp_client.put(localpath, remotepath)
    ftp_client.close()
    print(f"`{localpath}` uploaded to {instance_id}.")

def clean_remote_processes(res):
    global servers
    global workers
    global client_dict
    global training_script

    this_file = sys.argv[0]

    cmd = f"ps -ef | grep python| grep -w {training_script} | grep -v -w {this_file} | awk {{'print $2'}} | xargs kill -9"
    for instance in servers + workers:
        client = client_dict[instance]
        run_command(instance, client, cmd)
        print(f"Run command: {cmd}\n On {instance}")

def main():
    args = parse_args()

    global servers
    global workers
    global client_dict
    global training_script

    servers = args.ps.split(',')
    workers = args.worker.split(',')

    if args.only_show_instances:
        print("Parameter Servers:")
        for server in servers:
            print("\t",instance)
        print("Wrokers:")
        for worker in workers:
            print("\t",worker)
        return
    
    #world_size = len(instances)
    #print(f"Running world size {world_size} with instances: {instances}")

    # Key: instance id; value: paramiko.SSHClient object.
    for instance in servers + workers:

        client = connect_to_instance(
            instance.split(":")[0], args.ssh_key_file, args.ssh_user, args.http_proxy
        )
        client_dict[instance] = client

    assert os.path.exists(
        args.training_script
    ), f"File `{args.training_script}` does not exist"
    training_script = args.training_script
    file_paths = args.aux_files.split(",") if args.aux_files else []
    for local_path in file_paths:
        assert os.path.exists(local_path), f"File `{local_path}` does not exist"
    
    remote_dir = f"launcher-tmp-{uuid.uuid1()}"
    script_basename = os.path.basename(args.training_script)
    remote_script = os.path.join(remote_dir, script_basename)

    # Upload files to all instances concurrently.
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as uploaders:
        for instance_id, client in client_dict.items():
            run_command(instance_id, client, f"mkdir -p {remote_dir}")
            uploaders.submit(
                upload_file, instance_id, client, args.training_script, remote_script
            )
            for local_path in file_paths:
                uploaders.submit(
                    upload_file,
                    instance_id,
                    client,
                    local_path,
                    os.path.join(remote_dir, os.path.basename(local_path)),
                )
    for instance_id, client in client_dict.items():
        run_command(instance_id, client, f"chmod +x {remote_script}")
        run_command(instance_id, client, f"ls -al {remote_dir}")

    environment = {}

    ps_arg = "--ps {}".format(args.ps) if args.ps != "" else ""
    
    worker_arg = "--worker {}".format(args.worker)

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(client_dict)) as executor:
        rank = 0
        task_id = 0
        for server in servers:
            environment["RANK"] = str(rank)
            environment_cmd = "; ".join(
                [f"export {key}={value}" for (key, value) in environment.items()]
            )
            prepare_cmd = f"{args.prepare_cmd}; " if args.prepare_cmd else ""
            cmd = "{}; {} {} {} {} {} {} {} {}".format(
                environment_cmd,
                f"cd {remote_dir} ;",
                prepare_cmd,
                f"python {script_basename}",
                " ".join(args.training_script_args),
                ps_arg,
                worker_arg,
                "--job_name ps",
                f"--task_index {task_id}"
            )
            print(f"Run command: {cmd}")
            executor.submit(run_command, server, client_dict[server], cmd, environment)
            rank += 1
            task_id += 1

        task_id = 0
        for worker in workers:
            environment["RANK"] = str(rank)
            environment_cmd = "; ".join(
                [f"export {key}={value}" for (key, value) in environment.items()]
            )
            prepare_cmd = f"{args.prepare_cmd}; " if args.prepare_cmd else ""
            cmd = "{}; {} {} {} {} {} {} {} {}".format(
                environment_cmd,
                f"cd {remote_dir} ;",
                prepare_cmd,
                f"python {script_basename}",
                " ".join(args.training_script_args),
                ps_arg,
                worker_arg,
                "--job_name worker",
                f"--task_index {task_id}"
            )
            print(f"Run command: {cmd}")
            if task_id == 0:
                executor.submit(run_command, worker, client_dict[worker], cmd, environment).add_done_callback(clean_remote_processes)
            else:
                executor.submit(run_command, worker, client_dict[worker], cmd, environment)
            rank += 1
            task_id += 1          
    
    # Cleanup temp dir.
    for instance_id, client in client_dict.items():
        run_command(instance_id, client, f"rm -rf {remote_dir}")
        client.close()

def parse_args():
    """
    Helper function parsing the command line options
    """
    parser = ArgumentParser(
        description="PyTorch distributed training launch "
        "helper utilty that will spawn up "
        "parties for MPC scripts on cluster"
    )

    parser.add_argument(
        "--only_show_instances",
        action="store_true",
        default=False,
        help="Only show the given instances."
        "No other actions will be done",
    )

    parser.add_argument(
        "--ps",
        type=str,
        default="",
        help="The comma-separated of parameter servers",
    )

    parser.add_argument(
        "--worker",
        type=str,
        required=True,
        help="The comma-separated of workers",
    )

    parser.add_argument(
        "--ssh_key_file",
        type=str,
        required=True,
        help="Path to the RSA private key file " "used for instance authentication",
    )

    parser.add_argument(
        "--ssh_user",
        type=str,
        default="ubuntu",
        help="The username to ssh to AWS instance",
    )

    parser.add_argument(
        "--http_proxy",
        type=str,
        default=None,
        help="If not none, use the http proxy specified "
        "(e.g., fwdproxy:8080) to ssh to AWS instance",
    )

    parser.add_argument(
        "--aux_files",
        type=str,
        default=None,
        help="The comma-separated paths of additional files "
        " that need to be transferred to instances. "
        "If more than one file needs to be transferred, "
        "the basename of any two files can not be the "
        "same.",
    )

    parser.add_argument(
        "--prepare_cmd",
        type=str,
        default="",
        help="The command to run before running distribute "
        "training for prepare purpose, e.g., setup "
        "environment, extract data files, etc.",
    )

    # positional
    parser.add_argument(
        "training_script",
        type=str,
        help="The full path to the single machine training "
        "program/script to be launched in parallel, "
        "followed by all the arguments for the "
        "training script",
    )

    # rest from the training program
    parser.add_argument("training_script_args", nargs=REMAINDER)
    return parser.parse_args()

if __name__ == "__main__":
    main()