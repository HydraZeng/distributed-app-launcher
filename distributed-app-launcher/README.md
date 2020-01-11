## Introductions
This a launcher to run distributed applications easily. The code refer to [aws launcer provided by Facebook](https://github.com/facebookresearch/CrypTen/blob/master/scripts/aws_launcher.py).

*ps_launcher.py* is a special version that run parameter server applications, including kill the parameter server processes when the chief worker finish. (Yes, they always hung and need to be killed mannually :) )

## Prerequisite
```shell
python install -e requirements.txt
```
## How to run
```shell
python launcher.py --help
```

To learn how to use it, you can also walkthrough the code, which is very readable!