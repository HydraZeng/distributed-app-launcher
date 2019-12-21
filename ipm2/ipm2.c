/**
 *  
 */
#include <iostream>
#include <utility>
#include <vector>
#include <list>
#include <string>
#include <map>
#include <algorithm>
#include <stdlib.h>
#include <time.h>

//=============================== DEF ===========================================
typedef struct
{
    size_t minlen;
    size_t minsupp;
    size_t maxerror;
    double minscore;
} criterion;

typedef std::string pattern;

typedef std::vector<std::pair<size_t,size_t>> loclist;
/*
 The patterns store in a 'patternlist' instance.
 Their first element is the same.
*/
typedef std::map<pattern,loclist> patternlist;

std::vector<patternlist> IPM2Pro1(size_t ptlistsize, const criterion& c, const pattern& inputseq);

patternlist IPM2Pro2(const std::vector<patternlist>& ptlist, const criterion& c, const pattern& inputseq);

patternlist IPM2Pro2Extend(const pattern& p1, const loclist& p1loclist, const std::vector<patternlist>& ptlist, const criterion& c, const pattern& inputseq);

loclist IPM2Pro3(size_t p1length, const loclist& l1, const loclist& l2, size_t maxerror);

double nonoverlayscore(const loclist& loc, const pattern& inputseq, size_t* retcounter);

//========================================= CODE ==========================================

patternlist IPM2(size_t queuenumber, const criterion& c, const pattern& inputseq) {
    size_t ptlistsize = 1 << queuenumber;

    std::vector<patternlist> ptlist = IPM2Pro1(ptlistsize, c, inputseq);
 
    return std::move(IPM2Pro2(ptlist, c, inputseq));
}

bool new_IPM2(size_t queuenumber, const criterion& c, const std::vector<size_t>& S) {
  pattern inputseq;
  for (const size_t& s: S) {
    inputseq.push_back(s);
  } 
  printf("Start IPM2, inputseq: %s\n", inputseq.c_str());
  auto ipm2res = IPM2(queuenumber, c, inputseq);
  printf("Finish IPM2, res size: %ld\n", ipm2res.size());
  for (auto& item: ipm2res) {
    size_t len = 0;
    if(nonoverlayscore(item.second, inputseq, &len) >= c.minscore && len >= c.minsupp) {
        printf("Pattern:\n\t%s\n", item.first.c_str());
    	for (const auto& loc : item.second) {
        	printf("\tfirst: %ld, second: %ld\n", loc.first, loc.second);
    	}   
	return true;
    }
  }
  return false;
}

std::vector<patternlist> IPM2Pro1(size_t ptlistsize, const criterion& c, const pattern& inputseq) {
    size_t i, j;
    std::vector<patternlist> ptlist(ptlistsize);
    
    /*
        Construct all patterns whose length is 2.
    */
    for (i = 0; i < inputseq.size() - c.maxerror - 1; ++i) {
        for (j = i + 1; j <= i + c.maxerror + 1; ++j) {
            pattern p;
            p.push_back(inputseq[i]);
            p.push_back(inputseq[j]);
            patternlist& plist = ptlist[p[0]];
            if (plist.find(p) == plist.end()) {
                plist.emplace(p, NULL);
            }
            plist[p].emplace_back(i,j);
        }
    }

    for (i = inputseq.size() - c.maxerror - 1; i < inputseq.size() - 1; ++i) {
        for (j = i + 1; j < inputseq.size(); ++j) {
            pattern p;
            p.push_back(inputseq[i]);
            p.push_back(inputseq[j]);
            patternlist& plist = ptlist[p[0]];
            if (plist.find(p) == plist.end()) {
                plist.emplace(p, NULL);
            }
            plist[p].emplace_back(i,j);
        }
    }

    /*
        Remove all patterns that occurrence times are not enough.
    */
    for (patternlist& plist: ptlist) {
        std::list<pattern> removelist;
        for (auto plistit = plist.begin(); plistit != plist.end(); ++plistit) {
            if (plistit->second.size() < c.minsupp) {
                removelist.emplace_back(plistit->first);
            }
        }
        for (const pattern& removep: removelist) {
            plist.erase(removep);
        }
    }

    return std::move(ptlist);
}

bool small(std::pair<size_t,size_t> a, std::pair<size_t,size_t> b) {
    if (a.first != b.first) {
        return a.first < b.first;
    }
    else {
        return a.second < b.second;
    }
}

void merge(patternlist& pl1, patternlist& pl2) {
    for (auto& tmpp: pl2) {
        auto findit = pl1.find(tmpp.first);
        if (findit == pl1.end()) {
            pl1.emplace(std::move(tmpp.first), std::move(tmpp.second));
        }
        else {
            std::cout << "merge!" << std::endl;
            loclist& loc1 = findit->second;
            loclist& loc2 = tmpp.second;
            loclist mergelist(loc1.size() + loc2.size());
            
            size_t i, j, k;
            for (i = 0, j = 0, k = 0; i < loc1.size() && j < loc2.size();) {
                if (small(loc1[i],loc2[j])) {
                    mergelist[k++] = loc1[i++];
                }
                else {
                    mergelist[k++] = loc2[j++];
                }
            }
            for (; i < loc1.size();) {
                mergelist[k++] = loc1[i++];
            }
            for (; j < loc2.size();) {
                mergelist[k++] = loc2[j++];
            }

            findit->second = std::move(mergelist);

            /*
            findit->second.merge(std::move(tmpp.second),
                                [](std::pair<size_t,size_t> a, std::pair<size_t,size_t> b){
                                    if (a.first != b.first) {
                                        return a.first < b.first;
                                    }
                                    else {
                                        return a.second < b.second;
                                    }
                                });
            */
        }
    }
}

double simplescore(const loclist& loc, const pattern& inputseq) {
    double psize = 0;
    for (const auto& occ: loc) {
        psize += occ.second - occ.first + 1;
    }
    return psize / inputseq.size();
}


// Suppose 'loclist loc' is sorted
double nonoverlayscore(const loclist& loc, const pattern& inputseq, size_t* retcounter) {
    std::vector<double> s(loc.size() + 1, 0); 
    std::vector<size_t> counter(loc.size() + 1, 0);

    // s[i] & counter[i] are realted to loc[i-1]
    // Because s[0] & counter[0] are used
    size_t i, k;
    for (i = 1; i <= loc.size(); ++i) {
        for (k = i - 1; k > 0; --k) {
            if(loc[k-1].second < loc[i-1].first) {
                break;
            }
        }
        if (s[k] + loc[i-1].second - loc[i-1].first + 1 > s[i-1]) {
            s[i] = s[k] + loc[i-1].second - loc[i-1].first + 1;
            counter[i] = counter[k] + 1;
        }
        else {
            s[i] = s[i-1];
            counter[i] = counter[i-1];
        }
    }
    *retcounter = counter[loc.size()];
    return s[loc.size()] / inputseq.size();
}


double score(const pattern& p, const loclist& loc, const pattern& inputseq) {
    return simplescore(loc, inputseq);
}

patternlist IPM2Pro2(const std::vector<patternlist>& ptlist, const criterion& c, const pattern& inputseq) {
    patternlist resultptlist;
    
    for (const patternlist& plist: ptlist) {
        for (const auto& p: plist) {
            patternlist tmp = IPM2Pro2Extend(p.first, p.second, ptlist, c, inputseq);
	    //printf("map 1 size: %ld, map 2 size: %ld, ", resultptlist.size(), tmp.size());
            merge(resultptlist, tmp);
	    //printf("merged map size: %ld\n", resultptlist.size());
        }
    }

    /*
    std::vector<loclist> result(resultptlist.size());
    size_t i = 0;
    for (auto& ptlistitem: resultptlist) {
        result[i++] = std::move(ptlistitem.second);
    }

    return std::move(result);
    */
    return std::move(resultptlist);
}

patternlist IPM2Pro2Extend(const pattern& p1, 
                           const loclist& p1loclist,
                           const std::vector<patternlist>& ptlist, 
                           const criterion& c, 
                           const pattern& inputseq) {
    patternlist result;

    const patternlist& plist = ptlist[p1.back()];
    if(plist.size() == 0) {
        if (p1.size() >= c.minlen && result.find(p1) == result.end()) {
            if (score(p1, p1loclist, inputseq) >= c.minscore) {
                result.emplace(p1, p1loclist);
            }
        }
    }
    else {
        for (const auto& p2: plist) {
            pattern p3(p1);
            p3.push_back(p2.first.back());
            loclist p3loclist = IPM2Pro3(p1.size(), p1loclist, p2.second, c.maxerror);
	    //printf("p3: %s, p3loclist size: %ld\n", p3.c_str(), p3loclist.size());
            if (p3loclist.size() >= c.minsupp) {
                patternlist tmp = IPM2Pro2Extend(p3, p3loclist, ptlist, c, inputseq);
                merge(result, tmp);
                if (p1loclist.size() > p3loclist.size()) {
                    if (p1.size() >= c.minlen && result.find(p1) == result.end()) {
                        if (score(p1, p1loclist, inputseq) >= c.minscore) {
                            result.emplace(p1, p1loclist);
                        }
                    }
                }
            }
            else {
                if (p1.size() >= c.minlen && result.find(p1) == result.end()) {
                    if (score(p1, p1loclist, inputseq) >= c.minscore) {
                        result.emplace(p1, p1loclist);
                    }
                }
            }
        }
    }

    return std::move(result);
}

loclist IPM2Pro3(size_t p1length, const loclist& l1, const loclist& l2, size_t maxerror) {
    loclist l3;
    l3.clear();

    for (const auto& loc1: l1) {
        for (const auto& loc2: l2) {
            if (loc1.second == loc2.first && loc2.second <= loc1.first + maxerror + p1length) {
                l3.emplace_back(loc1.first, loc2.second);
            }
        }
    }

    std::sort(l3.begin(), l3.end());
    l3.erase(std::unique(l3.begin(),l3.end()), l3.end());

    return std::move(l3);
}

int main () {
    criterion c;
    c.minlen = 2;
    c.minsupp = 2;
    c.maxerror = 1;
    c.minscore = 0.5;

    size_t ptlistsize = 6;

    size_t max = 1 << ptlistsize;
    size_t length = 200;
    std::vector<size_t> input;
    input.reserve(length);
    srand((unsigned)time(0));
    printf("Pattern:\n");
    for (size_t i = 0; i < length; ++i) {
        size_t val = i % max;
        input.emplace_back(val);
        printf("%ld\t", val);
        if (i % max == max - 1) {
            printf("\n");
        }
    }
    printf("\n");
    bool res = new_IPM2(ptlistsize, c, input);
    //bool res = new_IPM2(2, c, {1,3,2,1,2});
    printf("res: %s\n", res ? "true" : "false");
    return 0;
}

