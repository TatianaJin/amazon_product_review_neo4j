#! /usr/bin/env python3

import json


def get_key_freq(path):
    key_freq = dict()
    with open(path, "r") as inf:
        for line in inf:
            j = json.loads(line.strip())
            for key in j:
                if key in key_freq:
                    key_freq[key] += 1
                else:
                    key_freq[key] = 1
    with open(path+".key_freq", "w") as outf:
        for k, v in key_freq.items():
            outf.write(f"{k} {v}\n")


get_key_freq("All_Amazon_Review.json")
get_key_freq("All_Amazon_Meta.json")
