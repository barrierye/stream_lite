#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-11-26
import requests
from time import time

with open("./resources/document-words.txt") as f:
    for idx, line in enumerate(f):
        line = line.strip()
        st = time()
        while True:
            a = requests.get(
                    "http://192.168.105.84:8081/api/put/{}/{}".format(idx, line))
            if a.status_code == 200:
                break
        et = time()
        print("P[{}] latency: {}ms".format(idx, et - st))
