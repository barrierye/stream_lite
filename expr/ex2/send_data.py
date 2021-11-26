#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-11-26
import requests

with open("./resources/document-words.txt") as f:
    for line in f:
        line = line.strip()
        a = requests.get("192.168.105.84:18080/api/data/1/line")
        print(a)
        break
