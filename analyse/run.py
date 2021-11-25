#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-11-25
import re
import matplotlib.pyplot as plt

pattern = r".*P\[(\d+)\] latency: (\d+)ms"
latencies = []

with open("./log.txt.1") as f:
    for line in f:
        matchObj = re.match(pattern, line)
        if matchObj:
            data_id = matchObj.group(1)
            latency = matchObj.group(2)
            latencies.append(float(latency))

plt.plot(latencies)
plt.show()
