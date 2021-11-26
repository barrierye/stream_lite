#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-11-25
import re
import sys
import matplotlib.pyplot as plt
import datetime

pattern = r".*P\[(.+)\] latency: (\d+)ms"
dates = []
latencies = []

with open(sys.argv[1]) as f:
    for line in f:
        matchObj = re.match(pattern, line)
        if matchObj:
            date = datetime.datetime.fromtimestamp(
                    float(matchObj.group(1)))
            latency = matchObj.group(2)
            dates.append(date)
            latencies.append(float(latency))

plt.plot(dates, latencies, ".")
plt.show()
