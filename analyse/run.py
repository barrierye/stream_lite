#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-11-25
import re
import sys
import matplotlib.pyplot as plt
import datetime
import numpy as np
import pandas as pd

plt.rcParams['font.sans-serif']=['SimHei'] #用来正常显示中文标签
plt.rcParams['axes.unicode_minus']=False #用来正常显示负号


def get_data(filename):
    pattern = r".*P\[(.+)\] latency: (\d+)ms"
    dates = []
    latencies = []

    with open(filename) as f:
        for line in f:
            matchObj = re.match(pattern, line)
            if matchObj:
                date = datetime.datetime.fromtimestamp(
                        float(matchObj.group(1)))
                latency = float(matchObj.group(2))
                dates.append(date)
                latencies.append(np.log10(latency))
    return dates, latencies

def find_key(filename, pattern):
    with open(filename) as f:
        for line in f:
            matchObj = re.match(pattern, line)
            if matchObj:
                strategy_date = datetime.datetime.fromtimestamp(
                        float(matchObj.group(1)))
                print(strategy_date)
                return strategy_date

# flink
plt.subplot(1, 2, 1)
dates, latencies = get_data("./log.txt.flink")
dates = [x - dates[50] for x in dates]
dates = [y.total_seconds() for y in dates]
print(dates)
plt.xlim(dates[50], dates[100])
plt.ylim(1.6, 3.75)
plt.xlabel("time(s)")
plt.ylabel("log10(latency)")
plt.title("Flink-like")
plt.gcf().autofmt_xdate()
#  plt.scatter(dates, latencies)
plt.plot(dates, latencies, ".")

# my
my_file = "./log.txt.my2"
plt.subplot(1, 2, 2)
dates, latencies = get_data(my_file)
dates = [x - dates[30] for x in dates]
dates = [y.total_seconds() for y in dates]
plt.xlim(dates[30], dates[100])
plt.ylim(1.6, 3.75)
plt.xlabel("time(s)")
plt.title(u"本文提出的新机制")
plt.ylabel("log10(latency)")
plt.gcf().autofmt_xdate()
#  plt.scatter(dates, latencies)
plt.plot(dates, latencies, ".")

pattern = r".*\[(.+)\] current latency.*"
strategy_date = find_key(my_file, pattern)
pattern = r".*\[(.+)\] step 1.*"
s1_date = find_key(my_file, pattern)
pattern = r".*\[(.+)\] step 2.*"
s2_date = find_key(my_file, pattern)
pattern = r".*\[(.+)\] step 3.*"
s3_date = find_key(my_file, pattern)
pattern = r".*\[(.+)\] step 4.*"
s4_date = find_key(my_file, pattern)
pattern = r".*\[(.+)\] step 5.*"
s5_date = find_key(my_file, pattern)
pattern = r".*\[(.+)\] FINISH terminate!"
end_date = find_key(my_file, pattern)
#  plt.vlines(s2_date, 0, 100, colors = "c", linestyles = "dashed", linewidth=4)
'''
print("total: " + str(end_date - s1_date))
count = 0
latency_all = 0
for i, d in enumerate(dates):
    if s1_date <= d and d <= end_date:
        latency_all += 10**latencies[i]
        count += 1
print(latency_all/count)
'''
plt.show()
