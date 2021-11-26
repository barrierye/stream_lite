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
plt.xlim(dates[50], dates[100])
plt.ylim(1.6, 3.75)
plt.xlabel("date")
plt.ylabel("log10(latency)")
plt.title(u"Flink-like")
plt.gcf().autofmt_xdate()
#  plt.scatter(dates, latencies)
plt.plot(dates, latencies, ".")

# my
my_file = "./log.txt.my2"
plt.subplot(1, 2, 2)
dates, latencies = get_data(my_file)
plt.xlim(dates[30], dates[100])
plt.ylim(1.6, 3.75)
plt.xlabel("date")
plt.title(u"The mechanism mentioned in this paper")
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

plt.show()
