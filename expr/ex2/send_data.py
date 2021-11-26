#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-11-26
import requests
from time import time, sleep
from flask import Flask
import threading
import queue 
import logging
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

def run(que, port):
    app = Flask(__name__)

    @app.route("/api/recv/<string:out>")
    def run(out):
        que.put(out)
        return "ok"

    app.run(host="0.0.0.0", debug=True, port=port, use_reloader=False)

que = queue.Queue()
th = threading.Thread(target=run, args=(que, 8998))
th.start()

with open("./resources/document-words.txt") as f:
    for idx, line in enumerate(f):
        sleep(0.08)
        line = line.strip()
        st = time()
        while True:
            try:
            a = requests.get(
                    "http://192.168.105.84:8081/api/put/{}/{}".format(idx, line))
            except Exception:
                sleep(0.01)
                continue
            if a.status_code == 200:
                break
            sleep(0.01)
        out = que.get()
        #  print(out)
        et = time()
        print("P[{}] latency: {}ms".format(idx, int((et - st) * 1000)))
