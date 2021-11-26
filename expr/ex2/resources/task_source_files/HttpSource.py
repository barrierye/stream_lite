#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-24
import time
import re
from flask import Flask
import queue
import threading

from stream_lite import SourceOperatorBase
from stream_lite.utils import FinishJobError

class HttpSource(SourceOperatorBase):

    def init(self, resource_path_dict):
        self.counter = 0
        self.register_var("counter")
        self.input_que = queue.Queue()
        
        def run(que, port):
            app = Flask(__name__)

            @app.route("/api/put/<int:line>/<string:data>")
            def recv_data(line, data):
                que.put((line, data))
                return "ok\n"

            app.run(debug=True, port=port, use_reloader=False)

        self._thread = threading.Thread(
                target=run,
                args=(self.input_que, 18080))
        self._thread.start()

    def compute(self, inputs):
        while True:
            line, data = self.input_que.get()
            if self.counter > line:
                continue
            else:
                self.counter += 1
                word = data.strip()
                return word
