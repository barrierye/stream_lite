#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-24
import os
import time
from typing import List, Dict, Optional, Tuple

from paddle_serving_app.local_predict import LocalPredictor
from stream_lite import OperatorBase

class DetOp(OperatorBase):

    def init(self, resource_path_dict):
        self.predictor = LocalPredictor()
        os.system("tar -xzvf {}".format(resource_path_dict["ocr_det_model"]))
        self.load_model_config(resource_path_dict["ocr_det_model"])
        with open(resource_path_dict["document-words.txt"]) as f:
            self.lines = f.readlines()
        self.counter = 0

    def __init__(self):
        super(DetOp, self).__init__()
        self.counter = {}
        self.register_var("counter")

    def compute(self, data: str) -> Tuple[str, int]:
        if data not in self.counter:
            self.counter[data] = 0
        self.counter[data] += 1
        time.sleep(0.01)
        #  print((data, self.counter[data]))
        return (data, self.counter[data])
