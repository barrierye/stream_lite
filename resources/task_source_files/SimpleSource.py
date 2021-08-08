#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-24
import time
import re

from stream_lite import SourceOperatorBase
from stream_lite.utils import FinishJobError

class SimpleSource(SourceOperatorBase):

    def init(self, resource_path_dict):
        with open(resource_path_dict["document-words.txt"]) as f:
            self.lines = f.readlines()
        self.counter = 0
        self.register_var("counter")

    def compute(self, inputs):
        time.sleep(0.5)
        if self.counter < len(self.lines):
            word = self.lines[self.counter].strip()
            self.counter += 1
            return word
        else:
            raise FinishJobError("") 
