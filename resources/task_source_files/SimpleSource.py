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
        self.file = open(resource_path_dict["document-words.txt"])
        #  self.file = open(resource_path_dict["document-words-small.txt"])
        self.counter = 0
        self.register_var("counter")

    def compute(self, inputs):
        time.sleep(1)
        word = self.file.readline()
        if word:
            self.counter += 1
            return word.strip()
        else:
            raise FinishJobError("") 
