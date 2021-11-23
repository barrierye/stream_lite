#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-11-23
import logging


_LOGGER = logging.getLogger(__name__)

class MigrateFilterWindow(object):

    def __init__(self, window_size: int = 1000):
        """
        window: [base_id, base_id + window_size)
        """
        self.window_size = window_size
        self.window = [False] * window_size
        self.window_base_id = 0

        self.duplicate_window = [False] * window_size
        self.duplicate_window_base_id = -1

    def _add_data_to_duplicate_window(self, data_id: int):
        if self.duplicate_window_base_id == -1:
            self.duplicate_window_base_id = data_id + 1
            return

        if data_id < self.duplicate_window_base_id:
            _LOGGER.warning(
                    "data_id({}) on the left of duplicate_window({}).".format(
                        data_id, self.duplicate_window_base_id))
        elif data_id < self.duplicate_window_base_id + self.window_size:
            if self.duplicate_window[data_id - self.duplicate_window_base_id] is True:
                _LOGGER.warning(
                        "data_id({}) is already in duplicate_window({}).".format(
                            data_id, self.duplicate_window_base_id))
            else:
                self.duplicate_window[data_id - self.duplicate_window_base_id] = True
        else:
            _LOGGER.warning(
                    "data_id({}) on the right of duplicate_window({}).".format(
                        data_id, self.duplicate_window_base_id))
            while self.duplicate_window_base_id + self.window_size != data_id + 1:
                self.duplicate_window_base_id += 1
                self.duplicate_window.pop(0)
                self.duplicate_window.append(False)
            self.duplicate_window[data_id - self.duplicate_window_base_id] = True

        # update base_id
        while self.duplicate_window[0] == True:
            self.duplicate_window_base_id += 1
            self.duplicate_window.pop(0)
            self.duplicate_window.append(False)

    def _add_out_window_duplicate_data(self, data_id: int):
        """
        新到数据在window左侧，需要插入duplicate_window
        data_id
           |
           v     
               [  window  ]
        """
        self._add_data_to_duplicate_window(data_id)
        return True

    def _add_in_window_data(self, data_id: int):
        """
        新到数据在window里，需要插入window
          data_id
             |
             v     
        [  window  ]
        """
        is_duplicate = False
        if self.window[data_id - self.window_base_id] == False:
            self.window[data_id - self.window_base_id] = True
        else:
            # 重复元素，更新duplicate_window
            self._add_data_to_duplicate_window(data_id)
            is_duplicate = True

        # update base_id
        while self.window[0] == True:
            self.window_base_id += 1
            self.window.pop(0)
            self.window.append(False)

        return is_duplicate

    def _add_out_window_new_data(self, data_id: int):
        """
        新到数据在window右侧，需要更新window
                    data_id
                       |
                       v     
        [  window  ]
        """
        _LOGGER.warning(
                "data_id({}) on the right of window({}).".format(
                    data_id, self.window_base_id))
        while self.window_base_id + self.window_size != data_id + 1:
            self.window_base_id += 1
            self.window.pop(0)
            self.window.append(False)
        self.window[data_id - self.window_base_id] = True

        # update base_id
        while self.window[0] == True:
            self.window_base_id += 1
            self.window.pop(0)
            self.window.append(False)

        return False

    def duplicate_or_update(self, data_id: int):
        is_duplicate = False
        if data_id < self.window_base_id:
            # 只能为重复元素
            is_duplicate = self._add_out_window_duplicate_data(data_id)
        elif data_id < self.window_base_id + self.window_size:
            # 可能为重复元素，也可能为新元素
            is_duplicate = self._add_in_window_data(data_id)
        else:
            # 新元素，需要强制移动窗口
            is_duplicate = self._add_out_window_new_data(data_id)

        return [is_duplicate, self._judge_if_reached()]

    def _judge_if_reached(self):
        return self.window_base_id == self.duplicate_window_base_id
