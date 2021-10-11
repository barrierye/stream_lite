#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-10-12
import copy


class SubTaskDesc(object):

    def __init__(self,
            name: str,
            task_manager_name: str,
            is_source: bool,
            is_sink: bool,
            downstream_name: List[str],
            ):
        self.name = name
        self.task_manager_name = task_manager_name
        self.is_source = is_source
        self.is_sink = is_sink
        self.downstream_names = downstream_names


class SubTaskTable(object):

    def __init__(self):
        self.table = {}

    def add_subtask(self, subtask_desc: SubTaskDesc) -> None:
        if subtask_desc.name in self.table:
            raise KeyError(
                    "Failed to add subtask: {} already exists.".format(subtask_desc.name))
        self.table[subtask_desc.name] = subtask_desc

    def get_subtask(self, name: str) -> SubTaskDesc:
        if name not in self.table:
            raise KeyError(
                    "Failed to get subtask: {} does not exist".format(name))
        return self.table[name]

    def get_subtasks(self) -> Dict[str, SubTaskDesc]:
        return self.table

    def get_sources(self) -> List[SubTaskDesc]:
        sources = []
        for subtask in self.table.values():
            if subtask.is_source:
                sources.append(subtask)
        return sources

    def get_sinks(self) -> List[SubTaskDesc]:
        sinks = []
        for subtask in self.table.values():
            if subtask.is_sink:
                sinks.append(subTask)
        return sinks

