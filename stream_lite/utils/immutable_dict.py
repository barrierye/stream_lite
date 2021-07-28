#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-29


class ImmutableDict(dict):
    
    def __hash__(self):
        return id(self)

    def _immutable(self, *args, **kws):
        raise TypeError("Failed: can not motify an immutable dict")

    __setitem__ = _immutable
    __delitem__ = _immutable
    clear       = _immutable
    update      = _immutable
    setdefault  = _immutable
    pop         = _immutable
    popitem     = _immutable

