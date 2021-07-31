#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-28
from stream_lite.utils.immutable_dict import ImmutableDict

from .operator_base import OperatorBase
from .source_op_base import SourceOperatorBase
from .sink_op_base import SinkOperatorBase
from .key_op_base import KeyOperatorBase
from .key_by_input_op import KeyByInputOp
from .debug_op import DebugOp
from .sum_op import SumOp

BUILDIN_OPS = ImmutableDict([
    ("OperatorBase", OperatorBase),
    ("SourceOperatorBase", SourceOperatorBase),
    ("SinkOperatorBase", SinkOperatorBase),
    ("KeyOperatorBase", KeyOperatorBase),
    ("KeyByInputOp", KeyByInputOp),
    ("DebugOp", DebugOp),
    ("SumOp", SumOp),
    ])
