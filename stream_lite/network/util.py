#-*- coding:utf8 -*-
# Copyright (c) 2021 barriery
# Python release: 3.7.0
# Create time: 2021-07-25

import stream_lite.proto.common_pb2 as common_pb2


def gen_nil_response(err_code: int = 0, message: str = "") -> common_pb2.NilResponse:
    return common_pb2.NilResponse(
            status=common_pb2.Status(
                err_code=err_code,
                message=message))
