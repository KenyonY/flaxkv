# Copyright (c) 2023 K.Y. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import msgpack
import msgspec.msgpack
import numpy as np

# or use msgpack_numpy: https://github.com/lebedov/msgpack-numpy  install: pip install msgpack-numpy
# usage:
# import msgpack
# import msgpack_numpy as m
# m.patch()


class NPArray(msgspec.Struct, gc=False, array_like=True):
    dtype: str
    shape: tuple
    data: bytes


numpy_array_encoder = msgspec.msgpack.Encoder()
numpy_array_decoder = msgspec.msgpack.Decoder(type=NPArray)


def encode_hook(obj):
    if isinstance(obj, np.ndarray):
        return msgspec.msgpack.Ext(
            1,
            numpy_array_encoder.encode(
                NPArray(dtype=obj.dtype.str, shape=obj.shape, data=obj.data)
            ),
        )
    return obj


def ext_hook(type, data: memoryview):
    if type == 1:
        serialized_array_rep = numpy_array_decoder.decode(data)
        return np.frombuffer(
            serialized_array_rep.data, dtype=serialized_array_rep.dtype
        ).reshape(serialized_array_rep.shape)
    return data


encode = msgspec.msgpack.Encoder(enc_hook=encode_hook).encode
decode = msgspec.msgpack.Decoder(ext_hook=ext_hook).decode


def decode_key(value):
    return msgpack.unpackb(value, use_list=False)
