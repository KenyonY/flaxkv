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

import pickle

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


np_encoder = msgspec.msgpack.Encoder()
np_decoder = msgspec.msgpack.Decoder(type=NPArray)

try:
    import pandas as pd

    def check_pandas_type(obj):
        return isinstance(obj, pd.DataFrame)

except ImportError:

    def check_pandas_type(obj):
        return type(obj).__name__ == "DataFrame"


def encode_hook(obj):
    if isinstance(obj, np.ndarray):
        return msgspec.msgpack.Ext(
            1,
            np_encoder.encode(
                NPArray(dtype=obj.dtype.str, shape=obj.shape, data=obj.data)
            ),
        )
    elif check_pandas_type(obj):
        # return msgspec.msgpack.Ext(2, pyarrow.serialize_pandas(obj).to_pybytes())
        return msgspec.msgpack.Ext(2, pickle.dumps(obj))
    return obj


def ext_hook(type, data: memoryview):
    if type == 1:
        serialized_array_rep = np_decoder.decode(data)
        return np.frombuffer(
            serialized_array_rep.data, dtype=serialized_array_rep.dtype
        ).reshape(serialized_array_rep.shape)
    elif type == 2:
        # return pyarrow.deserialize_pandas(pyarrow.py_buffer(data.tobytes()))
        return pickle.loads(data.tobytes())
    return data


encode = msgspec.msgpack.Encoder(enc_hook=encode_hook).encode
decode = msgspec.msgpack.Decoder(ext_hook=ext_hook).decode


def decode_key(value):
    return msgpack.unpackb(value, use_list=False)
