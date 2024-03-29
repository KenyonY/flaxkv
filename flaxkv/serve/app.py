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


from __future__ import annotations

from litestar import Litestar
from litestar.openapi import OpenAPIConfig

from .. import __version__
from .route import *


def on_startup():
    ...


def on_shutdown():
    ...


app = Litestar(
    route_handlers=[
        healthz,
        connect,
        disconnect,
        detach,
        get_,
        get_batch_stream_,
        set_,
        set_batch_,
        set_batch_stream_,
        delete_,
        delete_batch_,
        keys_,
        keys_stream_,
        dict_,
        dict_stream_,
        stat_,
        check_db,
    ],
    on_startup=[on_startup],
    on_shutdown=[on_shutdown],
    openapi_config=OpenAPIConfig(title="FlaxKV", version=f"v{__version__}"),
)
