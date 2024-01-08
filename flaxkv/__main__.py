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

import os
import platform

import fire
import uvicorn


class Cli:
    @staticmethod
    def run(port=8000, workers=1, **kwargs):
        """
        Runs the application using the Uvicorn server.

        Args:
            port (int): The port number on which to run the server. Default is 8000.
            workers (int): The number of worker processes to run. Default is 1.

        Returns:
            None
        """

        if platform.system() == "Windows":
            os.environ["TZ"] = ""

        uvicorn.run(
            app="flaxkv.serve.app:app",
            host=kwargs.get("host", "0.0.0.0"),
            port=port,
            workers=workers,
            app_dir="..",
            ssl_keyfile=kwargs.get("ssl_keyfile", None),
            ssl_certfile=kwargs.get("ssl_certfile", None),
            use_colors=False,
            log_level="info",
        )


def main():
    fire.Fire(Cli)


if __name__ == "__main__":
    main()
