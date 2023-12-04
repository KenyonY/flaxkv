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
            host="0.0.0.0",
            port=port,
            workers=workers,
            app_dir="..",
            ssl_keyfile=kwargs.get("ssl_keyfile", None),
            ssl_certfile=kwargs.get("ssl_certfile", None),
        )


def main():
    fire.Fire(Cli)


if __name__ == "__main__":
    main()
