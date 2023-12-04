from pathlib import Path

from .. import dictdb


class DBManager:
    def __init__(self, root_path="./FLAXKV_DB"):
        self._db_dict = {}
        self._root_path = Path(root_path)
        if not self._root_path.exists():
            self._root_path.mkdir(parents=True)

    def detach(self, db_name: str):
        return self._db_dict.pop(db_name, None)

    def set_db(self, db_name: str, backend: str, rebuild: bool):
        db_path = self._root_path / db_name
        self._db_dict[db_name] = dictdb(
            path_or_url=db_path.__str__(),
            backend=backend,
            rebuild=rebuild,
            raw=True,
            log=True,
        )

    def get(self, db_name: str, raise_key_error=False):
        if raise_key_error:
            return self._db_dict[db_name]
        else:
            return self._db_dict.get(db_name, None)
