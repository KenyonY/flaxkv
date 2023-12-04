import httpx

from ..pack import decode, encode


class RemoteDictDB:
    def __init__(
        self, url: str, db_name: str, rebuild=False, backend="leveldb", timeout=6
    ):
        self._url = url
        self._db_name = db_name
        self._client = httpx.Client(timeout=timeout)
        self._attach_db(rebuild=rebuild, backend=backend)

    def _attach_db(self, rebuild=False, backend="lmdb"):
        url = f"{self._url}/attach"
        response = self._client.post(
            url, json={"db_name": self._db_name, "backend": backend, "rebuild": rebuild}
        )
        return response.json()

    def detach_db(self, db_name=None):
        if db_name is None:
            db_name = self._db_name

        url = f"{self._url}/detach"
        response = self._client.post(url, json={"db_name": db_name})
        return response.json()

    def get(self, key):
        url = f"{self._url}/get_raw?db_name={self._db_name}"
        response = self._client.post(url, data=encode(key))
        return decode(response.read())

    def set(self, key, value):
        url = f"{self._url}/set_raw?db_name={self._db_name}"
        data = {"key": encode(key), "value": encode(value)}
        response = self._client.post(url, data=encode(data))
        return response.read()

    def keys(self):
        url = f"{self._url}/keys?db_name={self._db_name}"
        response = self._client.get(url)
        return response.json()["keys"]

    def __contains__(self, key):
        url = f"{self._url}/contains?db_name={self._db_name}"
        response = self._client.post(url, data=encode(key))
        return decode(response.read())

    def setdefault(self, key, default=None):
        value = self.get(key)
        if value is None:
            self.set(key, default)
            return default
        return value

    def __setitem__(self, key, value):
        self.set(key, value)

    def __getitem__(self, key):
        value = self.get(key)
        if value is None:
            raise KeyError(f"Key `{key}` not found in the database.")
        return value
