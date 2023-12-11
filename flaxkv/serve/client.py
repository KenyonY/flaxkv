from ..pack import decode, decode_key, encode


class RemoteDictDB:
    def __init__(
        self,
        url: str,
        db_name: str,
        rebuild=False,
        backend="leveldb",
        timeout=6,
        **kwargs,
    ):
        import httpx

        self._url = url
        self._db_name = db_name
        self._client = kwargs.pop("client", httpx.Client(timeout=timeout))
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

    def get(self, key, default=None):
        url = f"{self._url}/get_raw?db_name={self._db_name}"
        response = self._client.post(url, data=encode(key))
        raw_data = response.read()
        if raw_data == b"iamnull123":
            return default
        value = decode(raw_data)
        return value

    def set(self, key, value):
        url = f"{self._url}/set_raw?db_name={self._db_name}"
        data = {"key": encode(key), "value": encode(value)}
        response = self._client.post(url, data=encode(data))
        return response.json()

    def update(self, d: dict):
        url = f"{self._url}/update_raw?db_name={self._db_name}"
        response = self._client.post(url, data=encode(d))
        return response.json()

    def pop(self, key, default=None):
        url = f"{self._url}/pop"
        data = {"key": key, "db_name": self._db_name}
        response = self._client.post(url, json=data)
        result = decode(response.read())
        if result["success"]:
            value = result["data"]
            if value is None:
                return default
            return value
        else:
            raise

    def _items_dict(self):
        url = f"{self._url}/items?db_name={self._db_name}"
        response = self._client.get(url)
        return decode(response.read())["data"]

    def items(self):
        return self._items_dict().items()

    def __repr__(self):
        return str(self._items_dict())

    def keys(self):
        url = f"{self._url}/keys?db_name={self._db_name}"
        response = self._client.get(url)
        return list(decode_key(response.read())['data'])

    def values(self):
        url = f"{self._url}/values?db_name={self._db_name}"
        response = self._client.get(url)
        return decode(response.read())['data']

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

    def __len__(self):
        return len(self.keys())
