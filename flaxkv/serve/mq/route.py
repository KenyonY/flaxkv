from flaxkv.pack import encode, decode


def healthz() -> str:
    return "OK"


def disconnect(client_id: str) -> bytes:
    ...


def set_batch_stream(data: bytes):
    ...