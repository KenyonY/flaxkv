from .base import LevelDBDict, LMDBDict

__all__ = [
    "dbdict",
    "LMDBDict",
    "LevelDBDict",
]


def dbdict(path, backend='lmdb', recreate=False, **kwargs):
    if backend == 'lmdb':
        return LMDBDict(path, recreate=recreate, **kwargs)
    elif backend == 'leveldb':
        return LevelDBDict(path, recreate=recreate)
    else:
        raise ValueError(f"Unsupported DB type {backend}.")


__version__ = "0.1.0"
