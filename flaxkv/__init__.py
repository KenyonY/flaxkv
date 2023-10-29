from .base import LevelDBDict, LMDBDict

__version__ = "0.1.1"

__all__ = [
    "dbdict",
    "LMDBDict",
    "LevelDBDict",
]


def dbdict(path, backend='lmdb', rebuild=False, **kwargs):
    if backend == 'lmdb':
        return LMDBDict(path, rebuild=rebuild, **kwargs)
    elif backend == 'leveldb':
        return LevelDBDict(path, rebuild=rebuild, **kwargs)
    else:
        raise ValueError(f"Unsupported DB type {backend}.")
