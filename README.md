
<h1 align="center">
    <br>
    FlaxKV
</h1>


<p align="center">
Let you forget you're using a database —
Simple and high-performance persistent database solution
</p>
<p align="center">
    <a href="https://pypi.org/project/flaxkv/">
        <img src="https://img.shields.io/pypi/v/flaxkv?color=brightgreen&style=flat-square" alt="PyPI version" >
    </a>
    <a href="https://github.com/KenyonY/flaxkv/blob/main/LICENSE">
        <img alt="License" src="https://img.shields.io/github/license/KenyonY/flaxkv.svg?color=blue&style=flat-square">
    </a>
    <a href="https://github.com/KenyonY/flaxkv/releases">
        <img alt="Release (latest by date)" src="https://img.shields.io/github/v/release/KenyonY/flaxkv?&style=flat-square">
    </a>
    <a href="https://github.com/KenyonY/flaxkv/actions/workflows/ci.yml">
        <img alt="tests" src="https://img.shields.io/github/actions/workflow/status/KenyonY/flaxkv/ci.yml?style=flat-square&label=tests">
    </a>
    <a href="https://pypistats.org/packages/flaxkv">
        <img alt="pypi downloads" src="https://img.shields.io/pypi/dm/flaxkv?style=flat-square">
    </a>
</p>

<h4 align="center">
    <p>
        <b>English</b> |
        <a href="https://github.com/KenyonY/flaxkv/blob/main/README_ZH.md">简体中文</a> 
    </p>
</h4>

<p >
<br>
</p>


A persistent database masquerading as a dictionary.

The `flaxkv` module provides a dictionary-like interface for interacting with high-performance key-value databases (LMDB, LevelDB).
It abstracts the complexities of direct database interaction, allowing users to perform CRUD operations in a simple and 
intuitive manner. You can use it just like a Python dictionary without worrying about it blocking your main process at any stage.

**Use Cases**

- **Key-Value Structure**: `flaxkv` is suitable for storing simple key-value structured datasets.
 
- **High-Frequency Writing**: `flaxkv` is very suitable for scenarios that require high-frequency insertion/updating of data.
  
- **Machine Learning**: `flaxkv` is perfect for storing various embeddings, images, texts, and other large datasets with key-value structures in machine learning.

---

## Key Features

- **Always Up-to-date, Never Blocking**: It was designed from the ground up to ensure that no write operations block the user process, while users can always read the most recently written data.

- **Ease of Use**: Interacting with the database feels just like using a Python dictionary! You don't even have to worry about resource release.

- **Buffered Writing**: Data is buffered and scheduled for write to the database, reducing the overhead of frequent database writes.

- **High-Performance Database Backend**: Uses the high-performance key-value database LMDB as its default backend.

- **Atomic Operations**: Ensures that write operations are atomic, safeguarding data integrity.

- **Thread-Safety**: Employs only necessary locks to ensure safe concurrent access while balancing performance.

## TODO

- [ ] Client-Server Architecture
- [ ] Benchmark
---

## Quick Start

### Installation
```bash
pip install flaxkv
```
### Usage

```python
from flaxkv import dbdict
import numpy as np

d = dbdict('./test_db')
d[1] = 1
d[1.1] = 1 / 3
d['key'] = 'value'
d['a dict'] = {'a': 1, 'b': [1, 2, 3]}
d['a list'] = [1, 2, 3, {'a': 1}]
d[(1, 2, 3)] = [1, 2, 3]
d['numpy array'] = np.random.randn(100, 100)

d.setdefault('key', 'value_2')
assert d['key'] == 'value'

d.update({"key1": "value1", "key2": "value2"})

assert 'key2' in d

d.pop("key1")
assert 'key1' not in d

for key, value in d.items():
    print(key, value)

print(len(d))
```

You might have noticed that even when the program ends, we didn't use `d.close()` to release resources! 
Everything will be handled automatically.
More importantly, as a persistent database, it offers performance close to dictionary (in-memory) access!
(There should be a benchmark here..)

P.S.: Of course, you can also manually call `d.close()` to release resources immediately~.

## Citation
If `FlaxKV` has been helpful to your research, please cite:
```bibtex
@misc{flaxkv,
    title={FlaxKV: An Easy-to-use and High Performance Key-Value Database Solution},
    author={K.Y},
    howpublished = {\url{https://github.com/KenyonY/flaxkv}},
    year={2023}
}
```

## Contributions
Feel free to make contributions to this module by submitting pull requests or raising issues in the repository.

## License
`FlaxKV` is licensed under the [Apache-2.0 License](./LICENSE).


