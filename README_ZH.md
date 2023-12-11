
<h1 align="center">
    <br>
    🗲  FlaxKV
</h1>

<p align="center">
一个高性能字典数据库
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
        <b>简体中文</b> |
        <a href="https://github.com/KenyonY/flaxkv/blob/main/README.md">English</a> 
    </p>
</h4>

<p >
<br>
</p>



`flaxkv` 提供了一个非常类似字典的接口，用于与高性能键值数据库进行交互。更重要的是，它作为持久化数据库提供了接近原生字典(内存)存取的性能。
你可以直接将它当成python字典来使用而不必担心在任何时候操作数据库时会阻塞你的用户进程。

---

## 主要特征

- **保持最新,永不阻塞**：它被设计的初衷便是不让任何写入操作阻塞用户进程，同时用户永远可以读取到最新写入的数据。

- **易于使用**：与数据库交互，就像使用 Python 字典一样！你甚至不用关心它的资源释放

- **缓冲写入**：数据将被缓冲写入及计划写入数据库，减少数据库频繁写入的开销。

- **高性能数据库后端**: 使用高性能键值数据库 LMDB 作为默认后端。

- **原子操作**：确保写入操作是原子的，保证数据完整性。

- **线程安全**：仅使用必要的锁来确保安全的并发访问同时又能兼顾性能。


## TODO

- [x] 客户端-服务器架构
- [ ] 性能测试

---

## 快速入门

### 安装
```bash
pip install flaxkv
```
### 使用

```python
from flaxkv import dictdb
import numpy as np

db = dictdb('test_db')
# or run server `flaxkv run --port 8000`, then:
# db = dictdb('test_db', root_path_or_url='http://localhost:8000')

db[1] = 1
db[1.1] = 1 / 3
db['key'] = 'value'
db['a dict'] = {'a': 1, 'b': [1, 2, 3]}
db['a list'] = [1, 2, 3, {'a': 1}]
db[(1, 2, 3)] = [1, 2, 3]
db['numpy array'] = np.random.randn(100, 100)

db.setdefault('key', 'value_2')
assert db['key'] == 'value'

db.update({"key1": "value1", "key2": "value2"})

assert 'key2' in db

db.pop("key1")
assert 'key1' not in db

for key, value in db.items():
    print(key, value)

print(len(db))
```


### Tips

- `flaxkv`作为持久化数据库提供了接近原生字典(内存)存取的性能！(这里应存在一个benchmark)
- 也许你注意到在前面的示例代码中并没有使用到`db.close()`来进行资源释放！因为这一切都将被`flaxkv`自动处理。 当然也可以手动调用 `db.close()` 来立即释放资源
- 由于`flaxkv`通过缓冲写入的方式来保存数据，这种延迟写入的特性在一些场景(如jupyter中)下将不能及时将数据写入磁盘，
此时可使用`db.write_immediately()`来立即触发写入操作。

### Benchmark
todo


### 适用场景

- **键-值型结构**：
  用于保存简单的键值结构数据
- **高频写入**：
  非常适合需要高频插入/更新数据的场景
- **机器学习**：
  `flaxkv`十分适合用于保存机器学习中的各种嵌入向量、图像、文本和其它键-值结构的大型数据集。


## 引用
如果`FlaxKV`对你的研究有帮助，欢迎引用：
```bibtex
@misc{flaxkv,
    title={FlaxKV: An Easy-to-use and High Performance Key-Value Database Solution},
    author={K.Y},
    howpublished = {\url{https://github.com/KenyonY/flaxkv}},
    year={2023}
}
```

## 贡献
欢迎通过提交拉取请求或在仓库中提出问题来为此模块做出贡献。

## License
`FlaxKV`遵循[Apache-2.0开源协议](./LICENSE)
