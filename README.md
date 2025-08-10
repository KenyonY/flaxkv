# FlaxKV 2.0

> 新一代高性能持久化字典 - 企业级LevelDB存储，智能缓存，ACID事务支持

## 🚀 特性亮点

- **🎯 原生字典接口** - 像使用Python字典一样简单
- **⚡ 极致性能** - LevelDB存储 + 智能缓存 + 异步I/O  
- **🔄 多接口支持** - 异步接口 + 同步接口 + 完全兼容dict的FlaxDict
- **💾 企业级可靠性** - ACID事务，数据完整性保证，自动恢复
- **🧠 智能优化** - 自适应缓存，预测性预加载，性能监控
- **🔧 高度可配置** - 开发/生产/高性能多种预设配置
- **📊 完整监控** - 详细的性能指标和运维统计

## 📦 快速安装

```bash
# 基础安装
pip install flaxkv

# 安装开发工具
pip install flaxkv[dev]
```

## 🎯 基本用法

### 异步接口 (推荐)

```python
import asyncio
import flaxkv

async def main():
    # 创建数据库
    config = flaxkv.FlaxKVConfig.for_production()
    async with flaxkv.FlaxKV("mydb", config=config) as db:
        # 基本操作
        await db.set("user:123", {"name": "Alice", "age": 25})
        user = await db.get("user:123")
        print(user)  # {"name": "Alice", "age": 25}
        
        # 批量操作
        await db.mset({
            "user:124": {"name": "Bob", "age": 30},
            "user:125": {"name": "Charlie", "age": 35}
        })
        
        users = await db.mget(["user:123", "user:124", "user:125"])
        print(f"找到 {len(users)} 个用户")

asyncio.run(main())
```

### 事务支持

```python
async def transfer_money(db, from_account, to_account, amount):
    """转账操作 - 原子性保证"""
    async with db.transaction() as tx:
        # 检查余额
        from_balance = await tx.get(f"account:{from_account}")
        if from_balance < amount:
            raise ValueError("余额不足")
        
        # 执行转账
        await tx.set(f"account:{from_account}", from_balance - amount)
        
        to_balance = await tx.get(f"account:{to_account}")
        await tx.set(f"account:{to_account}", to_balance + amount)
        
        # 事务自动提交，保证原子性
```

### 同步接口

```python
import flaxkv

# 创建线程安全的同步接口
config = flaxkv.FlaxKVConfig.for_production()
with flaxkv.ThreadSafeFlaxKV("mydb", config=config) as db:
    # 基本操作 - 完全同步调用
    db.set("user:123", {"name": "Alice", "age": 25})
    user = db.get("user:123")
    print(user)  # {"name": "Alice", "age": 25}
    
    # 字典式操作
    db["settings:theme"] = "dark"
    theme = db["settings:theme"]
    print(theme)  # "dark"
    
    # 批量操作
    db.mset({
        "product:1": {"name": "Laptop", "price": 999},
        "product:2": {"name": "Mouse", "price": 29}
    })
    
    products = db.mget(["product:1", "product:2"])
    print(f"找到 {len(products)} 个产品")
    
    # 事务支持
    with db.transaction() as tx:
        tx.set("account:alice", 100)
        tx.set("account:bob", 200)
        # 自动提交

# 在同步应用框架中使用
app_db = flaxkv.ThreadSafeFlaxKV("app_database")

def handle_user_request(user_id, data):
    """业务逻辑函数 - 纯同步调用"""
    # 存储用户数据
    app_db.set(f"user:{user_id}", data)
    
    # 获取并返回
    return app_db.get(f"user:{user_id}")
```

### 字典式接口 (FlaxDict)

```python
import flaxkv

# 创建持久化字典 - 完全兼容Python dict接口
db = flaxkv.FlaxDict('mydict')

with db:
    # 像使用普通字典一样
    db['key'] = 'value'
    db['data'] = {'nested': [1, 2, 3]}
    
    # 所有dict方法都支持
    db.setdefault('key', 'value_2')  # 返回 'value'
    db.update({"key1": "value1", "key2": "value2"})
    
    assert 'key2' in db
    assert len(db) == 4
    
    # 删除和弹出
    value = db.pop("key1")
    del db['key']
    
    # 迭代和视图
    for key, value in db.items():
        print(key, value)
    
    # 批量操作 (高性能扩展)
    db.mset({f'item_{i}': i for i in range(1000)})
    results = db.mget(['item_0', 'item_500', 'item_999'])
    
    # 事务支持
    with db.transaction() as tx:
        tx.set('account:alice', 100)
        tx.set('account:bob', 200)

# FlaxDict可以直接替换dict使用，同时获得持久化能力！
```

> **性能说明：** 
> - ThreadSafeFlaxKV: 基于全局后台事件循环，批量操作性能 写入1000条 ~10ms，读取1000条 ~3ms
> - FlaxDict: 完全兼容Python dict接口，批量写入10000条记录 ~40ms，支持所有dict方法

## 🔥 架构特点

FlaxKV 2.0 采用现代分层架构，支持多种使用接口：

```
┌─────────────────────────────────────────┐
│    字典接口层 (FlaxDict)                │  ← 完全兼容Python dict
├─────────────────────────────────────────┤
│   同步封装层 (ThreadSafeFlaxKV)         │  ← 线程安全，后台事件循环
├─────────────────────────────────────────┤
│        用户 API 层 (FlaxKV)             │  ← dict接口，异步操作
├─────────────────────────────────────────┤
│          事务管理层                     │  ← ACID事务，锁管理
├─────────────────────────────────────────┤
│         智能缓存层                      │  ← LRU/LFU/ARC，预加载
├─────────────────────────────────────────┤
│        高性能缓冲层                     │  ← 批量写入，WAL保证
├─────────────────────────────────────────┤
│        存储引擎层                       │  ← LevelDB优化
└─────────────────────────────────────────┘
```

**多接口设计：**
- **异步接口 (FlaxKV)** - 直接访问底层异步架构，性能最优
- **同步接口 (ThreadSafeFlaxKV)** - 基于后台事件循环的线程安全封装
- **字典接口 (FlaxDict)** - 完全兼容Python dict接口，可直接替换dict使用

## 🚧 开发和测试

### 环境设置

```bash
# 安装开发依赖
pip install -e .[dev]

# 运行测试
pytest -v

# 代码格式化
black .
isort .
```

## 📄 许可协议

本项目采用 [MIT 许可协议](LICENSE)。

---

<p align="center">
  <strong>FlaxKV 2.0 - 让持久化存储像字典一样简单</strong>
</p>