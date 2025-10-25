# FlaxKV 后端类型重构总结

## 重构目标

将 FlaxKV 的后端类型实现从 `flaxkv2/core/base.py` 移至 `flaxkv2/__init__.py`，使架构更加清晰。

## 重构内容

### 1. 架构优化

**之前的结构：**
```
flaxkv2/
├── __init__.py          # 简单导入 FlaxKV
└── core/
    └── base.py          # 包含 BaseDBDict + FlaxKV + BackendType
```

**重构后的结构：**
```
flaxkv2/
├── __init__.py          # FlaxKV 工厂类 + BackendType 枚举
└── core/
    └── base.py          # 只包含 BaseDBDict 基类
```

### 2. 职责分离

- **`flaxkv2/__init__.py`**: 
  - 公共API入口
  - `FlaxKV` 工厂类（负责创建不同后端实例）
  - `BackendType` 枚举（定义支持的后端类型）

- **`flaxkv2/core/base.py`**: 
  - `BaseDBDict` 抽象基类（定义数据库字典的统一接口）

- **`flaxkv2/core/raw_leveldb_dict.py`**: 
  - `RawLevelDBDict` 本地后端实现

- **`flaxkv2/client/remote.py`**: 
  - `RemoteDBDict` 远程后端实现

### 3. 改进的 FlaxKV 工厂类

#### 新增功能：

1. **后端类型枚举**
```python
class BackendType:
    """后端类型枚举"""
    REMOTE = "remote"
    LOCAL = "local"
```

2. **自动检测后端类型**
```python
@staticmethod
def _detect_backend_type(root_path_or_url: str) -> str:
    """根据 URL 自动检测是本地还是远程后端"""
    if root_path_or_url.startswith(("http://", "https://")):
        return BackendType.REMOTE
    return BackendType.LOCAL
```

3. **分离的后端创建方法**
```python
@staticmethod
def _create_local_backend(...):
    """创建本地 LevelDB 后端"""
    
@staticmethod
def _create_remote_backend(...):
    """创建远程 HTTP 后端"""
```

4. **增强的参数验证**
- 支持显式指定 `backend` 参数
- 自动验证参数与检测结果的一致性
- 提供清晰的错误消息

### 4. 使用示例

#### 本地后端
```python
from flaxkv2 import FlaxKV

# 自动检测（推荐）
db = FlaxKV("mydb", "./data")

# 显式指定
db = FlaxKV("mydb", "/var/lib/flaxkv", backend='local')

# 带选项
db = FlaxKV("mydb", "./data", 
            raw=True,              # 原始模式
            auto_nested=True,      # 自动嵌套
            default_ttl=3600)      # 默认TTL
```

#### 远程后端
```python
from flaxkv2 import FlaxKV

# 自动检测（推荐）
db = FlaxKV("mydb", "http://localhost:8000")

# 显式指定
db = FlaxKV("mydb", "https://api.example.com", 
            backend='remote',
            root_path="/data")
```

#### 使用 BackendType 枚举
```python
from flaxkv2 import FlaxKV, BackendType

# 检查后端类型
backend_type = FlaxKV._detect_backend_type("http://localhost:8000")
if backend_type == BackendType.REMOTE:
    print("远程后端")
```

## 优势

### 1. 更清晰的架构
- **单一职责**: 每个模块只负责一件事
- **易于理解**: 用户只需从 `flaxkv2` 导入，无需关心内部结构
- **易于维护**: 工厂类和基类分离，修改更安全

### 2. 更好的可扩展性
- 添加新后端类型只需：
  1. 在 `BackendType` 添加枚举值
  2. 添加 `_create_xxx_backend` 方法
  3. 在 `__new__` 中添加分支

### 3. 更友好的API
- 所有公共API都在顶层可用
- 导入路径更短：`from flaxkv2 import FlaxKV`
- 类型提示更完整

### 4. 向后兼容
- 所有现有代码无需修改
- `from flaxkv2 import FlaxKV` 依然有效
- 功能完全保持一致

## 测试验证

所有基本功能测试通过：
- ✓ 后端类型自动检测
- ✓ 本地后端创建和操作
- ✓ 参数传递（raw, auto_nested, default_ttl, rebuild）
- ✓ 后端类型验证和错误处理
- ✓ BackendType 枚举

## 文件变更

### 修改的文件：
1. `flaxkv2/__init__.py` - 添加 FlaxKV 和 BackendType
2. `flaxkv2/core/base.py` - 移除 FlaxKV 和 BackendType
3. `flaxkv2/core/__init__.py` - 更新导出列表

### 影响：
- 对外API保持不变
- 内部导入路径更清晰
- 代码组织更合理

## 未来改进建议

1. **添加更多后端类型**
   - Redis 后端
   - SQLite 后端
   - 内存后端（用于测试）

2. **增强类型提示**
   - 为不同后端返回类型添加 TypeVar
   - 使用 Protocol 定义后端接口

3. **配置管理**
   - 支持从配置文件加载后端设置
   - 环境变量配置

4. **性能监控**
   - 添加后端性能指标收集
   - 支持切换后端的热重载

## 总结

这次重构成功地将 FlaxKV 的架构优化为更清晰、更易维护的结构。通过将工厂类移至包的入口，我们实现了：

- ✅ 更好的代码组织
- ✅ 更清晰的职责分离
- ✅ 更友好的用户API
- ✅ 完全的向后兼容
- ✅ 更好的可扩展性

重构后的代码更符合软件工程的最佳实践，为未来的功能扩展打下了良好的基础。

