# FlaxKV2 日志配置指南

## 设计理念

FlaxKV2 作为基础库，遵循 Python 库日志配置的最佳实践：

- **默认静默**：导入库时不会有任何日志输出，不会污染应用程序的终端
- **灵活控制**：应用程序可以完全控制是否启用日志和日志级别
- **避免冲突**：与应用程序自己的 loguru 配置不冲突

## 使用场景

### 场景1：在生产应用中使用（默认行为）

当你将 FlaxKV2 作为依赖库导入时，默认不会有任何日志输出：

```python
from flaxkv2 import LevelDBDict

# 正常使用，没有任何日志输出
db = LevelDBDict("mydb")
db["key"] = "value"
```

### 场景2：调试时启用日志

如果需要调试 FlaxKV2 的内部行为，可以通过两种方式启用日志：

**方式1：通过代码启用**

```python
from flaxkv2.utils.log import enable_logging

# 启用日志，级别为 INFO
enable_logging(level="INFO")

# 或者启用 DEBUG 级别查看更详细的信息
enable_logging(level="DEBUG")

# 使用 FlaxKV2
from flaxkv2 import LevelDBDict
db = LevelDBDict("mydb")
```

**方式2：通过环境变量启用**

```bash
# 启用日志
export FLAXKV_ENABLE_LOGGING=1
export FLAXKV_LOG_LEVEL=DEBUG  # 可选，默认 WARNING

python your_script.py
```

### 场景3：将日志输出到文件

```python
from flaxkv2.utils.log import enable_logging, add_file_log

# 启用控制台日志
enable_logging(level="INFO")

# 添加文件日志（更详细的 DEBUG 级别）
add_file_log(
    "flaxkv2.log",
    level="DEBUG",
    rotation="10 MB",    # 日志文件达到 10MB 时轮转
    retention="7 days"   # 保留 7 天的日志
)
```

### 场景4：在已有 loguru 配置的应用中使用

FlaxKV2 使用的是全局的 loguru logger，因此与应用程序共享同一个 logger 实例。

**如果应用程序已经配置了 loguru**，FlaxKV2 会使用应用程序的配置：

```python
from loguru import logger

# 应用程序的 loguru 配置
logger.remove()
logger.add("app.log", level="INFO")

# 导入 FlaxKV2，它会使用上面的配置
from flaxkv2 import LevelDBDict
db = LevelDBDict("mydb")  # FlaxKV2 的日志会输出到 app.log
```

**如果需要分别控制应用和 FlaxKV2 的日志**，可以使用 loguru 的过滤功能：

```python
from loguru import logger

logger.remove()

# 应用日志：所有级别，排除 flaxkv2
logger.add(
    "app.log",
    level="INFO",
    filter=lambda record: "flaxkv2" not in record["name"]
)

# FlaxKV2 日志：只记录 WARNING 以上，只包含 flaxkv2
logger.add(
    "flaxkv2.log",
    level="WARNING",
    filter=lambda record: "flaxkv2" in record["name"]
)
```

### 场景5：临时禁用日志

```python
from flaxkv2.utils.log import enable_logging, disable_logging

# 启用日志
enable_logging(level="DEBUG")

# ... 一些操作 ...

# 临时禁用日志（例如在执行大批量操作时）
disable_logging()

# 大批量操作，不产生日志
for i in range(100000):
    db[f"key_{i}"] = f"value_{i}"

# 重新启用日志
enable_logging(level="INFO")
```

## API 参考

### `enable_logging(level="INFO", format_str=None)`

启用 FlaxKV2 日志输出。

**参数：**
- `level`: 日志级别，可选 "DEBUG", "INFO", "WARNING", "ERROR"，默认 "INFO"
- `format_str`: 自定义格式字符串，默认为彩色格式

**示例：**
```python
from flaxkv2.utils.log import enable_logging

# 使用默认格式
enable_logging(level="DEBUG")

# 自定义格式
enable_logging(
    level="INFO",
    format_str="{time} | {level} | {message}"
)
```

### `disable_logging()`

禁用 FlaxKV2 日志输出。

### `add_file_log(filepath, level="DEBUG", rotation="10 MB", retention="7 days")`

添加文件日志输出。

**参数：**
- `filepath`: 日志文件路径
- `level`: 日志级别
- `rotation`: 轮转条件，如 "10 MB", "1 day", "00:00"
- `retention`: 保留时间，如 "7 days", "1 week"

### `set_log_level(level)`

更改日志级别（需要先启用日志）。

## 环境变量

- `FLAXKV_ENABLE_LOGGING`: 设置为 "1" 启用日志（默认禁用）
- `FLAXKV_LOG_LEVEL`: 设置日志级别，如 "DEBUG", "INFO", "WARNING"（默认 "WARNING"）

## 常见问题

### Q: 为什么我看不到日志输出？

A: 默认情况下 FlaxKV2 不输出日志。需要调用 `enable_logging()` 或设置环境变量 `FLAXKV_ENABLE_LOGGING=1`。

### Q: 如何在 pytest 中查看 FlaxKV2 的日志？

A: 在测试文件开头添加：

```python
from flaxkv2.utils.log import enable_logging
enable_logging(level="DEBUG")
```

或者使用环境变量：

```bash
FLAXKV_ENABLE_LOGGING=1 FLAXKV_LOG_LEVEL=DEBUG pytest tests/
```

### Q: FlaxKV2 的日志会影响我的应用日志吗？

A: 默认情况下不会，因为 FlaxKV2 默认不输出日志。如果你启用了 FlaxKV2 日志，它们会使用相同的 loguru 实例，可以通过过滤器分离。

### Q: 如何在生产环境中只记录错误？

A:
```python
from flaxkv2.utils.log import enable_logging
enable_logging(level="ERROR")
```

或使用环境变量：
```bash
export FLAXKV_ENABLE_LOGGING=1
export FLAXKV_LOG_LEVEL=ERROR
```
