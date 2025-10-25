# FlaxKV2 测试指南

## 快速开始

### 1. 安装依赖

```bash
# macOS 系统需要先安装 leveldb
brew install leveldb

# 安装 Python 依赖
pip install -r requirements.txt

# 如果遇到 plyvel 编译问题，使用以下命令
CPLUS_INCLUDE_PATH=/opt/homebrew/include \
LIBRARY_PATH=/opt/homebrew/lib \
pip install plyvel
```

### 2. 运行测试

```bash
# 运行所有测试
pytest tests/ -v

# 仅运行核心功能测试
pytest tests/test_core.py -v

# 仅运行重构相关测试
pytest tests/test_refactoring.py -v

# 运行远程客户端测试（需要先启动服务器）
pytest tests/test_remote.py -v
```

### 3. 快速验证

如果你只想快速验证修复是否生效，可以运行：

```bash
python test_quick.py
```

这个脚本会测试：
- 缓冲机制是否正常工作
- 默认TTL功能是否有效
- 上下文管理器是否正确

## 测试覆盖

### 核心功能测试 (`tests/test_core.py`)
- ✅ 基本的增删改查操作
- ✅ NumPy数组支持
- ✅ Pandas DataFrame支持
- ✅ 批量操作
- ✅ TTL功能
- ✅ 数据库重建
- ✅ 哈希索引
- ✅ 范围索引
- ✅ 索引更新

### 重构测试 (`tests/test_refactoring.py`)
- ✅ 缓冲机制
  - 缓冲区启用测试
  - 缓冲区溢出触发写入
  - 手动刷新缓冲区
  - 批量更新使用缓冲
- ✅ 默认TTL
  - 单个键自动应用TTL
  - 批量更新自动应用TTL
  - 动态修改默认TTL
  - 无默认TTL行为
- ✅ 上下文管理器
  - 基本使用
  - 异常处理
- ✅ 数据持久化
  - 写入模式关闭
  - 非写入模式关闭

### 远程客户端测试 (`tests/test_remote.py`)
- ✅ 远程连接
- ✅ 远程操作
- ✅ 远程TTL
- ✅ 错误处理

## 常见问题

### Q: plyvel 导入失败

**错误信息：**
```
ImportError: dlopen(.../_plyvel.cpython-312-darwin.so, 0x0002):
symbol not found in flat namespace '__ZTIN7leveldb10ComparatorE'
```

**解决方案：**
```bash
# 1. 确保 leveldb 已安装
brew install leveldb

# 2. 重新编译 plyvel
pip uninstall plyvel -y
CPLUS_INCLUDE_PATH=/opt/homebrew/include \
LIBRARY_PATH=/opt/homebrew/lib \
LDFLAGS="-L/opt/homebrew/lib" \
CPPFLAGS="-I/opt/homebrew/include" \
pip install --no-cache-dir plyvel

# 3. 如果仍然失败，尝试设置运行时库路径
export DYLD_LIBRARY_PATH=/opt/homebrew/lib:$DYLD_LIBRARY_PATH
```

### Q: pandas/numpy 未安装

**解决方案：**
```bash
pip install pandas numpy
```

### Q: 测试超时

某些TTL测试需要等待键过期，这是正常的。默认超时为30秒。

### Q: 远程测试失败

远程测试需要先启动FlaxKV服务器：

```bash
# 终端1：启动服务器
python -m flaxkv2.server

# 终端2：运行测试
pytest tests/test_remote.py -v
```

## 性能测试

```bash
# 运行性能测试
pytest tests/test_performance.py -v
```

## 生成测试覆盖率报告

```bash
# 安装覆盖率工具
pip install pytest-cov

# 生成HTML报告
pytest --cov=flaxkv2 --cov-report=html tests/

# 查看报告
open htmlcov/index.html
```

## 持续集成

测试配置文件位于 `pytest.ini`。

在CI环境中运行：
```bash
pytest tests/ --tb=short --strict-markers -v
```

## 调试技巧

### 1. 详细日志输出
```bash
pytest tests/ -v -s --log-cli-level=DEBUG
```

### 2. 只运行失败的测试
```bash
pytest --lf
```

### 3. 停止在第一个失败
```bash
pytest -x
```

### 4. 运行特定测试
```bash
pytest tests/test_refactoring.py::TestBufferingMechanism::test_buffering_enabled -v
```

## 贡献测试

添加新测试时，请遵循：
1. 使用有意义的测试名称（`test_specific_feature_behavior`）
2. 添加清晰的文档字符串
3. 使用fixtures管理测试资源
4. 确保测试是独立的（不依赖其他测试的执行顺序）
5. 清理临时文件和目录

## 更多信息

查看 `REFACTORING_SUMMARY.md` 了解最近的重构修复详情。
