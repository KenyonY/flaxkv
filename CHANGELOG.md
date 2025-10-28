# Changelog

所有重要的项目变更都将记录在此文件中。

本项目遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [Unreleased]

### 新增
- 添加 GitHub Actions CI/CD 自动化测试
- 添加安全警告文档（pickle 和远程连接风险）
- 添加 CHANGELOG.md 变更日志
- 添加服务器端TTL验证，网络请求减少50%

### 变更
- **依赖调整**: 将 pandas 移至可选依赖，numpy 保留为核心依赖
  - 基础安装: `pip install flaxkv2`
  - 完整安装: `pip install flaxkv2[pandas]` 或 `pip install flaxkv2[full]`
- 更新 pre-commit 配置到最新版本（black 24.3.0, isort 5.13.2）
- 更新架构文档，明确区分已实现功能和未来规划
- **TTL实现重构**: TTL信息现在作为普通键存储，简化架构并提升性能
  - 移除内存字典 `_expiry_dict`，所有TTL信息直接存储在数据库中
  - 本地和远程模式行为完全一致
  - 服务器端自动处理TTL验证，客户端无需额外检查

### 弃用 ⚠️
- **LevelDBDict 已弃用**: 将在未来版本中移除
  - 原因: 性能测试显示 RawLevelDBDict 在所有场景下都更快（提升13-25%）
  - 缓冲机制和索引功能未能带来性能提升，反而增加复杂度
  - 迁移: 直接替换为 `RawLevelDBDict` 或使用 `FlaxKV` 工厂类
  - 详见: `DEPRECATION_NOTICE.md`

### 移除
- **移除 LevelDBDict 中的布隆过滤器**: 在有内存缓冲区的设计中，布隆过滤器是多余的优化
  - 查询时先检查内存缓冲区，布隆过滤器的检查是纯开销
  - 初始化时扫描全部键很耗时
  - 不支持删除操作，导致假阳性累积
  - 移除后代码更简洁，性能不受影响（缓冲区已提供优化）

### 修复
- 修复序列化模块对可选依赖的处理
- 改进错误提示信息，指导用户安装缺失的依赖

### 安全
- 在 README 和架构文档中添加 pickle 序列化安全警告
- 在文档中明确说明远程连接的安全风险和最佳实践

## [0.1.0] - 2024-10-25

### 新增
- 基于 LevelDB 的高性能键值存储
- 支持本地和远程（ZeroMQ）两种后端
- RawLevelDBDict: 高性能简化版本
- LevelDBDict: 功能完整版本（包含缓冲、索引等）
- NestedDBDict: 嵌套字典支持
- TTL (Time-To-Live) 支持
- 自动关闭和上下文管理器支持
- 布隆过滤器优化查询性能
- 支持多种数据类型序列化（msgpack, pickle, NumPy, Pandas）
- ZeroMQ 远程服务器和客户端
- 命令行工具 `flaxkv2`

### 特性
- 类 Python 字典的 API 接口
- 线程安全
- 原子操作
- 实例复用机制
- 自动序列化/反序列化

---

## 版本说明

### 语义化版本格式

- **主版本号 (MAJOR)**: 不兼容的 API 变更
- **次版本号 (MINOR)**: 向后兼容的功能新增
- **修订号 (PATCH)**: 向后兼容的问题修复

### 变更类型

- **新增 (Added)**: 新功能
- **变更 (Changed)**: 现有功能的变更
- **弃用 (Deprecated)**: 即将移除的功能
- **移除 (Removed)**: 已移除的功能
- **修复 (Fixed)**: 错误修复
- **安全 (Security)**: 安全相关的修复

---

[Unreleased]: https://github.com/KenyonY/flaxkv2/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/KenyonY/flaxkv2/releases/tag/v0.1.0

