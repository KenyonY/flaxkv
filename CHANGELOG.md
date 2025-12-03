# Changelog

本文件记录 FlaxKV2 的所有重要变更。

格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.0.0/)，
本项目遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

---

## [0.1.0] - 2025-11

### 新增

#### 核心功能
- **RawLevelDBDict**: 无缓存本地后端，简单可靠
- **CachedLevelDBDict**: 智能缓存后端，提供 9-13x 性能提升
- **NestedDBDict / NestedDBList**: 嵌套字典/列表支持
- **TTL 自动过期**: 支持键级别的过期时间设置
- **自动嵌套检测**: `auto_nested=True` 自动识别嵌套结构

#### 远程访问
- **FlaxKVServer**: ZeroMQ 服务器端
- **AsyncRemoteDBDict**: 异步远程客户端（核心实现）
- **RemoteDBDict**: 同步远程客户端（包装器，向后兼容）
- **AsyncConnectionPool**: 异步连接池，支持并发操作
- **CurveZMQ 加密**: 基于 NaCl/Curve25519 的端到端加密
- **LZ4 压缩**: 可选的网络传输压缩
- **密码认证**: 两种方案（确定性派生密钥 / 文件存储密钥）

#### 序列化
- **msgpack**: 基本类型的高效序列化
- **NumPy 原生支持**: 保留 dtype/shape 的二进制序列化
- **Pandas 支持**: DataFrame 的高效存储（可选依赖）
- **类型缓存优化**: 15-30% 编码性能提升

#### 工具
- **CLI 命令行**: `flaxkv2 run/kill/version/inspect` 等命令
- **Inspector**: 数据可视化和管理工具
- **Web UI**: 基于 Flask 的浏览器界面（可选依赖）
- **性能配置文件**: 6 种预设（balanced/read_optimized/write_optimized 等）

### 性能优化

- **P0: LevelDB 配置优化**: 4-25% 性能提升
  - 256MB LRU 缓存（默认 8MB）
  - 布隆过滤器（10 bits/key）
  - 优化块大小（16KB）

- **P1: 智能类型缓存**: 14-30% 编码性能提升
  - 缓存每个 Python 类型的最佳编码器
  - 99%+ 缓存命中率

- **TTL 内嵌重构**: 减少 50% I/O 操作
  - TTL 元数据与值一起存储

### 文档

- **核心设计文档**: 项目设计理念和关键决策
- **架构设计文档**: 完整的系统架构和数据流
- **键值编码详解**: 存储格式的技术细节
- **性能优化计划**: P0-P3 优化路线图
- **密码认证指南**: 安全配置最佳实践

---

## [未发布]

### 计划中

- P1: 批量写入队列
- P1: TTL 写入合并
- P2: 读写锁分离
- P2: 监控和统计接口
- P3: Unix Domain Socket 支持
- P3: 服务端批量写入优化

---

**完整文档**: [docs/README.md](docs/README.md)
