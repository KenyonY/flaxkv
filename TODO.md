# FlaxKV2 项目 TODO

本文档记录FlaxKV2项目的已完成工作和待办事项。

**最后更新**: 2025-10-31

---

## ✅ 已完成 (Completed)

### 核心功能

- [x] **RawLevelDBDict** - 高性能无缓冲后端
  - 直接写入LevelDB
  - 支持TTL、auto_nested
  - 实例复用机制
  - 线程安全

- [x] **LevelDBDict** - 功能完整版后端（已弃用）
  - 缓冲机制（性能测试显示慢62.9%）
  - 布隆过滤器（已移除，不必要的开销）
  - 标记为deprecated

- [x] **NestedDBDict** - 嵌套字典实现
  - 基于LevelDB prefixed_db
  - 独立字段序列化
  - 支持递归嵌套
  - 实现MutableMapping接口
  - 添加copy()、__eq__()方法

- [x] **FlaxKV工厂类** - 统一入口
  - 自动检测本地/远程后端
  - 支持auto_nested、TTL等所有功能
  - 便捷的参数传递

### 序列化系统

- [x] **智能序列化** - 自动类型检测
  - msgpack优先（基础类型）
  - NumPy数组特殊处理
  - Pandas DataFrame支持（可选依赖）
  - pickle回退（复杂对象）

- [x] **P1: 类型缓存优化** - 编码性能提升15-30%
  - 缓存每个类型的最佳编码器
  - 避免重复try-except开销
  - 缓存命中率99%+
  - 线程安全实现

- [x] **键编码** - 保证顺序性质
  - 支持str、int、float、bytes、tuple
  - 整数大端序编码
  - 支持范围查询

### TTL功能

- [x] **TTL内嵌重构** - I/O操作减少50%
  - TTL信息嵌入value字节流
  - 版本化编码格式（VERSION + FLAGS）
  - 向后兼容旧格式
  - auto_nested + TTL自动支持

- [x] **TTL管理** - TTLManager类
  - 基本TTL设置（set_ttl, get_ttl, remove_ttl）
  - 默认TTL支持（default_ttl参数）
  - TTL持久化（存储在同一数据库）
  - 自动过期检查和清理

- [x] **服务器端TTL验证** - 网络请求减少50%
  - 服务器端检查TTL过期
  - 客户端代码简化
  - 时间以服务器为准

### 远程访问 (ZeroMQ)

- [x] **RemoteDBDict客户端**
  - ZeroMQ REQ socket
  - 自动重连机制
  - 超时控制和重试
  - 支持所有核心操作

- [x] **FlaxKVServer服务器**
  - ZeroMQ ROUTER socket（多客户端）
  - 数据库实例管理
  - 只处理二进制数据（不序列化）
  - 统计信息收集

- [x] **CurveZMQ加密** - 端到端加密
  - CurveZMQ协议集成
  - 密码认证（两种方案）
  - 密钥管理工具

- [x] **密码派生密钥** - 跨机器部署友好
  - PBKDF2-HMAC-SHA256密钥派生
  - 相同密码生成相同密钥
  - 无需复制密钥文件
  - 推荐为默认方案

- [x] **LZ4压缩** - 网络传输优化
  - 大消息自动压缩
  - 可配置压缩阈值
  - 减少50-70%带宽

- [x] **客户端LRU缓存** - 减少网络请求
  - SimpleLRUCache实现
  - 可配置缓存大小
  - 写穿透，读缓存

### 性能优化

- [x] **P0: LevelDB配置优化** - 4-25%性能提升
  - 256MB LRU缓存（从默认8MB）
  - 布隆过滤器（10 bits）
  - 优化块大小（16KB）
  - 6种预设配置文件
  - 性能测试验证

- [x] **性能配置管理** - flaxkv2/config.py
  - balanced（默认）
  - read_optimized（读密集型）
  - write_optimized（写密集型）
  - memory_constrained（内存受限）
  - large_database（大数据库）
  - ml_workload（机器学习）

### 工程实践

- [x] **依赖管理优化**
  - pandas移至可选依赖
  - numpy保留为核心依赖
  - 安装选项：basic/pandas/full

- [x] **CI/CD自动化**
  - GitHub Actions工作流
  - 多平台测试（Ubuntu, macOS, Windows）
  - 多Python版本（3.8-3.12）
  - 代码覆盖率报告

- [x] **代码质量工具**
  - pre-commit hooks（black, isort）
  - 更新到最新版本（2025）
  - flake8静态分析

- [x] **自动关闭机制**
  - atexit注册清理函数
  - 上下文管理器支持
  - 实例管理器

- [x] **安全警告文档**
  - Pickle序列化风险说明
  - 远程连接安全最佳实践
  - 密码强度要求

### 文档

- [x] **架构文档** - ARCHITECTURE.md
  - 完整的架构设计说明
  - 组件关系图
  - 数据流图

- [x] **CHANGELOG** - 版本变更记录
  - 遵循Keep a Changelog规范
  - 记录所有重要变更

- [x] **CONTRIBUTING** - 贡献指南
  - 开发环境设置
  - 代码风格要求
  - PR提交流程

- [x] **CLAUDE.md** - Claude Code集成
  - 项目概述
  - 开发命令
  - 架构要点

- [x] **PASSWORD_AUTH_GUIDE** - 密码认证指南
  - 两种方案对比
  - 使用示例
  - 安全建议

- [x] **核心设计文档** - CORE_DESIGN.md
  - 设计理念
  - 核心组件详解
  - 关键设计决策

### 测试

- [x] **测试结构整理**
  - tests/unit/ - 单元测试
  - tests/integration/ - 集成测试
  - tests/benchmarks/ - 性能测试

- [x] **核心功能测试**
  - test_core.py - RawLevelDBDict基础功能
  - test_special_values.py - 特殊值处理
  - test_nested_dict.py - 嵌套字典

- [x] **TTL测试**
  - test_raw_leveldb_ttl.py - 基本TTL
  - test_default_ttl.py - 默认TTL
  - test_ttl_persistence.py - TTL持久化
  - test_ttl_refactor.py - TTL重构验证
  - test_ttl_auto_nested_bug.py - TTL+auto_nested边缘情况

- [x] **远程访问测试**
  - test_zmq_remote.py - ZeroMQ基础功能
  - test_cache_basic.py - 客户端缓存
  - test_server_ttl.py - 服务器端TTL
  - test_encryption_compression.py - 加密和压缩
  - test_password_auth.py - 密码认证
  - test_derive_password.py - 密码派生

- [x] **性能基准测试**
  - benchmark_cache.py - 缓存性能
  - benchmark_nested.py - 嵌套字典性能
  - benchmark_config_profiles.py - 配置文件对比

---

## 🚧 进行中 (In Progress)

### 文档整理

- [ ] **docs/README.md** - 文档索引（待创建）
  - 列出所有文档及其用途
  - 提供快速导航

---

## 📋 待办事项 (Todo)

### 高优先级 (P1)

- [ ] **批量写入队列** - 预期30-100%写入性能提升
  - 自动批处理后台线程
  - 可配置batch_size和interval
  - 支持flush()手动刷新
  - 权衡：增加5-10ms写入延迟

  **实施计划**:
  ```python
  db = FlaxKV("mydb", auto_batch=True, batch_size=1000, batch_interval_ms=10)
  for i in range(100000):
      db[f'key{i}'] = f'value{i}'  # 自动批处理
  db.flush(wait=True)  # 手动刷新
  ```

- [ ] **TTL写入合并** - 带TTL的写入性能提升~40%
  - 使用单个WriteBatch同时写入数据和TTL
  - 减少50%磁盘I/O

  **实施计划**:
  ```python
  # 当前：两次put
  db.put(key, value)
  db.put(ttl_key, ttl_value)

  # 优化后：单个batch
  batch = db.write_batch()
  batch.put(key, value)
  batch.put(ttl_key, ttl_value)
  batch.write()
  ```

### 中优先级 (P2)

- [ ] **读写锁分离** - 并发读性能提升100-200%
  - RWLock替代RLock
  - 允许多个读操作并发
  - 写操作独占锁

  **实施计划**:
  ```python
  class RWLock:
      def acquire_read(self): ...
      def release_read(self): ...
      def acquire_write(self): ...
      def release_write(self): ...
  ```

- [ ] **监控和统计** - 可观测性增强
  - 操作计数（读/写/删除）
  - 性能指标（吞吐量、延迟）
  - 缓存命中率
  - TTL过期统计
  - 可选Prometheus导出

- [ ] **文档网站** - 提升用户体验
  - 使用MkDocs或Sphinx
  - 自动从代码生成API文档
  - 部署到GitHub Pages
  - 中英文双语支持

### 低优先级 (P3)

- [ ] **安全增强**
  - safe_mode参数（禁用pickle）
  - 访问控制列表（ACL）
  - 审计日志

- [ ] **备份和恢复**
  - 自动备份机制
  - 增量备份支持
  - 快照功能
  - 恢复工具

- [ ] **高级查询**
  - 二级索引
  - 范围查询优化
  - 前缀匹配
  - 批量扫描

---

## 🔮 长期规划 (Future / Need Evaluation)

需要用户需求调研和评估的功能：

- [ ] **集群支持**
  - 主从复制
  - 数据分片
  - 一致性哈希
  - 故障转移

- [ ] **事务支持**
  - 多键原子操作
  - 乐观锁/悲观锁
  - ACID保证

- [ ] **全文搜索**
  - 集成Whoosh或Elasticsearch
  - 倒排索引
  - 分词支持

- [ ] **图形化管理工具**
  - Web管理界面
  - 数据浏览器
  - 性能监控面板

---

## ❌ 已废弃 (Deprecated)

- [x] **LevelDBDict缓冲机制** - 性能测试显示不必要
  - 原因：SSD环境下LevelDB自身缓存已足够高效
  - 读性能慢62.9%，写性能提升仅17.8%
  - 决策：标记为deprecated，RawLevelDBDict为默认

- [x] **布隆过滤器（LevelDBDict中）** - 移除
  - 原因：与缓冲区设计冲突（先查缓冲区，布隆过滤器是纯开销）
  - 决策：移除，性能不受影响

- [x] **TTL分离式存储** - 已重构为内嵌式
  - 原因：两次I/O操作，批量写入复杂
  - 决策：TTL内嵌到value中，I/O减少50%

- [x] **HTTP远程后端** - 已移除，使用ZeroMQ
  - 原因：ZeroMQ性能更好，延迟更低
  - 决策：专注于ZeroMQ实现

---

## 📝 待解决问题 (Known Issues)

### 需要修复

无当前已知的严重bug。

### 需要改进

- [ ] **错误提示** - 部分错误信息可以更友好
  - 缺少依赖时的提示可以更清晰
  - 添加常见问题的解决方案链接

- [ ] **日志级别** - 默认日志可能过于详细
  - 考虑添加quiet模式
  - 区分调试和生产环境的日志级别

---

## 📊 性能目标

### 当前性能 (已达成)

- **本地读取**: 500K+ ops/sec (热数据)
- **本地写入**: 400K+ ops/sec (单次)
- **远程读取**: ~100K ops/sec (本地网络)
- **远程写入**: ~80K ops/sec (本地网络)

### 优化目标 (待实现)

完成P1优化后预期：

- **本地读取**: 800K-1M ops/sec (热数据，读写锁分离)
- **本地写入**: 800K-1M ops/sec (批量模式)
- **远程读取**: ~150K ops/sec (缓存+优化)
- **远程写入**: ~150K ops/sec (批量+压缩)

---

## 📚 文档待完善

- [ ] **API参考文档** - 完整的API说明
  - 自动从docstring生成
  - 所有公开方法的详细说明
  - 参数类型和返回值

- [ ] **性能调优指南** - 帮助用户优化性能
  - 不同场景的最佳配置
  - 性能分析工具使用
  - 常见性能问题排查

- [ ] **故障排除指南** - 常见问题解决方案
  - FAQ
  - 错误代码说明
  - 调试技巧

- [ ] **迁移指南** - 从旧版本迁移
  - 版本兼容性说明
  - 破坏性变更列表
  - 迁移步骤

---

## 🧪 测试待增强

- [ ] **并发测试** - 多线程场景
  - 读写并发
  - 多客户端并发
  - 压力测试

- [ ] **错误恢复测试** - 异常情况处理
  - 磁盘满
  - 网络中断
  - 进程崩溃

- [ ] **性能回归测试** - 自动化性能测试
  - 每次提交运行benchmark
  - 性能下降告警
  - 历史性能趋势

---

## 🎯 里程碑

### v2.0 (当前版本)

- [x] TTL内嵌重构
- [x] 密码派生密钥
- [x] LevelDB配置优化
- [x] 序列化类型缓存
- [x] 文档和测试整理

### v2.1 (下一版本)

目标：性能和可观测性

- [ ] 批量写入队列
- [ ] TTL写入合并
- [ ] 读写锁分离
- [ ] 监控和统计
- [ ] 性能调优指南

### v2.2 (未来版本)

目标：易用性和生产就绪

- [ ] 文档网站
- [ ] 备份和恢复
- [ ] 安全增强
- [ ] 图形化工具

---

**维护说明**:
- 完成任务时将其从"待办"移至"已完成"
- 新增需求添加到相应优先级分类
- 定期review和更新优先级

**最后更新**: 2025-10-31
