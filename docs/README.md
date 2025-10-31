# FlaxKV2 文档索引

欢迎使用FlaxKV2文档！本文档提供了项目所有文档的快速导航。

---

## 📚 核心文档

### 用户文档

| 文档 | 描述 | 位置 |
|-----|------|------|
| **README** | 项目介绍、快速开始、主要特性 | [../README.md](../README.md) |
| **密码认证指南** | 两种密码认证方案对比和使用指南 | [../PASSWORD_AUTH_GUIDE.md](../PASSWORD_AUTH_GUIDE.md) |
| **CHANGELOG** | 版本变更记录 | [../CHANGELOG.md](../CHANGELOG.md) |

### 开发者文档

| 文档 | 描述 | 位置 |
|-----|------|------|
| **贡献指南** | 如何参与项目开发 | [../CONTRIBUTING.md](../CONTRIBUTING.md) |
| **Claude Code集成** | Claude Code使用指南和项目架构速查 | [development/CLAUDE.md](development/CLAUDE.md) |

---

## 🏗️ 设计文档

深入理解FlaxKV2的架构和设计理念：

| 文档 | 描述 | 推荐阅读顺序 |
|-----|------|-------------|
| [**核心设计**](design/CORE_DESIGN.md) | 项目设计理念、核心组件、关键决策 | ⭐ 首先阅读 |
| [**架构设计**](design/ARCHITECTURE.md) | 完整的系统架构、组件关系、数据流 | ⭐⭐ 其次阅读 |
| [**键值编码详解**](design/KEY_VALUE_ENCODING_EXPLAINED.md) | 键值对编码格式的技术细节 | ⭐⭐⭐ 深入阅读 |

### 阅读建议

**如果你是**:
- **新用户**: 阅读 README → 核心设计 → 使用指南
- **贡献者**: 阅读 核心设计 → 架构设计 → 贡献指南 → Claude Code集成
- **深度优化**: 阅读 所有设计文档 → 性能优化计划 → 测试README

---

## ⚡ 性能文档

性能优化相关的规划和分析：

| 文档 | 描述 |
|-----|------|
| [**性能优化计划**](performance/PERFORMANCE_OPTIMIZATION_PLAN.md) | 完整的性能优化路线图（P0-P2） |

### 性能优化摘要

已完成的优化：
- ✅ **P0: LevelDB配置优化** - 4-25%性能提升（已实施）
- ✅ **P1: 序列化类型缓存** - 15-30%编码性能提升（已实施）
- ✅ **TTL内嵌重构** - 减少50% I/O操作（已实施）

待实施的优化：
- ⏳ **P1: 批量写入队列** - 预期30-100%写入性能提升
- ⏳ **P2: 读写锁分离** - 预期并发读性能提升100-200%
- ⏳ **P2: 远程传输压缩** - 预期减少50-70%网络带宽

---

## 🧪 测试文档

| 文档 | 描述 |
|-----|------|
| [**测试README**](../tests/README.md) | 测试结构、运行方式、测试分类 |

### 测试结构

```
tests/
├── unit/          # 单元测试（核心功能）
├── integration/   # 集成测试（功能组合）
└── benchmarks/    # 性能基准测试
```

---

## 📋 项目管理

| 文档 | 描述 |
|-----|------|
| [**TODO**](../TODO.md) | 已完成工作和待办事项的完整列表 |

### TODO摘要

**已完成** (✅):
- 核心功能（RawLevelDBDict, NestedDBDict, RemoteDBDict）
- TTL功能（内嵌重构、服务器端验证）
- 远程访问（ZeroMQ、加密、压缩、密码认证）
- 性能优化（P0配置优化、P1类型缓存）
- 工程实践（CI/CD、依赖管理、文档）

**进行中** (🚧):
- 文档整理

**待办事项** (📋):
- P1: 批量写入队列
- P1: TTL写入合并
- P2: 读写锁分离
- P2: 监控和统计

---

## 📖 按主题导航

### 功能特性

| 主题 | 相关文档 |
|-----|---------|
| **基础使用** | README, 核心设计 |
| **TTL过期** | 架构设计, 核心设计, 测试README |
| **嵌套字典** | 核心设计, 键值编码详解 |
| **远程访问** | 架构设计, 核心设计, 密码认证指南 |
| **性能调优** | 性能优化计划, Claude Code集成 |
| **加密安全** | 密码认证指南, 核心设计（安全考虑） |

### 实现细节

| 主题 | 相关文档 |
|-----|---------|
| **序列化** | 核心设计, 键值编码详解, 性能优化计划 |
| **存储格式** | 键值编码详解, 架构设计 |
| **网络协议** | 核心设计, 架构设计 |
| **线程安全** | 核心设计, 性能优化计划（读写锁） |

---

## 🚀 快速入门流程

### 第一次使用FlaxKV2

1. **安装**: `pip install flaxkv2`
2. **快速开始**: 阅读 [README](../README.md) 的"快速入门"部分
3. **了解特性**: 浏览 [核心设计](design/CORE_DESIGN.md) 的"核心特点"
4. **尝试示例**: 运行README中的代码示例

### 准备贡献代码

1. **Fork项目**: 在GitHub上fork仓库
2. **阅读贡献指南**: [CONTRIBUTING.md](../CONTRIBUTING.md)
3. **理解架构**: 阅读 [核心设计](design/CORE_DESIGN.md) 和 [架构设计](design/ARCHITECTURE.md)
4. **查看TODO**: 选择感兴趣的任务 [TODO.md](../TODO.md)
5. **设置环境**: 按照 [Claude Code集成](development/CLAUDE.md) 配置开发环境

### 深入研究实现

1. **核心设计理念**: [CORE_DESIGN.md](design/CORE_DESIGN.md)
2. **完整架构**: [ARCHITECTURE.md](design/ARCHITECTURE.md)
3. **编码细节**: [KEY_VALUE_ENCODING_EXPLAINED.md](design/KEY_VALUE_ENCODING_EXPLAINED.md)
4. **性能优化**: [PERFORMANCE_OPTIMIZATION_PLAN.md](performance/PERFORMANCE_OPTIMIZATION_PLAN.md)
5. **代码实现**: 浏览 `flaxkv2/` 目录的源代码

---

## 🔍 按角色推荐

### 我是普通用户

**目标**: 快速上手使用FlaxKV2

推荐阅读顺序：
1. README（快速开始）
2. 密码认证指南（如果需要远程访问）
3. 核心设计（了解高级特性）

### 我是后端开发者

**目标**: 集成FlaxKV2到项目中

推荐阅读顺序：
1. README（了解功能）
2. 核心设计（理解设计理念）
3. 架构设计（了解系统边界）
4. 密码认证指南（生产环境配置）

### 我是性能工程师

**目标**: 优化FlaxKV2性能

推荐阅读顺序：
1. 性能优化计划（了解优化方向）
2. 核心设计（理解性能瓶颈）
3. 测试README（运行benchmark）
4. Claude Code集成（性能配置）

### 我是开源贡献者

**目标**: 为FlaxKV2贡献代码

推荐阅读顺序：
1. 贡献指南（了解流程）
2. 核心设计（掌握设计理念）
3. 架构设计（理解系统架构）
4. TODO（选择任务）
5. Claude Code集成（开发环境）

---

## 📞 获取帮助

- **问题反馈**: [GitHub Issues](https://github.com/KenyonY/flaxkv2/issues)
- **功能请求**: [GitHub Issues](https://github.com/KenyonY/flaxkv2/issues)
- **代码贡献**: 参考 [CONTRIBUTING.md](../CONTRIBUTING.md)

---

## 📝 文档维护

### 如何更新文档

1. **修改文档**: 编辑对应的Markdown文件
2. **更新索引**: 如果添加新文档，更新此README
3. **遵循格式**: 保持文档结构一致
4. **中英双语**: 主要文档建议提供英文版本

### 文档规范

- **格式**: 使用Markdown
- **代码风格**: 使用代码块，指定语言
- **链接**: 使用相对路径
- **图片**: 存放在`docs/images/`目录（如需）
- **更新日期**: 在文档底部注明最后更新时间

---

**最后更新**: 2025-10-31
