# FlaxKV2 文档目录

本目录包含 FlaxKV2 项目的所有文档，按类别组织。

## 📚 文档结构

### 🎯 功能特性 (features/)
用户功能和特性的详细说明文档。

- [README_TTL.md](features/README_TTL.md) - TTL (生存时间) 功能说明
- [README_DefaultTTL.md](features/README_DefaultTTL.md) - 默认 TTL 功能说明
- [NESTED_DICT_GUIDE.md](features/NESTED_DICT_GUIDE.md) - 嵌套字典使用指南
- [NESTED_DICT_SUMMARY.md](features/NESTED_DICT_SUMMARY.md) - 嵌套字典功能总结
- [AUTO_NESTED_README.md](features/AUTO_NESTED_README.md) - 自动嵌套功能说明
- [QUICKSTART_NESTED.md](features/QUICKSTART_NESTED.md) - 嵌套字典快速入门

### 🌐 ZeroMQ 远程访问 (zeromq/)
基于 ZeroMQ 的高性能远程数据库访问文档。

- [ZEROMQ_REMOTE.md](zeromq/ZEROMQ_REMOTE.md) - ZeroMQ 远程访问使用指南
- [ZEROMQ_IMPLEMENTATION_SUMMARY.md](zeromq/ZEROMQ_IMPLEMENTATION_SUMMARY.md) - ZeroMQ 实现总结
- [ZEROMQ_TEST_REPORT.md](zeromq/ZEROMQ_TEST_REPORT.md) - ZeroMQ 测试报告

### 🛠️ 开发文档 (development/)
开发相关的技术文档和变更记录。

- [BACKEND_REFACTORING.md](development/BACKEND_REFACTORING.md) - 后端重构文档
- [NESTED_DICT_SERIALIZATION_FIX.md](development/NESTED_DICT_SERIALIZATION_FIX.md) - 嵌套字典序列化修复
- [IMPROVED_ERROR_HANDLING.md](development/IMPROVED_ERROR_HANDLING.md) - 错误处理改进
- [CHANGELOG_NESTED.md](development/CHANGELOG_NESTED.md) - 嵌套字典变更日志
- [TESTING_GUIDE.md](development/TESTING_GUIDE.md) - 测试指南
- [SPECIAL_KEYS_AND_VALUES.md](development/SPECIAL_KEYS_AND_VALUES.md) - 特殊键和值说明

### ⚡ 性能分析 (performance/)
性能测试和分析报告。

- [PERFORMANCE_ANALYSIS.md](performance/PERFORMANCE_ANALYSIS.md) - 性能分析报告
- [BENCHMARK_REPORT.md](performance/BENCHMARK_REPORT.md) - 基准测试报告

### 📖 参考文档 (reference/)
API 参考和详细技术文档。

- [documentation.md](reference/documentation.md) - 完整 API 文档

### 📦 归档文档 (archive/)
历史文档和总结。

- [FINAL_SUMMARY.md](archive/FINAL_SUMMARY.md) - 最终总结
- [CLEANUP_SUMMARY.md](archive/CLEANUP_SUMMARY.md) - 清理总结

## 🚀 快速导航

### 新用户
1. 从根目录的 [README.md](../README.md) 开始
2. 阅读 [QUICKSTART_NESTED.md](features/QUICKSTART_NESTED.md) 快速入门
3. 查看 [NESTED_DICT_GUIDE.md](features/NESTED_DICT_GUIDE.md) 了解核心功能

### 高级功能
- TTL 功能: [README_TTL.md](features/README_TTL.md)
- 远程访问: [ZEROMQ_REMOTE.md](zeromq/ZEROMQ_REMOTE.md)
- 性能优化: [PERFORMANCE_ANALYSIS.md](performance/PERFORMANCE_ANALYSIS.md)

### 开发者
- 测试指南: [TESTING_GUIDE.md](development/TESTING_GUIDE.md)
- 重构文档: [BACKEND_REFACTORING.md](development/BACKEND_REFACTORING.md)
- API 文档: [documentation.md](reference/documentation.md)

## 📝 文档维护

### 添加新文档
新文档应该放在相应的类别目录中，并更新本 README.md 的索引。

### 文档分类标准
- **features/**: 面向用户的功能说明
- **zeromq/**: ZeroMQ 相关的所有文档
- **development/**: 开发和维护相关文档
- **performance/**: 性能测试和分析
- **reference/**: API 和技术参考
- **archive/**: 历史文档和总结

## 🔗 外部文档

- [plyvel API 参考](../plyvel-doc/plyvel_api_reference.md) - LevelDB Python 绑定文档
- [Benchmark README](../benchmark/README.md) - 性能测试说明

## 📄 许可证

所有文档遵循项目的 MIT 许可证。

