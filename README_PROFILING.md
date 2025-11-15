# 性能分析工具使用说明

## 📊 快速开始

```bash
# 一键运行所有性能测试 (推荐)
./quick_profile.sh

# 或使用自定义参数
./quick_profile.sh 200  # 200MB 测试文件
```

## 🛠️ 工具列表

| 工具 | 用途 | 运行时间 |
|------|------|---------|
| `quick_profile.sh` | 一键自动化分析 | ~5-10分钟 |
| `profile_file_transfer.py` | 完整性能对比 | ~8-15分钟 |
| `profile_file_transfer_detailed.py` | 详细阶段分析 | ~2-5分钟 |

## 📈 输出报告

运行后会生成以下报告:

```
profile_results/
├── summary_report.md              # 汇总对比报告 ⭐
├── detailed_analysis.txt          # 详细阶段分析 ⭐
├── sync_upload.txt                # 同步上传详细分析
├── parallel_upload_workers_16.txt # 并行上传详细分析
└── async_upload_concurrency_16.txt # 异步上传详细分析
```

## 🎯 主要用途

### 1. 对比不同传输方式性能

```bash
python3 profile_file_transfer.py
```

**输出示例**:
```
传输方式              耗时(秒)      吞吐量(MB/s)
------------------------------------------------------------
同步上传              45.32        4.4
并行上传(16线程)      12.56        15.9
异步上传(16并发)      8.73         22.9  ← 最优
```

### 2. 找出具体性能瓶颈

```bash
python3 profile_file_transfer_detailed.py
```

**输出示例**:
```
阶段                  耗时(秒)      占比(%)
--------------------------------------------------
文件读取              1.23         12.8
上传分块总耗时        8.15         85.1  ← 主要瓶颈
```

### 3. 测试组件性能

```bash
python3 profile_file_transfer_detailed.py --components
```

**输出示例**:
```
msgpack序列化 (10MB): 2.45ms
Fernet加密 (10MB): 45.32ms
SHA256 (10MB): 18.67ms
ZeroMQ往返时延: 0.82ms
```

## 💡 常见优化建议

根据测试结果选择优化方向:

### 网络传输慢 (占比 > 80%)
→ 增加并发数, 调大 chunk_size

### 序列化慢 (msgpack 耗时高)
→ 增大 chunk_size, 减少序列化次数

### 加密慢 (Fernet 耗时高)
→ 关闭加密(可信网络) 或 增大 chunk_size

### 文件 I/O 慢 (占比 > 10%)
→ 使用 SSD, 预读文件到内存

## 📚 详细文档

- **[PERFORMANCE_ANALYSIS.md](./PERFORMANCE_ANALYSIS.md)** - 工具概述和快速上手
- **[PROFILE_GUIDE.md](./PROFILE_GUIDE.md)** - 详细使用指南和优化案例 (60+ 页)
- **[analyze_server_bottleneck.md](./analyze_server_bottleneck.md)** - 服务器端性能分析

## 🧹 清理

```bash
# 清理测试文件和报告
rm -rf test_data/ profile_results/ server.log
```

## ⚙️ 前置条件

1. **启动服务器**:
```bash
flaxkv2 run --host 127.0.0.1 --port 25555 --data-dir ./data --password yao
```

2. **安装可选依赖** (用于更详细分析):
```bash
pip install line_profiler memory_profiler snakeviz py-spy
```

## 📞 常见问题

**Q: 为什么异步比并行快?**
A: 异步使用 asyncio,避免了 GIL 竞争,I/O 密集型场景性能更好

**Q: 应该使用多大的 chunk_size?**
A: 一般 10-20MB,高延迟网络可用 50-100MB

**Q: 应该使用多少并发数?**
A: 本地网络 8-16,互联网 4-8,通过实际测试找到最佳值

---

**快速链接**: [完整指南](./PROFILE_GUIDE.md) | [工具概述](./PERFORMANCE_ANALYSIS.md)
