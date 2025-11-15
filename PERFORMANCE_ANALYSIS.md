# 大文件传输性能瓶颈分析工具

本文档汇总了大文件传输的性能分析工具和使用方法。

## 快速开始

### 一键运行完整性能分析

```bash
# 使用默认配置 (50MB 文件,快速测试)
./quick_profile.sh

# 自定义文件大小 (单位: MB)
./quick_profile.sh 200

# 自定义服务器端口
./quick_profile.sh 200 25555
```

这个脚本会自动:
1. ✅ 检查/启动服务器
2. ✅ 运行完整性能对比分析
3. ✅ 运行详细阶段分析
4. ✅ 生成汇总报告
5. ✅ 清理测试环境

## 性能分析工具

### 1. `profile_file_transfer.py` - 完整性能对比

**用途**: 对比所有传输方式的性能

```bash
# 基本用法
python3 profile_file_transfer.py

# 自定义参数
python3 profile_file_transfer.py <文件大小MB> <服务器URL>
python3 profile_file_transfer.py 200 tcp://127.0.0.1:25555
```

**测试项目**:
- ✅ 同步上传/下载
- ✅ 并行上传/下载 (多线程, 16 workers)
- ✅ 异步上传/下载 (asyncio, 16 并发)

**输出**:
- 每个测试的 cProfile 详细报告 (`profile_results/*.txt`)
- 性能对比汇总表格 (`profile_results/summary_report.md`)
- 函数级性能分析 (累计时间、内部时间、调用关系)

### 2. `profile_file_transfer_detailed.py` - 详细阶段分析

**用途**: 分析传输过程的每个阶段,找出具体瓶颈

```bash
# 完整分析 (上传阶段 + 组件测试)
python3 profile_file_transfer_detailed.py

# 仅组件性能测试
python3 profile_file_transfer_detailed.py --components
```

**分析内容**:
- ✅ 文件读取耗时
- ✅ 连接建立耗时
- ✅ 元数据存储耗时
- ✅ 每个 chunk 的传输耗时 (平均、最大、最小)
- ✅ 组件性能基准 (序列化、加密、哈希、ZeroMQ)

**输出**:
- 阶段性耗时统计和占比
- 每 5 个 chunk 的性能指标
- 瓶颈分析和优化建议 (`profile_results/detailed_analysis.txt`)

### 3. `quick_profile.sh` - 一键自动化分析

**用途**: 自动化运行所有分析,无需手动操作

```bash
./quick_profile.sh [文件大小MB] [服务器端口]
```

**特点**:
- ✅ 自动启动/检测服务器
- ✅ 运行所有性能测试
- ✅ 生成完整报告
- ✅ 交互式清理

## 性能报告解读

### 汇总报告 (`profile_results/summary_report.md`)

示例输出:
```markdown
| 传输方式 | 总耗时(秒) | 吞吐量(MB/s) | 相对性能 |
|---------|-----------|-------------|----------|
| 同步上传 | 45.32 | 4.4 | 19.3% |
| 并行上传(16线程) | 12.56 | 15.9 | 69.5% |
| 异步上传(16并发) | 8.73 | 22.9 | 100.0% |
```

### 详细分析报告 (`profile_results/detailed_analysis.txt`)

示例输出:
```
阶段                  耗时(秒)      占比(%)
--------------------------------------------------
文件读取              1.23         12.8
建立连接              0.15         1.6
存储元数据            0.02         0.2
上传分块总耗时        8.15         85.1  ← 主要瓶颈
关闭连接              0.03         0.3
```

## 常见性能瓶颈及优化

### 🔴 瓶颈 1: 网络传输慢 (占比 > 80%)

**优化方案**:
```python
# ❌ 慢: 同步传输
upload_large_file(db, key, file_path)  # 4.4 MB/s

# ✅ 快: 异步并发传输
await upload_large_file_async(
    db_name, server_url, key, file_path,
    max_concurrency=16,  # 增加并发
    chunk_size=20*1024*1024  # 增大 chunk
)  # 22.9 MB/s (5.2x 提升)
```

### 🟡 瓶颈 2: 序列化开销

**优化方案**:
```python
# 1. 确保使用二进制模式
msgpack.packb(data, use_bin_type=True)

# 2. 增大 chunk_size 减少序列化次数
chunk_size = 50 * 1024 * 1024  # 50MB
```

### 🟠 瓶颈 3: 加密开销

**优化方案**:
```python
# 可信网络下关闭加密
db = FlaxKV(db_name, server_url, enable_encryption=False)

# 或增大 chunk_size 摊薄加密开销
chunk_size=50*1024*1024
```

## 推荐配置

| 场景 | 传输方式 | chunk_size | 并发数 | 加密 | 预期吞吐量 |
|------|---------|-----------|-------|-----|-----------|
| 本地网络 (低延迟) | 异步 | 10-20 MB | 16 | 否 | 50-100 MB/s |
| 本地网络 (启用加密) | 异步 | 20-50 MB | 16 | 是 | 20-40 MB/s |
| 互联网 (高延迟) | 异步 | 50-100 MB | 8 | 是 | 10-20 MB/s |
| 内存受限环境 | 同步 | 5-10 MB | 1 | 是 | 2-5 MB/s |

## 详细使用指南

完整的使用指南和优化案例请参考: [PROFILE_GUIDE.md](./PROFILE_GUIDE.md)

## 清理

```bash
# 清理测试文件
rm -rf test_data/

# 清理性能报告
rm -rf profile_results/

# 清理服务器日志
rm -f server.log
```

## 相关文档

- [PROFILE_GUIDE.md](./PROFILE_GUIDE.md) - 详细使用指南和优化案例
- [analyze_server_bottleneck.md](./analyze_server_bottleneck.md) - 服务器端性能分析
