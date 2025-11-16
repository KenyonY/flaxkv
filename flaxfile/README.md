# FlaxFile - 高性能文件传输工具

基于ZMQ优化的跨网络文件传输系统，专为大文件高速传输设计。

## ⚡ 性能

- **本地测试**: 3800+ MB/s
- **1Gbps网络**: 110-125 MB/s（跑满带宽）
- **10Gbps网络**: 1000-1200 MB/s

vs 其他方案：
- **vs HTTP**: 13倍更快
- **vs 原版ZMQ**: 2.7倍更快

## 🚀 快速开始

### 安装依赖

```bash
pip install pyzmq fire
```

### 启动服务器

```bash
# 方法1: 使用CLI
python -m flaxfile.cli serve

# 方法2: 直接运行
python flaxfile/server.py

# 监听所有网卡（允许远程连接）
python -m flaxfile.cli serve --host 0.0.0.0

# 自定义端口
python -m flaxfile.cli serve --upload-port 26555 --download-port 26556
```

### 使用客户端

```bash
# 上传文件
python -m flaxfile.cli set myfile /path/to/file.bin

# 下载文件
python -m flaxfile.cli get myfile output.bin

# 删除文件
python -m flaxfile.cli delete myfile

# 查看版本
python -m flaxfile.cli version
```

## 📖 详细使用

### 配置管理

```bash
# 显示当前配置
python -m flaxfile.cli config show

# 添加远程服务器
python -m flaxfile.cli config add-server prod 192.168.1.100

# 添加服务器（自定义端口）
python -m flaxfile.cli config add-server dev 10.0.0.5 \
  --upload-port 26555 \
  --download-port 26556 \
  --control-port 26557

# 设置默认服务器
python -m flaxfile.cli config set-default prod

# 删除服务器
python -m flaxfile.cli config remove-server dev
```

配置文件位置：`~/.flaxfile/config.json`

### 使用远程服务器

```bash
# 上传到指定服务器
python -m flaxfile.cli set myfile /path/to/file.bin --server prod

# 从指定服务器下载
python -m flaxfile.cli get myfile output.bin --server prod

# 删除远程服务器文件
python -m flaxfile.cli delete myfile --server prod
```

### Python API

```python
from flaxfile import FlaxFileClient

# 创建客户端
client = FlaxFileClient(server_host="192.168.1.100")

# 上传文件
result = client.upload_file("test.bin", "remote_key")
print(f"上传吞吐量: {result['throughput']:.2f} MB/s")

# 下载文件
result = client.download_file("remote_key", "output.bin")
print(f"下载吞吐量: {result['throughput']:.2f} MB/s")

# 删除文件
client.delete_file("remote_key")

# 关闭连接
client.close()
```

## 🔧 高级配置

### 服务器端

```bash
# 仅本地访问（安全）
python -m flaxfile.cli serve --host 127.0.0.1

# 允许所有网卡访问（生产环境）
python -m flaxfile.cli serve --host 0.0.0.0

# 自定义所有端口
python -m flaxfile.cli serve \
  --upload-port 26555 \
  --download-port 26556 \
  --control-port 26557
```

### 客户端端口配置

如果服务器使用非默认端口，需要在配置中指定：

```bash
python -m flaxfile.cli config add-server custom 192.168.1.100 \
  --upload-port 26555 \
  --download-port 26556 \
  --control-port 26557
```

## 📊 性能优化技术

FlaxFile采用以下优化技术实现极致性能：

1. **PUSH/PULL模式** - 单向数据流，无identity开销
2. **批量接收** - 减少系统调用，提升吞吐量
3. **大缓冲区** - 128MB缓冲区，充分利用网络
4. **零拷贝发送** - 减少内存拷贝
5. **大chunk size** - 4MB chunk，减少往返次数
6. **TCP优化** - keepalive等参数优化

详细技术文档：`../profile_results/FINAL_TCP_OPTIMIZATION_REPORT.md`

## 🌐 网络性能

### 本地loopback

| 文件大小 | 上传 | 下载 |
|---------|------|------|
| 500MB | 3868 MB/s | 1327 MB/s |
| 1GB+ | 3800+ MB/s | 1300+ MB/s |

### 真实网络

| 网络 | 理论上限 | 实际吞吐量 |
|------|---------|----------|
| 1Gbps | 125 MB/s | 110-125 MB/s |
| 10Gbps | 1250 MB/s | 1000-1200 MB/s |

## 🔒 安全注意事项

**当前版本不支持加密传输**，仅适用于：
- ✅ 内网/局域网传输
- ✅ 可信网络环境
- ✅ VPN隧道内传输

**不推荐**：
- ❌ 公网直接传输敏感数据
- ❌ 不可信网络环境

未来版本将支持TLS/SSL加密。

## 📁 项目结构

```
flaxfile/
├── __init__.py       # 包初始化
├── server.py         # 服务器实现
├── client.py         # 客户端实现
├── config.py         # 配置管理
├── cli.py            # CLI接口
└── README.md         # 本文档
```

## 🆚 vs 其他方案

| 方案 | 500MB上传 | 跨网络 | 大文件稳定 |
|------|----------|--------|----------|
| **FlaxFile** | **3868 MB/s** | ✅ | ✅ |
| HTTP (FastAPI) | 292 MB/s | ✅ | ❌ |
| scp | ~100 MB/s | ✅ | ✅ |
| rsync | ~120 MB/s | ✅ | ✅ |

## 🛠️ 故障排除

### 连接超时

```bash
# 检查服务器是否启动
lsof -i :25555

# 检查防火墙
# macOS
sudo pfctl -s rules

# Linux
sudo iptables -L
```

### 端口已被占用

```bash
# 使用不同端口
python -m flaxfile.cli serve --upload-port 26555 --download-port 26556 --control-port 26557
```

### 性能不佳

1. 检查网络带宽是否为瓶颈
2. 确保使用SSD而非HDD
3. 检查CPU使用率
4. 尝试增大系统TCP缓冲区：
   ```bash
   # Linux
   sudo sysctl -w net.core.rmem_max=134217728
   sudo sysctl -w net.core.wmem_max=134217728
   ```

## 📝 开发路线图

- [ ] 支持TLS/SSL加密传输
- [ ] 支持断点续传
- [ ] 支持文件列表功能
- [ ] 支持多文件批量传输
- [ ] Web界面管理
- [ ] 传输进度条美化

## 📄 License

MIT License

## 🙏 致谢

基于ZeroMQ (ØMQ)高性能消息传递库构建。

---

**FlaxFile** - 让大文件传输飞起来！ 🚀
