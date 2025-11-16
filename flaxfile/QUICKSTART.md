# FlaxFile 快速开始指南

## 📦 安装

```bash
cd flaxfile
pip install -e .
```

安装后，`flaxfile`命令将全局可用。

## 🚀 5分钟上手

### 1. 启动服务器

```bash
# 终端1: 启动服务器
flaxfile serve
```

你会看到：
```
======================================================================
ZMQ 流式文件传输服务器 (TCP优化版)
======================================================================
存储目录: /path/to/zmq_streaming_storage
上传地址: tcp://0.0.0.0:25555
下载地址: tcp://0.0.0.0:25556
控制地址: tcp://0.0.0.0:25557

优化特性:
  ✅ PUSH/PULL模式 (单向高速)
  ✅ 批量接收 (减少上下文切换)
  ✅ 128MB缓冲区
  ✅ TCP优化参数
  ✅ 零拷贝发送
  ✅ 支持跨网络传输
======================================================================

✓ 服务器已启动，监听 0.0.0.0
  ⚠️  监听所有网卡，允许远程连接

等待客户端连接...
```

### 2. 上传文件

```bash
# 终端2: 上传文件
flaxfile set myfile test_data/test_500mb.bin
```

输出：
```
📤 上传文件: test_500mb.bin
   大小: 500.0 MB
   服务器: 127.0.0.1
✓ 已连接到服务器: 127.0.0.1
   进度: 100%
✓ 上传完成:
   耗时: 0.13秒
   吞吐量: 3868.41 MB/s
   SHA256: e5bead4f719f66f8...

✓ 上传成功
  键名: myfile
  大小: 500.00 MB
  吞吐量: 3868.41 MB/s
```

### 3. 下载文件

```bash
# 下载到当前目录
flaxfile get myfile
```

输出：
```
📥 下载文件: myfile
   服务器: 127.0.0.1
   大小: 500.0 MB
   进度: 100%
✓ 下载完成:
   耗时: 0.38秒
   吞吐量: 1327.43 MB/s
   SHA256: e5bead4f719f66f8...

✓ 下载成功
  保存到: myfile
  大小: 500.00 MB
  吞吐量: 1327.43 MB/s
```

### 4. 删除文件

```bash
flaxfile delete myfile
```

输出：
```
✓ 删除成功: myfile
```

## 🌐 跨服务器使用

### 服务器端（192.168.1.100）

```bash
# 启动服务器（监听所有网卡）
flaxfile serve --host 0.0.0.0
```

### 客户端（任何机器）

```bash
# 1. 添加服务器配置
flaxfile config add-server prod 192.168.1.100

# 2. 设置为默认服务器
flaxfile config set-default prod

# 3. 上传文件
flaxfile set video /path/to/large_video.mp4

# 4. 下载文件
flaxfile get video downloaded_video.mp4

# 5. 删除文件
flaxfile delete video
```

## 📋 常用命令

```bash
# 查看配置
flaxfile config show

# 查看版本
flaxfile version

# 帮助
flaxfile --help
flaxfile serve --help
flaxfile set --help
```

## 🔧 Python API示例

```python
from flaxfile import FlaxFileClient

# 连接服务器
client = FlaxFileClient(server_host="192.168.1.100")

# 上传
result = client.upload_file("test.bin", "remote_key", show_progress=True)
print(f"上传: {result['throughput']:.2f} MB/s")

# 下载
result = client.download_file("remote_key", "output.bin", show_progress=True)
print(f"下载: {result['throughput']:.2f} MB/s")

# 清理
client.delete_file("remote_key")
client.close()
```

## 📊 性能测试

```bash
# 创建测试文件
dd if=/dev/urandom of=test_1gb.bin bs=1M count=1024

# 上传测试
time flaxfile set testfile test_1gb.bin

# 下载测试
time flaxfile get testfile output.bin

# 清理
flaxfile delete testfile
rm test_1gb.bin output.bin
```

## ❓ 常见问题

### 无法连接服务器？

1. 检查服务器是否启动：
   ```bash
   lsof -i :25555
   ```

2. 检查防火墙设置

3. 确认服务器配置正确：
   ```bash
   flaxfile config show
   ```

### 性能不理想？

1. 确保使用SSD
2. 检查网络带宽
3. 本地测试应该达到 3000+ MB/s
4. 1Gbps网络应该达到 110-125 MB/s

### 端口被占用？

```bash
# 使用不同端口
flaxfile serve --upload-port 26555 --download-port 26556 --control-port 26557

# 客户端配置
flaxfile config add-server custom 192.168.1.100 \
  --upload-port 26555 \
  --download-port 26556 \
  --control-port 26557
```

## 🎉 完成！

你已经掌握了FlaxFile的基本使用。更多高级功能请查看 `README.md`。
