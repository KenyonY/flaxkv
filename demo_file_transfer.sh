#!/bin/bash
# FlaxKV 文件传输功能演示脚本
# 演示如何在服务器之间传输文件和文件夹

echo "==================================="
echo "FlaxKV 文件传输功能演示"
echo "==================================="
echo ""

# 配置
SERVER_HOST="127.0.0.1"
SERVER_PORT="15555"
SERVER_ADDR="${SERVER_HOST}:${SERVER_PORT}"
DATA_DIR="/tmp/flaxkv_demo"

echo "步骤 1: 启动 FlaxKV 服务器"
echo "-----------------------------------"
echo "在实际使用中，服务器应该在服务器 A 上运行"
echo "命令: flaxkv2 run --host 0.0.0.0 --port ${SERVER_PORT} --data-dir ${DATA_DIR}"
echo ""
echo "按 Enter 继续..."
read

# 在后台启动服务器
python -m flaxkv2 run --host ${SERVER_HOST} --port ${SERVER_PORT} --data-dir ${DATA_DIR} > /tmp/flaxkv_server.log 2>&1 &
SERVER_PID=$!
echo "服务器已启动 (PID: ${SERVER_PID})"
sleep 2
echo ""

echo "步骤 2: 创建测试文件和文件夹"
echo "-----------------------------------"
# 创建测试文件
TEST_DIR="/tmp/flaxkv_test_files"
mkdir -p ${TEST_DIR}/my_project/{src,docs}

echo "Hello from FlaxKV file transfer!" > ${TEST_DIR}/readme.txt
echo "print('Hello World')" > ${TEST_DIR}/my_project/src/main.py
echo "def test(): pass" > ${TEST_DIR}/my_project/src/utils.py
echo "# Project Documentation" > ${TEST_DIR}/my_project/docs/README.md

echo "创建的测试文件:"
find ${TEST_DIR} -type f -exec echo "  - {}" \;
echo ""
echo "按 Enter 继续..."
read

echo "步骤 3: 上传文件到服务器"
echo "-----------------------------------"
echo "在服务器 B 上执行上传命令..."
echo ""

# 上传单个文件
echo "[上传] 单个文件: readme.txt"
python -m flaxkv2 set ${TEST_DIR}/readme.txt --key readme --server ${SERVER_ADDR}
echo ""

# 上传文件夹
echo "[上传] 文件夹: my_project"
python -m flaxkv2 set ${TEST_DIR}/my_project --key project_v1 --server ${SERVER_ADDR}
echo ""

echo "按 Enter 继续..."
read

echo "步骤 4: 列出服务器上的所有文件"
echo "-----------------------------------"
python -m flaxkv2 list --server ${SERVER_ADDR}
echo ""
echo "按 Enter 继续..."
read

echo "步骤 5: 从服务器下载文件"
echo "-----------------------------------"
echo "在服务器 C 上执行下载命令..."
echo ""

DOWNLOAD_DIR="/tmp/flaxkv_downloads"
mkdir -p ${DOWNLOAD_DIR}

# 下载文件
echo "[下载] readme 文件"
python -m flaxkv2 get readme --output ${DOWNLOAD_DIR} --server ${SERVER_ADDR}
echo ""

# 下载文件夹
echo "[下载] 项目文件夹"
python -m flaxkv2 get project_v1 --output ${DOWNLOAD_DIR} --server ${SERVER_ADDR}
echo ""

echo "按 Enter 查看下载的文件..."
read

echo "步骤 6: 验证下载的文件"
echo "-----------------------------------"
echo "下载的文件结构:"
find ${DOWNLOAD_DIR} -type f -exec sh -c 'echo "  📄 {}" && head -1 "{}"' \;
echo ""

echo "==================================="
echo "演示完成!"
echo "==================================="
echo ""
echo "清理资源..."

# 停止服务器
kill ${SERVER_PID} 2>/dev/null

echo "服务器已停止"
echo ""
echo "临时文件保留在以下位置供检查:"
echo "  - 测试文件: ${TEST_DIR}"
echo "  - 下载文件: ${DOWNLOAD_DIR}"
echo "  - 服务器数据: ${DATA_DIR}"
echo "  - 服务器日志: /tmp/flaxkv_server.log"
echo ""
echo "要清理这些文件，请运行:"
echo "  rm -rf ${TEST_DIR} ${DOWNLOAD_DIR} ${DATA_DIR} /tmp/flaxkv_server.log"
