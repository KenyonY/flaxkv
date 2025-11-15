#!/bin/bash
# 快速性能分析脚本
# 自动启动服务器、运行性能分析、生成报告

set -e  # 遇到错误立即退出

echo "========================================"
echo "FlaxKV2 快速性能分析"
echo "========================================"

# 配置
FILE_SIZE_MB=${1:-50}  # 默认 50MB (快速测试)
SERVER_PORT=${2:-25555}
SERVER_HOST="127.0.0.1"
SERVER_URL="tcp://$SERVER_HOST:$SERVER_PORT"
PASSWORD="yao"

echo ""
echo "配置:"
echo "  测试文件大小: ${FILE_SIZE_MB}MB"
echo "  服务器地址: $SERVER_URL"
echo "  密码: $PASSWORD"
echo ""

# 检查服务器是否在运行
echo "========================================"
echo "1. 检查服务器状态"
echo "========================================"

SERVER_RUNNING=0
if lsof -Pi :$SERVER_PORT -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo "✓ 检测到服务器正在端口 $SERVER_PORT 运行"
    SERVER_RUNNING=1
else
    echo "✗ 服务器未运行,正在启动..."

    # 启动服务器
    flaxkv2 run --host $SERVER_HOST --port $SERVER_PORT --data-dir ./data --password $PASSWORD > server.log 2>&1 &
    SERVER_PID=$!

    echo "  服务器 PID: $SERVER_PID"
    echo "  等待服务器启动..."
    sleep 3

    # 验证服务器是否成功启动
    if lsof -Pi :$SERVER_PORT -sTCP:LISTEN -t >/dev/null 2>&1; then
        echo "✓ 服务器启动成功"
    else
        echo "✗ 服务器启动失败,请检查 server.log"
        exit 1
    fi
fi

# 运行性能分析
echo ""
echo "========================================"
echo "2. 运行性能分析"
echo "========================================"

echo ""
echo "2.1 完整性能对比分析..."
echo "----------------------------------------"
python3 profile_file_transfer.py $FILE_SIZE_MB $SERVER_URL

echo ""
echo "2.2 详细阶段分析..."
echo "----------------------------------------"
python3 profile_file_transfer_detailed.py

# 生成可视化报告 (如果安装了相关工具)
echo ""
echo "========================================"
echo "3. 生成可视化报告"
echo "========================================"

if command -v snakeviz &> /dev/null; then
    echo "✓ 检测到 snakeviz,生成交互式可视化..."
    # snakeviz 需要 .prof 文件,这里跳过
    echo "  (需要使用 cProfile.run() 生成 .prof 文件)"
else
    echo "  未安装 snakeviz (可选)"
    echo "  安装命令: pip install snakeviz"
fi

# 显示结果
echo ""
echo "========================================"
echo "4. 查看结果"
echo "========================================"

echo ""
echo "汇总报告:"
if [ -f "profile_results/summary_report.md" ]; then
    cat profile_results/summary_report.md
else
    echo "  未找到汇总报告"
fi

echo ""
echo "详细报告位置:"
echo "  profile_results/"
ls -lh profile_results/ 2>/dev/null || echo "  未找到详细报告目录"

# 清理
echo ""
echo "========================================"
echo "5. 清理"
echo "========================================"

if [ $SERVER_RUNNING -eq 0 ]; then
    echo "正在停止测试服务器..."
    kill $SERVER_PID 2>/dev/null || true
    echo "✓ 服务器已停止"
fi

echo ""
echo "是否删除测试文件? (test_data/)"
read -p "  [y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    rm -rf test_data/
    echo "✓ 测试文件已删除"
else
    echo "  保留测试文件"
fi

echo ""
echo "========================================"
echo "✅ 性能分析完成!"
echo "========================================"
echo ""
echo "查看报告:"
echo "  cat profile_results/summary_report.md"
echo "  cat profile_results/detailed_analysis.txt"
echo ""
echo "查看详细函数级分析:"
echo "  cat profile_results/sync_upload.txt"
echo "  cat profile_results/async_upload_concurrency_16.txt"
echo ""
