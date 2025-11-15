#!/bin/bash
# 清理临时测试文件脚本
# 注意: 运行前请确认这些文件确实不需要了

echo "========================================"
echo "临时测试文件清理工具"
echo "========================================"
echo ""
echo "以下文件将被删除:"
echo ""

# 列出要删除的文件
FILES_TO_DELETE=(
    "test_file_transfer.py"
    "test_parallel_transfer.py"
)

for file in "${FILES_TO_DELETE[@]}"; do
    if [ -f "$file" ]; then
        echo "  ✗ $file"
    fi
done

# 列出要保留的文件
echo ""
echo "以下文件将被保留:"
echo ""

FILES_TO_KEEP=(
    "test_cli_parallel.sh"
    "test_cli_folder.sh"
    "test_async_client.py"
    "test_async_file_transfer.py"
    "test_sync_wrapper.py"
)

for file in "${FILES_TO_KEEP[@]}"; do
    if [ -f "$file" ]; then
        echo "  ✓ $file"
    fi
done

echo ""
read -p "确认删除上述文件? [y/N] " -n 1 -r
echo

if [[ $REPLY =~ ^[Yy]$ ]]; then
    for file in "${FILES_TO_DELETE[@]}"; do
        if [ -f "$file" ]; then
            rm "$file"
            echo "✓ 已删除: $file"
        fi
    done
    echo ""
    echo "✅ 清理完成!"
else
    echo "取消删除"
fi
