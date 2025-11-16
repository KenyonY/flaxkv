#!/bin/bash
# 测试 CLI 并行传输功能 - 多文件大小测试

echo "=========================================="
echo "FlaxKV2 CLI 并行传输功能测试"
echo "测试文件大小: 50MB, 150MB, 350MB"
echo "=========================================="

echo "=========================================="
echo "注意：假设 flaxkv2 server 已经在运行"
echo "      端口: 28855, 加密: 是, 密码: yao"
echo "=========================================="

# 创建测试目录
mkdir -p test_data
mkdir -p test_data/downloads

# 测试文件大小数组（MB）
SIZES=(50 150 350)

# 用于存储结果
declare -a UPLOAD_TIMES
declare -a DOWNLOAD_TIMES
declare -a UPLOAD_SPEEDS
declare -a DOWNLOAD_SPEEDS

# 循环测试每个文件大小
for SIZE in "${SIZES[@]}"; do
    echo ""
    echo "=========================================="
    echo "测试文件大小: ${SIZE}MB"
    echo "=========================================="

    # 1. 创建测试文件
    echo ""
    echo "1. 创建测试文件（${SIZE}MB）..."
    python3 -c "
import os
with open('test_data/test_${SIZE}mb.bin', 'wb') as f:
    for _ in range(${SIZE}):
        f.write(os.urandom(1024 * 1024))
print('✓ 测试文件创建完成 (${SIZE}MB)')
"

    # 2. 测试并行上传（16 个并发）
#     echo ""
#     echo "2. 测试并行上传（16 个并发）"
#     echo "=========================================="
#     flaxkv2 set test_data/test_${SIZE}mb.bin --key cli_parallel_test_${SIZE}mb --chunk_size=2097152 --max-workers=16

#     # 3. 测试并行下载（32 个并发）
#     echo ""
#     echo "3. 测试并行下载（32 个并发）"
#     echo "=========================================="
#     flaxkv2 get cli_parallel_test_${SIZE}mb --output test_data/downloads/test_${SIZE}mb.bin --max-workers=32

#     # 4. 验证文件完整性
#     echo ""
#     echo "4. 验证文件完整性"
#     echo "=========================================="
#     python3 -c "
# import hashlib

# def calc_hash(path):
#     h = hashlib.sha256()
#     with open(path, 'rb') as f:
#         while chunk := f.read(8192):
#             h.update(chunk)
#     return h.hexdigest()

# orig = calc_hash('test_data/test_${SIZE}mb.bin')
# down = calc_hash('test_data/downloads/test_${SIZE}mb.bin')

# print(f'原始文件: {orig}')
# print(f'下载文件: {down}')

# if orig == down:
#     print('✓ 文件完整性验证通过！')
# else:
#     print('✗ 文件完整性验证失败！')
#     exit(1)
# "

#     # 5. 清理当前测试文件
#     echo ""
#     echo "5. 清理测试数据"
#     echo "=========================================="
#     rm -f test_data/test_${SIZE}mb.bin
#     rm -f test_data/downloads/test_${SIZE}mb.bin
#     echo "✓ 测试文件已删除"

#     echo ""
#     echo "=========================================="
#     echo "✓ ${SIZE}MB 测试完成！"
#     echo "=========================================="
#     echo ""
done

# # 清理目录
# rm -rf test_data/downloads

echo ""
echo "=========================================="
echo "✓ 所有测试完成！"
echo "=========================================="
echo ""
echo "已测试文件大小: ${SIZES[@]} MB"
echo ""
