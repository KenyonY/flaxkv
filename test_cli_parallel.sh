#!/bin/bash
# 测试 CLI 并行传输功能

echo "=========================================="
echo "FlaxKV2 CLI 并行传输功能测试"
echo "=========================================="

# flaxkv2 kill 25555
echo "=========================================="
echo "注意：假设 flaxkv2 server 已经在运行"
echo "      端口: 25555, 加密: 是, 密码: yao"
echo "=========================================="

# 创建测试文件（150MB）
echo ""
echo "1. 创建测试文件（150MB）..."
mkdir -p test_data
python3 -c "
import os
with open('test_data/test_150mb.bin', 'wb') as f:
    for _ in range(150):
        f.write(os.urandom(1024 * 1024))
print('✓ 测试文件创建完成 (150MB)')
"

# 测试并行上传（个并发）
echo ""
echo "=========================================="
echo "2. 测试并行上传（）"
echo "=========================================="
flaxkv2 set test_data/test_150mb.bin --key cli_parallel_test --chunk_size=2097152 --max-workers=16

# 测试并行下载（16 个并发）
echo ""
echo "=========================================="
echo "3. 测试并行下载（ 个并发）"
echo "=========================================="
mkdir -p test_data/downloads
flaxkv2 get cli_parallel_test --output test_data/downloads/test_150mb.bin --max-workers=32

# 验证文件完整性
echo ""
echo "=========================================="
echo "4. 验证文件完整性"
echo "=========================================="
python3 -c "
import hashlib

def calc_hash(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()

orig = calc_hash('test_data/test_150mb.bin')
down = calc_hash('test_data/downloads/test_150mb.bin')

print(f'原始文件: {orig}')
print(f'下载文件: {down}')

if orig == down:
    print('✓ 文件完整性验证通过！')
else:
    print('✗ 文件完整性验证失败！')
    exit(1)
"

# 清理
echo ""
echo "=========================================="
echo "5. 清理测试数据"
echo "=========================================="
rm -rf test_data/test_150mb.bin
rm -rf test_data/downloads
echo "✓ 测试文件已删除"

echo ""
echo "=========================================="
echo "✓ 所有测试完成！"
echo "=========================================="
