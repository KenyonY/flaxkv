#!/bin/bash
# 测试 CLI 目录分块传输功能

echo "=========================================="
echo "FlaxKV2 CLI 目录分块传输功能测试"
echo "=========================================="

# 创建测试目录（包含多个文件，总大小 > 100MB）
echo ""
echo "1. 创建测试目录（包含 10 个文件，共 150MB）..."
mkdir -p test_data/test_large_folder

python3 -c "
import os
for i in range(10):
    with open(f'test_data/test_large_folder/file_{i}.bin', 'wb') as f:
        f.write(os.urandom(15 * 1024 * 1024))  # 每个文件 15MB
print('✓ 测试目录创建完成')
"

# 显示目录大小
echo ""
echo "目录内容:"
du -sh test_data/test_large_folder
ls -lh test_data/test_large_folder

# 测试并行上传目录
echo ""
echo "=========================================="
echo "2. 测试并行上传目录（默认 5 个线程）"
echo "=========================================="
flaxkv2 set test_data/test_large_folder --key test_folder

# 测试并行下载目录
echo ""
echo "=========================================="
echo "3. 测试并行下载目录（默认 5 个线程）"
echo "=========================================="
mkdir -p test_data/downloads
flaxkv2 get test_folder --output test_data/downloads/

# 验证目录完整性
echo ""
echo "=========================================="
echo "4. 验证目录完整性"
echo "=========================================="
python3 -c "
import os
import hashlib

def calc_dir_hash(path):
    \"\"\"计算目录下所有文件的哈希值\"\"\"
    hashes = []
    for root, dirs, files in os.walk(path):
        for file in sorted(files):
            file_path = os.path.join(root, file)
            h = hashlib.sha256()
            with open(file_path, 'rb') as f:
                while chunk := f.read(8192):
                    h.update(chunk)
            hashes.append(h.hexdigest())
    return hashlib.sha256(''.join(hashes).encode()).hexdigest()

orig = calc_dir_hash('test_data/test_large_folder')
down = calc_dir_hash('test_data/downloads/test_large_folder')

print(f'原始目录: {orig}')
print(f'下载目录: {down}')

if orig == down:
    print('✓ 目录完整性验证通过！')
else:
    print('✗ 目录完整性验证失败！')
    exit(1)
"

# 测试小目录（< 100MB，传统方式）
echo ""
echo "=========================================="
echo "5. 测试小目录上传（< 100MB，传统方式）"
echo "=========================================="
mkdir -p test_data/test_small_folder
python3 -c "
import os
for i in range(5):
    with open(f'test_data/test_small_folder/small_{i}.txt', 'w') as f:
        f.write('test data ' * 1000)  # 每个文件 ~9KB
print('✓ 小目录创建完成')
"

flaxkv2 set test_data/test_small_folder --key test_small_folder

# 测试串行上传大目录
echo ""
echo "=========================================="
echo "6. 测试串行上传大目录（--serial 参数）"
echo "=========================================="
flaxkv2 set test_data/test_large_folder --key test_folder_serial --serial

# 清理
echo ""
echo "=========================================="
echo "7. 清理测试数据"
echo "=========================================="
rm -rf test_data/test_large_folder
rm -rf test_data/test_small_folder
rm -rf test_data/downloads
echo "✓ 测试文件已删除"

echo ""
echo "=========================================="
echo "✓ 所有测试完成！"
echo "=========================================="
