"""调试删除功能"""
import tempfile
import shutil
from flaxkv2 import LevelDBDict

tmpdir = tempfile.mkdtemp()
print(f"临时目录: {tmpdir}\n")

try:
    db = LevelDBDict('test', path=tmpdir, rebuild=True)

    # 写入嵌套字典
    db['config'] = {
        'database': {
            'host': 'localhost'
        }
    }
    print("✓ 写入 config")

    # 检查标记
    try:
        marker = db[f'__nested__:config']
        print(f"✓ 标记存在：__nested__:config = {marker}")
    except KeyError:
        print("❌ 标记不存在")

    # 删除
    print("\n删除 config...")
    del db['config']

    # 检查标记是否删除
    try:
        marker = db[f'__nested__:config']
        print(f"❌ 标记未删除：__nested__:config = {marker}")
    except KeyError:
        print("✓ 标记已删除")

    # 尝试读取
    try:
        config = db['config']
        print(f"❌ config 仍然存在: {config}")
    except KeyError:
        print("✓ config 已删除")

    # 查看所有键
    print("\n所有键:")
    for key, value in db._db:
        key_str = key.decode('utf-8', errors='ignore')
        print(f"  {key_str}")

    db.close()

finally:
    shutil.rmtree(tmpdir)
    print(f"\n已清理")
