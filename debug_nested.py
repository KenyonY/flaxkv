"""调试 NestedDBDict"""
import tempfile
import shutil
from flaxkv2 import LevelDBDict

tmpdir = tempfile.mkdtemp()
print(f"临时目录: {tmpdir}")

try:
    db = LevelDBDict('test', path=tmpdir, rebuild=True)

    # 创建嵌套字典
    nested = db.nested('data')
    print(f"\n嵌套字典前缀: {nested._prefix}")

    # 写入数据
    print("\n写入 field_0 = 100")
    nested['field_0'] = 100

    # 检查缓冲区
    print(f"\n父数据库缓冲区内容:")
    with db._buffer_lock:
        for key, value in db._buffer_dict.items():
            print(f"  {key}: {value}")

    # 尝试读取
    print(f"\n尝试读取 field_0:")
    try:
        value = nested['field_0']
        print(f"  成功！值: {value}")
    except KeyError as e:
        print(f"  失败！KeyError: {e}")

    # 刷新到磁盘（使用同步模式）
    print(f"\n刷新到磁盘...")
    db.write_immediately(block=True)

    # 查看 LevelDB 中的所有键
    print(f"\nLevelDB 中的所有键:")
    for key, value in db._db:
        print(f"  {key}: {value}")

    # 查看 prefixed_db 中的键
    print(f"\nprefixed_db 中的键:")
    for key, value in nested._prefixed_db:
        print(f"  {key}: {value}")

    # 手动测试 prefixed_db.get
    print(f"\n手动测试 prefixed_db.get(b'field_0'):")
    result = nested._prefixed_db.get(b'field_0')
    print(f"  结果: {result}")

    # 再次尝试读取
    print(f"\n再次尝试读取 field_0:")
    try:
        value = nested['field_0']
        print(f"  成功！值: {value}")
    except KeyError as e:
        print(f"  失败！KeyError: {e}")

    db.close()

finally:
    shutil.rmtree(tmpdir)
    print(f"\n已清理")
