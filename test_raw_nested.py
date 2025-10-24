"""测试 RawLevelDBDict 的 nested() 功能"""
import tempfile
import shutil
from flaxkv2.core.raw_leveldb_dict import RawLevelDBDict

tmpdir = tempfile.mkdtemp()
print(f"临时目录: {tmpdir}")

try:
    db = RawLevelDBDict('test', path=tmpdir, rebuild=True)

    # 创建嵌套字典
    user = db.nested('user:1')
    print(f"\n✓ 创建嵌套字典成功")

    # 写入数据
    user['name'] = 'Alice'
    user['age'] = 30
    user['email'] = 'alice@example.com'
    print(f"\n✓ 写入数据成功")

    # 读取数据
    print(f"\n读取数据:")
    print(f"  name: {user['name']}")
    print(f"  age: {user['age']}")
    print(f"  email: {user['email']}")

    # 修改数据
    user['age'] = 31
    print(f"\n✓ 修改数据成功，新age: {user['age']}")

    # 迭代
    print(f"\n所有字段:")
    for key, value in user.items():
        print(f"  {key}: {value}")

    # 测试持久化
    db.close()
    db = RawLevelDBDict('test', path=tmpdir)
    user = db.nested('user:1')

    print(f"\n重新打开数据库后:")
    print(f"  name: {user['name']}")
    print(f"  age: {user['age']}")

    db.close()
    print(f"\n✓ 所有测试通过！")

finally:
    shutil.rmtree(tmpdir)
    print(f"\n已清理")
