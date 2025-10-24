"""测试 plyvel prefixed_db 特性"""
import plyvel
import tempfile
import shutil

# 创建临时数据库
tmpdir = tempfile.mkdtemp()
print(f"临时数据库路径: {tmpdir}")

try:
    # 创建数据库
    db = plyvel.DB(tmpdir, create_if_missing=True)

    # 创建带前缀的数据库视图
    user1_db = db.prefixed_db(b'user:1:')
    user2_db = db.prefixed_db(b'user:2:')

    # 在 user1 的前缀视图中写入数据
    user1_db.put(b'name', b'Alice')
    user1_db.put(b'age', b'30')
    user1_db.put(b'city', b'NYC')

    # 在 user2 的前缀视图中写入数据
    user2_db.put(b'name', b'Bob')
    user2_db.put(b'age', b'25')

    print("\n=== 通过前缀视图读取 ===")
    print(f"user1 name: {user1_db.get(b'name')}")
    print(f"user1 age: {user1_db.get(b'age')}")
    print(f"user2 name: {user2_db.get(b'name')}")

    print("\n=== 通过主数据库读取（查看实际存储的键）===")
    print(f"db.get(b'user:1:name'): {db.get(b'user:1:name')}")
    print(f"db.get(b'user:1:age'): {db.get(b'user:1:age')}")

    print("\n=== 迭代 user1 的所有键值对 ===")
    for key, value in user1_db:
        print(f"  {key.decode()}: {value.decode()}")

    print("\n=== 修改单个字段（只序列化单个值）===")
    user1_db.put(b'age', b'31')  # 只修改 age，不影响 name 和 city
    print(f"修改后 user1 age: {user1_db.get(b'age')}")

    print("\n=== 删除单个字段 ===")
    user1_db.delete(b'city')
    print(f"删除后 user1 city: {user1_db.get(b'city')}")  # None

    print("\n=== 查看主数据库中的所有键 ===")
    for key, value in db:
        print(f"  {key.decode()}: {value.decode()}")

    # 关闭数据库
    db.close()

finally:
    # 清理临时目录
    shutil.rmtree(tmpdir)
    print(f"\n已清理临时目录")
