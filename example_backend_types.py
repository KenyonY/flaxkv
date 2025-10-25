"""
FlaxKV 后端类型使用示例
演示如何使用本地和远程后端
"""

from flaxkv2 import FlaxKV, BackendType
import tempfile
import shutil


def example_local_backend():
    """本地后端示例"""
    print("=" * 60)
    print("示例 1: 本地后端")
    print("=" * 60)
    
    # 创建临时目录
    temp_dir = tempfile.mkdtemp()
    
    try:
        # 方式1: 自动检测（推荐）
        db = FlaxKV("mydb", temp_dir)
        print(f"✓ 创建数据库: {type(db).__name__}")
        
        # 存储数据
        db['user:1'] = {'name': 'Alice', 'age': 30}
        db['user:2'] = {'name': 'Bob', 'age': 25}
        
        # 读取数据
        print(f"✓ user:1 = {db['user:1']}")
        print(f"✓ user:2 = {db['user:2']}")
        
        # 关闭数据库
        db.close()
        print("✓ 数据库已关闭")
        
    finally:
        shutil.rmtree(temp_dir)
    
    print()


def example_local_backend_with_options():
    """本地后端高级选项示例"""
    print("=" * 60)
    print("示例 2: 本地后端高级选项")
    print("=" * 60)
    
    temp_dir = tempfile.mkdtemp()
    
    try:
        # 使用 auto_nested 模式
        db = FlaxKV("mydb", temp_dir, auto_nested=True)
        print(f"✓ 创建数据库（auto_nested=True）")
        
        # 存储嵌套字典
        db['config'] = {
            'database': {
                'host': 'localhost',
                'port': 5432
            },
            'cache': {
                'enabled': True,
                'ttl': 3600
            }
        }
        
        # 读取嵌套字典（自动返回 NestedDBDict）
        config = db['config']
        print(f"✓ config 类型: {type(config).__name__}")
        print(f"✓ config['database'] = {config['database']}")
        
        db.close()
        
        # 使用 default_ttl
        db2 = FlaxKV("mydb2", temp_dir, default_ttl=60)
        print(f"✓ 创建数据库（default_ttl=60秒）")
        
        db2['temp_key'] = 'temp_value'
        ttl = db2.get_ttl('temp_key')
        print(f"✓ temp_key 的 TTL: {ttl}秒")
        
        db2.close()
        
    finally:
        shutil.rmtree(temp_dir)
    
    print()


def example_backend_detection():
    """后端类型检测示例"""
    print("=" * 60)
    print("示例 3: 后端类型检测")
    print("=" * 60)
    
    # 检测不同的路径/URL
    paths = [
        ".",
        "./data",
        "/var/lib/flaxkv",
        "http://localhost:8000",
        "https://api.example.com"
    ]
    
    for path in paths:
        backend_type = FlaxKV._detect_backend_type(path)
        print(f"✓ {path:30s} -> {backend_type}")
    
    print()


def example_explicit_backend():
    """显式指定后端类型示例"""
    print("=" * 60)
    print("示例 4: 显式指定后端类型")
    print("=" * 60)
    
    temp_dir = tempfile.mkdtemp()
    
    try:
        # 显式指定本地后端
        db = FlaxKV("mydb", temp_dir, backend='local')
        print(f"✓ 显式指定 backend='local': {type(db).__name__}")
        
        db['key'] = 'value'
        assert db['key'] == 'value'
        print(f"✓ 数据读写正常")
        
        db.close()
        
    finally:
        shutil.rmtree(temp_dir)
    
    print()


def example_backend_type_enum():
    """BackendType 枚举使用示例"""
    print("=" * 60)
    print("示例 5: BackendType 枚举")
    print("=" * 60)
    
    print(f"✓ BackendType.LOCAL = '{BackendType.LOCAL}'")
    print(f"✓ BackendType.REMOTE = '{BackendType.REMOTE}'")
    
    # 在代码中使用
    url = "http://localhost:8000"
    detected = FlaxKV._detect_backend_type(url)
    
    if detected == BackendType.REMOTE:
        print(f"✓ {url} 是远程后端")
    elif detected == BackendType.LOCAL:
        print(f"✓ {url} 是本地后端")
    
    print()


def example_context_manager():
    """上下文管理器示例"""
    print("=" * 60)
    print("示例 6: 使用上下文管理器")
    print("=" * 60)
    
    temp_dir = tempfile.mkdtemp()
    
    try:
        # 使用 with 语句自动管理数据库生命周期
        with FlaxKV("mydb", temp_dir) as db:
            db['key1'] = 'value1'
            db['key2'] = 'value2'
            print(f"✓ 在上下文中写入数据")
            print(f"✓ key1 = {db['key1']}")
        
        print(f"✓ 退出上下文时自动关闭数据库")
        
        # 重新打开验证数据已保存
        with FlaxKV("mydb", temp_dir) as db:
            assert db['key1'] == 'value1'
            assert db['key2'] == 'value2'
            print(f"✓ 数据已持久化")
        
    finally:
        shutil.rmtree(temp_dir)
    
    print()


def main():
    """运行所有示例"""
    print("\n" + "=" * 60)
    print("FlaxKV 后端类型使用示例")
    print("=" * 60 + "\n")
    
    example_local_backend()
    example_local_backend_with_options()
    example_backend_detection()
    example_explicit_backend()
    example_backend_type_enum()
    example_context_manager()
    
    print("=" * 60)
    print("所有示例运行完成！")
    print("=" * 60)


if __name__ == "__main__":
    main()

