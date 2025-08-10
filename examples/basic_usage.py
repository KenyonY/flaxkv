"""
FlaxKV 2.0 基本使用示例

展示FlaxKV的核心功能和用法。
"""

import asyncio
import tempfile
from pathlib import Path
import flaxkv


async def basic_operations_example():
    """基本CRUD操作示例"""
    print("=== 基本操作示例 ===")
    
    # 创建临时目录
    with tempfile.TemporaryDirectory() as temp_dir:
        config = flaxkv.FlaxKVConfig.for_development()
        
        async with flaxkv.FlaxKV("example_db", "local", temp_dir, config) as db:
            # 设置键值对
            await db.set("user:001", {"name": "Alice", "age": 25, "city": "Beijing"})
            await db.set("user:002", {"name": "Bob", "age": 30, "city": "Shanghai"})
            await db.set("product:001", {"name": "iPhone", "price": 999.99})
            
            print("✅ 数据已写入")
            
            # 获取单个值
            user = await db.get("user:001")
            print(f"用户信息: {user}")
            
            # 检查键是否存在
            exists = await db.contains("user:001")
            print(f"user:001 存在: {exists}")
            
            # 获取数据库大小
            size = await db.size()
            print(f"数据库大小: {size} 个键")
            
            # 删除键
            deleted = await db.delete("product:001")
            print(f"删除product:001: {'成功' if deleted else '失败'}")


async def batch_operations_example():
    """批量操作示例"""
    print("\n=== 批量操作示例 ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        config = flaxkv.FlaxKVConfig.for_development()
        
        async with flaxkv.FlaxKV("batch_db", "local", temp_dir, config) as db:
            # 批量写入
            users = {
                f"user:{i:03d}": {
                    "name": f"User{i}",
                    "age": 20 + i % 50,
                    "score": i * 10
                }
                for i in range(1, 11)
            }
            
            await db.mset(users)
            print("✅ 批量写入完成")
            
            # 批量读取
            keys = [f"user:{i:03d}" for i in range(1, 6)]
            result = await db.mget(keys)
            print(f"批量读取结果: {len(result)} 个用户")
            
            for key, value in result.items():
                print(f"  {key}: {value['name']}, 年龄 {value['age']}")
            
            # 批量删除
            delete_keys = [f"user:{i:03d}" for i in range(6, 11)]
            deleted_count = await db.mdelete(delete_keys)
            print(f"批量删除: {deleted_count} 个键")


async def scan_operations_example():
    """扫描查询示例"""
    print("\n=== 扫描查询示例 ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        config = flaxkv.FlaxKVConfig.for_development()
        
        async with flaxkv.FlaxKV("scan_db", "local", temp_dir, config) as db:
            # 准备测试数据
            test_data = {
                "user:001": {"name": "Alice", "type": "premium"},
                "user:002": {"name": "Bob", "type": "basic"},
                "user:003": {"name": "Charlie", "type": "premium"},
                "product:001": {"name": "Laptop", "category": "electronics"},
                "product:002": {"name": "Book", "category": "education"},
                "order:001": {"user": "Alice", "total": 299.99},
                "order:002": {"user": "Bob", "total": 19.99},
            }
            
            await db.mset(test_data)
            print("✅ 测试数据准备完成")
            
            # 前缀扫描
            print("\n用户数据:")
            async for key, value in db.scan(prefix="user:"):
                print(f"  {key}: {value['name']} ({value['type']})")
            
            print("\n产品数据:")
            async for key, value in db.scan(prefix="product:"):
                print(f"  {key}: {value['name']} - {value['category']}")
            
            # 范围扫描
            print("\n范围查询 (user:001 到 user:003):")
            async for key, value in db.scan(start="user:001", end="user:004"):
                print(f"  {key}: {value['name']}")
            
            # 限制数量扫描
            print("\n限制查询 (前3个键):")
            count = 0
            async for key, value in db.scan(limit=3):
                print(f"  {key}: {value}")
                count += 1


async def transaction_example():
    """事务操作示例"""
    print("\n=== 事务操作示例 ===")

    with tempfile.TemporaryDirectory() as temp_dir:
        config = flaxkv.FlaxKVConfig.for_development()

        async with flaxkv.FlaxKV("tx_db", "local", temp_dir, config) as db:
            # 使用 mset 原子性的初始化账户，避免状态不一致
            await db.mset({
                "account:alice": 1000.0,
                "account:bob": 500.0
            })
            # 强制将写缓冲区刷新到磁盘，确保后续事务能读取到
            await db.flush()

            print("初始账户余额:")
            # 直接从 db 获取，确保数据已写入
            alice_balance = await db.get("account:alice")
            bob_balance = await db.get("account:bob")
            print(f"  Alice: ${alice_balance}")
            print(f"  Bob: ${bob_balance}")

            # 成功的转账事务
            print("\n尝试进行一笔成功的转账 (Alice -> Bob $200)...")
            async with db.transaction() as tx:
                # 从Alice转账200给Bob
                alice_balance = await tx.get("account:alice")
                bob_balance = await tx.get("account:bob")

                if alice_balance >= 200:
                    await tx.set("account:alice", alice_balance - 200)
                    await tx.set("account:bob", bob_balance + 200)
                    print("✅ 转账事务提交成功")
                else:
                    # This part of the logic will not be reached in the example
                    raise ValueError("余额不足")

            print("\n转账后账户余额:")
            alice_balance = await db.get("account:alice")
            bob_balance = await db.get("account:bob")
            print(f"  Alice: ${alice_balance}")
            print(f"  Bob: ${bob_balance}")

            # 失败的事务（会自动回滚）
            print("\n尝试进行一笔失败的转账 (Alice -> Bob $2000)，预期将回滚...")
            try:
                async with db.transaction() as tx:
                    alice_balance = await tx.get("account:alice")
                    if alice_balance < 2000:
                        # Manually raise the error to simulate a failed check
                        raise ValueError("余额不足，无法转账 $2000")

                    # This code will not be reached
                    await tx.set("account:alice", alice_balance - 2000)
                    print("这一行不应该被执行")

            except ValueError as e:
                print(f"✅ 事务失败并成功回滚: {e}")

            print("\n回滚后账户余额:")
            alice_balance = await db.get("account:alice")
            bob_balance = await db.get("account:bob")
            print(f"  Alice: ${alice_balance} (未变化)")
            print(f"  Bob: ${bob_balance} (未变化)")




async def configuration_example():
    """配置选项示例"""
    print("\n=== 配置选项示例 ===")
    
    # 不同的预设配置
    configs = {
        "开发环境": flaxkv.FlaxKVConfig.for_development(),
        "生产环境": flaxkv.FlaxKVConfig.for_production(), 
        "高性能": flaxkv.FlaxKVConfig.for_high_performance(),
        "测试环境": flaxkv.FlaxKVConfig.for_testing(),
    }
    
    for name, config in configs.items():
        print(f"\n{name}配置:")
        print(f"  缓冲区大小: {config.buffer_size}")
        print(f"  缓存大小: {config.cache_size}")
        print(f"  同步模式: {config.sync_mode}")
        print(f"  缓存策略: {config.cache_policy}")
        print(f"  监控启用: {config.metrics_enabled}")
    
    # 自定义配置
    custom_config = flaxkv.FlaxKVConfig(
        buffer_size=2000,
        cache_size="256MB",
        sync_mode="async",
        cache_policy="arc",
        compression=True,
        backup_interval=1800,
        metrics_enabled=True,
        slow_query_threshold=0.5
    )
    
    print(f"\n自定义配置:")
    print(f"  {custom_config}")


async def monitoring_example():
    """监控和统计示例"""
    print("\n=== 监控和统计示例 ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        config = flaxkv.FlaxKVConfig.for_development()
        
        async with flaxkv.FlaxKV("monitor_db", "local", temp_dir, config) as db:
            # 执行一些操作
            for i in range(100):
                await db.set(f"key_{i:03d}", f"value_{i}")
            
            for i in range(0, 100, 10):
                await db.get(f"key_{i:03d}")
            
            await db.mget([f"key_{i:03d}" for i in range(10)])
            
            # 获取统计信息
            stats = db.get_stats()
            print("数据库统计:")
            print(f"  总操作数: {stats['total_operations']}")
            print(f"  缓存命中: {stats.get('cache_hits', 0)}")
            print(f"  缓存未命中: {stats.get('cache_misses', 0)}")
            print(f"  运行时间: {stats['uptime']:.2f} 秒")
            
            # 获取详细信息
            info = db.get_info()
            print(f"\n数据库信息:")
            print(f"  名称: {info['name']}")
            print(f"  后端: {info['backend']}")
            print(f"  位置: {info['location']}")
            print(f"  已初始化: {info['initialized']}")
            
            # 缓存信息
            if 'cache' in stats:
                cache_info = stats['cache']
                print(f"\n缓存信息:")
                print(f"  当前大小: {cache_info['current_size']}")
                print(f"  最大大小: {cache_info['max_size']}")
                print(f"  命中率: {cache_info['hit_rate']:.2%}")


async def data_persistence_example():
    """数据持久化示例"""
    print("\n=== 数据持久化示例 ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "persistent_db"
        config = flaxkv.FlaxKVConfig.for_development()
        
        # 第一个会话：写入数据
        async with flaxkv.FlaxKV("persist_test", "local", db_path, config) as db:
            await db.set("persistent_key", "这个值会被持久化")
            await db.set("user_data", {"name": "持久化用户", "id": 12345})
            await db.flush()  # 强制刷新到磁盘
            print("✅ 数据已写入并持久化")
        
        # 第二个会话：读取数据
        async with flaxkv.FlaxKV("persist_test", "local", db_path, config) as db:
            try:
                value = await db.get("persistent_key")
                user_data = await db.get("user_data")
                print(f"✅ 读取持久化数据: {value}")
                print(f"✅ 读取用户数据: {user_data}")
            except KeyError:
                print("❌ 数据未能持久化")
        
        print("✅ 数据持久化测试完成")


async def main():
    """运行所有示例"""
    print("FlaxKV 2.0 基本使用示例")
    print("=" * 50)
    
    try:
        await basic_operations_example()
        await batch_operations_example() 
        await scan_operations_example()
        await transaction_example()
        await configuration_example()
        await monitoring_example()
        await data_persistence_example()
        
        print("\n" + "=" * 50)
        print("✅ 所有示例运行完成！")
        
    except Exception as e:
        print(f"\n❌ 示例运行出错: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())