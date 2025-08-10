"""
FlaxKV 2.0 高级特性示例

演示FlaxKV的高级功能：TTL、备份恢复、监控等。
"""

import asyncio
import tempfile
import time
from pathlib import Path
import flaxkv


async def ttl_expiration_example():
    """TTL过期机制示例"""
    print("=== TTL过期机制示例 ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        config = flaxkv.FlaxKVConfig.for_testing()
        config.cache_cleanup_interval = 0.1  # 快速清理用于演示
        
        async with flaxkv.FlaxKV("ttl_db", "local", temp_dir, config) as db:
            # 使用 setex 设置带过期时间的键
            await db.setex("session:123", {"user_id": 456, "login_time": time.time()}, ttl=2)
            await db.setex("temp_data", "这是临时数据", ttl=1)
            
            # 设置普通键，然后添加过期时间
            await db.set("cache_key", {"data": "cached_value"})
            await db.expire("cache_key", 3)
            
            print("✅ 已设置TTL键:")
            print(f"  session:123 (2秒后过期): {await db.get('session:123')}")
            print(f"  temp_data (1秒后过期): {await db.get('temp_data')}")
            print(f"  cache_key (3秒后过期): {await db.get('cache_key')}")
            
            # 等待1.5秒，temp_data应该过期
            print("\n等待1.5秒...")
            await asyncio.sleep(1.5)
            
            print("检查过期状态:")
            try:
                session_data = await db.get("session:123")
                print(f"  session:123 仍存在: {session_data}")
            except KeyError:
                print("  session:123 已过期")
            
            try:
                temp_data = await db.get("temp_data")
                print(f"  temp_data 仍存在: {temp_data}")
            except KeyError:
                print("  temp_data 已过期 ✅")
            
            try:
                cache_data = await db.get("cache_key")
                print(f"  cache_key 仍存在: {cache_data}")
            except KeyError:
                print("  cache_key 已过期")
            
            # 再等待2秒，所有键都应该过期
            print("\n再等待2秒...")
            await asyncio.sleep(2)
            
            print("最终检查:")
            for key in ["session:123", "temp_data", "cache_key"]:
                exists = await db.contains(key)
                print(f"  {key}: {'存在' if exists else '已过期 ✅'}")


async def backup_restore_example():
    """备份和恢复示例"""
    print("\n=== 备份和恢复示例 ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "original_db"
        backup_path = Path(temp_dir) / "backup.flaxkv"
        restore_path = Path(temp_dir) / "restored_db"
        
        config = flaxkv.FlaxKVConfig.for_development()
        
        # 创建原始数据
        original_data = {
            "users:001": {"name": "Alice", "email": "alice@example.com", "age": 25},
            "users:002": {"name": "Bob", "email": "bob@example.com", "age": 30},
            "products:001": {"name": "Laptop", "price": 999.99, "category": "electronics"},
            "products:002": {"name": "Book", "price": 19.99, "category": "education"},
            "orders:001": {"user_id": "001", "product_id": "001", "quantity": 1},
            "settings": {"theme": "dark", "language": "zh-CN", "notifications": True}
        }
        
        async with flaxkv.FlaxKV("backup_demo", "local", db_path, config) as db:
            await db.mset(original_data)
            await db.flush()
            print(f"✅ 创建了 {len(original_data)} 条原始数据")
            
            # 执行备份
            print("\n执行备份...")
            backup_stats = await db.backup(str(backup_path))
            print(f"✅ 备份完成:")
            print(f"  备份文件: {backup_path}")
            print(f"  备份键数: {backup_stats['total_keys']}")
            print(f"  备份大小: {backup_stats.get('backup_size', 'N/A')} 字节")
        
        # 验证备份文件存在
        if backup_path.exists():
            print(f"  备份文件确认存在: {backup_path.stat().st_size} 字节")
        
        # 在新数据库中恢复数据
        print("\n从备份恢复数据...")
        async with flaxkv.FlaxKV("restore_demo", "local", restore_path, config) as restored_db:
            restore_stats = await restored_db.restore(str(backup_path))
            print(f"✅ 恢复完成:")
            print(f"  恢复键数: {restore_stats['restored_keys']}")
            print(f"  跳过键数: {restore_stats.get('skipped_keys', 0)}")
            
            # 验证恢复的数据
            print("\n验证恢复的数据:")
            restored_size = await restored_db.size()
            print(f"  数据库大小: {restored_size}")
            
            # 抽样检查几个键
            sample_keys = ["users:001", "products:001", "settings"]
            for key in sample_keys:
                try:
                    value = await restored_db.get(key)
                    original_value = original_data[key]
                    match = value == original_value
                    print(f"  {key}: {'✅ 匹配' if match else '❌ 不匹配'}")
                except KeyError:
                    print(f"  {key}: ❌ 未找到")


async def performance_monitoring_example():
    """性能监控示例"""
    print("\n=== 性能监控示例 ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        config = flaxkv.FlaxKVConfig.for_high_performance()
        config.metrics_enabled = True
        config.slow_query_threshold = 0.01  # 10ms作为慢查询阈值
        
        async with flaxkv.FlaxKV("monitor_db", "local", temp_dir, config) as db:
            # 执行各种操作来生成监控数据
            print("执行测试操作以生成监控数据...")
            
            # 1. 批量写入
            batch_data = {f"user:{i:06d}": {"id": i, "name": f"User{i}", "score": i * 10} 
                         for i in range(1000)}
            start_time = time.perf_counter()
            await db.mset(batch_data)
            batch_write_time = time.perf_counter() - start_time
            print(f"  批量写入1000条: {batch_write_time:.3f}s")
            
            # 2. 随机读取
            import random
            random_keys = random.sample(list(batch_data.keys()), 100)
            start_time = time.perf_counter()
            read_results = await db.mget(random_keys)
            batch_read_time = time.perf_counter() - start_time
            print(f"  批量读取100条: {batch_read_time:.3f}s")
            
            # 3. 扫描操作
            start_time = time.perf_counter()
            scan_count = 0
            async for key, value in db.scan(prefix="user:", limit=50):
                scan_count += 1
            scan_time = time.perf_counter() - start_time
            print(f"  前缀扫描50条: {scan_time:.3f}s")
            
            # 4. 单个读写操作
            for i in range(100):
                await db.set(f"test:{i}", f"value_{i}")
                await db.get(f"test:{i}")
            
            # 获取性能统计
            print("\n性能统计:")
            stats = db.get_stats()
            print(f"  总操作数: {stats['total_operations']}")
            print(f"  缓存命中率: {stats.get('cache_hits', 0) / max(stats.get('cache_hits', 0) + stats.get('cache_misses', 0), 1):.2%}")
            print(f"  运行时间: {stats['uptime']:.2f}s")
            
            # 缓存统计
            if 'cache' in stats:
                cache_stats = stats['cache']
                print(f"\n缓存统计:")
                print(f"  当前大小: {cache_stats['current_size']}")
                print(f"  最大大小: {cache_stats['max_size']}")
                print(f"  命中率: {cache_stats['hit_rate']:.2%}")
                print(f"  驱逐次数: {cache_stats.get('evictions', 0)}")
            
            # 数据库信息
            info = db.get_info()
            print(f"\n数据库信息:")
            print(f"  名称: {info['name']}")
            print(f"  后端: {info['backend']}")
            print(f"  已初始化: {info['initialized']}")
            print(f"  是否关闭: {info['closed']}")


async def advanced_scan_example():
    """高级扫描查询示例"""
    print("\n=== 高级扫描查询示例 ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        config = flaxkv.FlaxKVConfig.for_development()
        
        async with flaxkv.FlaxKV("scan_db", "local", temp_dir, config) as db:
            # 创建分层数据结构
            test_data = {}
            
            # 用户数据
            for i in range(1, 21):
                test_data[f"users:{i:03d}"] = {
                    "id": i,
                    "name": f"User{i}",
                    "department": "engineering" if i % 3 == 0 else "marketing" if i % 2 == 0 else "sales",
                    "salary": 50000 + i * 1000,
                    "join_date": f"2024-{i%12+1:02d}-01"
                }
            
            # 订单数据
            for i in range(1, 51):
                test_data[f"orders:2024:{i:04d}"] = {
                    "order_id": i,
                    "user_id": i % 20 + 1,
                    "amount": 100 + i * 10,
                    "status": "completed" if i % 3 == 0 else "pending",
                    "date": f"2024-{i%12+1:02d}-{i%28+1:02d}"
                }
            
            # 产品数据
            categories = ["electronics", "books", "clothing", "home"]
            for i, category in enumerate(categories, 1):
                for j in range(1, 6):
                    product_id = i * 10 + j
                    test_data[f"products:{category}:{product_id:03d}"] = {
                        "id": product_id,
                        "name": f"{category.title()} Item {j}",
                        "category": category,
                        "price": 50 + j * 20,
                        "in_stock": j % 2 == 1
                    }
            
            await db.mset(test_data)
            print(f"✅ 创建了 {len(test_data)} 条测试数据")
            
            # 1. 前缀扫描 - 用户数据
            print("\n1. 扫描所有用户 (前5个):")
            count = 0
            async for key, value in db.scan(prefix="users:", limit=5):
                print(f"   {key}: {value['name']} - {value['department']}")
                count += 1
            
            # 2. 范围扫描 - 特定用户ID范围
            print("\n2. 扫描用户ID 010-015:")
            async for key, value in db.scan(start="users:010", end="users:016"):
                print(f"   {key}: {value['name']} (${value['salary']})")
            
            # 3. 分层扫描 - 特定类别的产品
            print("\n3. 扫描电子产品:")
            async for key, value in db.scan(prefix="products:electronics:"):
                status = "有库存" if value['in_stock'] else "缺货"
                print(f"   {key}: {value['name']} - ${value['price']} ({status})")
            
            # 4. 复杂扫描 - 2024年订单
            print("\n4. 扫描2024年订单 (前10个):")
            async for key, value in db.scan(prefix="orders:2024:", limit=10):
                print(f"   {key}: User{value['user_id']} - ${value['amount']} ({value['status']})")
            
            # 5. 迭代器模式扫描
            print("\n5. 按键类型统计:")
            key_counts = {"users": 0, "orders": 0, "products": 0}
            async for key, value in db.scan():
                key_type = key.split(":")[0]
                if key_type in key_counts:
                    key_counts[key_type] += 1
            
            for key_type, count in key_counts.items():
                print(f"   {key_type}: {count} 条记录")


async def cache_management_example():
    """缓存管理示例"""
    print("\n=== 缓存管理示例 ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        config = flaxkv.FlaxKVConfig(
            cache_size="64MB",
            cache_policy="lru",
            buffer_size=1000
        )
        
        async with flaxkv.FlaxKV("cache_db", "local", temp_dir, config) as db:
            # 填充数据以测试缓存
            print("填充测试数据...")
            for i in range(1000):
                await db.set(f"data:{i:04d}", {"index": i, "value": f"test_value_{i}", "data": "x" * 100})
            
            # 热身缓存 - 访问前100个键
            print("热身缓存...")
            for i in range(100):
                await db.get(f"data:{i:04d}")
            
            # 获取缓存统计
            stats = db.get_stats()
            if 'cache' in stats:
                cache_info = stats['cache']
                print(f"\n缓存统计 (热身后):")
                print(f"  当前大小: {cache_info['current_size']}")
                print(f"  最大大小: {cache_info['max_size']}")
                print(f"  命中率: {cache_info['hit_rate']:.2%}")
            
            # 测试缓存命中
            print("\n测试缓存性能:")
            
            # 缓存命中测试
            start_time = time.perf_counter()
            for i in range(100):  # 访问已缓存的数据
                await db.get(f"data:{i:04d}")
            cache_hit_time = time.perf_counter() - start_time
            
            # 缓存未命中测试
            start_time = time.perf_counter()
            for i in range(900, 1000):  # 访问未缓存的数据
                await db.get(f"data:{i:04d}")
            cache_miss_time = time.perf_counter() - start_time
            
            print(f"  缓存命中时间 (100次): {cache_hit_time*1000:.2f}ms")
            print(f"  缓存未命中时间 (100次): {cache_miss_time*1000:.2f}ms")
            print(f"  性能提升: {cache_miss_time/cache_hit_time:.1f}x")
            
            # 最终缓存统计
            final_stats = db.get_stats()
            if 'cache' in final_stats:
                cache_info = final_stats['cache']
                print(f"\n最终缓存统计:")
                print(f"  命中率: {cache_info['hit_rate']:.2%}")
                print(f"  总访问: {stats.get('cache_hits', 0) + stats.get('cache_misses', 0)}")


async def main():
    """运行所有高级特性示例"""
    print("FlaxKV 2.0 高级特性示例")
    print("=" * 60)
    
    try:
        await ttl_expiration_example()
        await backup_restore_example()
        await performance_monitoring_example()
        await advanced_scan_example()
        await cache_management_example()
        
        print("\n" + "=" * 60)
        print("✅ 所有高级特性示例运行完成！")
        
    except Exception as e:
        print(f"\n❌ 示例运行出错: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())