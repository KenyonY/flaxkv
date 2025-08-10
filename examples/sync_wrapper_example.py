#!/usr/bin/env python3
"""
ThreadSafeFlaxKV 同步封装示例

展示如何在同步应用中使用FlaxKV的高性能同步封装。
"""

import time
import threading
import tempfile
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any

import flaxkv
from flaxkv.core.sync_wrapper import ThreadSafeFlaxKV, create_sync_flaxkv


def basic_usage_example():
    """基本用法示例。"""
    print("=" * 50)
    print("基本用法示例")
    print("=" * 50)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # 创建同步FlaxKV实例
        config = flaxkv.FlaxKVConfig.for_production()
        db = ThreadSafeFlaxKV("sync_example", "local", temp_dir, config)
        
        with db:
            # 基本操作
            db.set("hello", "world")
            value = db.get("hello")
            print(f"基本读写: hello = {value}")
            
            # 字典式操作
            db["user:123"] = {"name": "Alice", "age": 25}
            user = db["user:123"]
            print(f"字典式操作: user = {user}")
            
            # 检查包含和大小
            print(f"包含 'hello': {'hello' in db}")
            print(f"数据库大小: {len(db)}")
            
            # 批量操作
            batch_data = {f"item_{i}": f"value_{i}" for i in range(5)}
            db.mset(batch_data)
            
            batch_result = db.mget(["item_0", "item_2", "item_4"])
            print(f"批量读取: {batch_result}")
            
            # TTL操作
            db.setex("temp_key", "temp_value", ttl=2)
            print(f"TTL键设置: temp_key = {db.get('temp_key')}")
            
            # 扫描操作
            print("扫描 'item_' 前缀的键:")
            for key, value in db.scan(prefix="item_", limit=3):
                print(f"  {key} = {value}")
            
            print(f"所有键: {db.keys()[:5]}...")  # 只显示前5个


def web_application_example():
    """Web应用集成示例（模拟）。"""
    print("=" * 50)
    print("Web应用集成示例")
    print("=" * 50)
    
    # 模拟Web应用配置
    class MockWebApp:
        def __init__(self):
            self.config: Dict[str, Any] = {}
    
    app = MockWebApp()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # 在应用启动时创建数据库连接
        config = flaxkv.FlaxKVConfig.for_production()
        app.config['DB'] = ThreadSafeFlaxKV("webapp_db", "local", temp_dir, config)
        
        with app.config['DB'] as db:
            # 模拟路由处理函数
            def create_user(user_id: str, user_data: dict):
                """创建用户的路由处理函数。"""
                db.set(f"user:{user_id}", user_data)
                return f"User {user_id} created"
            
            def get_user(user_id: str):
                """获取用户的路由处理函数。"""
                user_data = db.get(f"user:{user_id}")
                if user_data is None:
                    return f"User {user_id} not found", 404
                return user_data
            
            def update_user_score(user_id: str, score: int):
                """更新用户分数（事务操作）。"""
                with db.transaction() as tx:
                    user_data = tx.get(f"user:{user_id}")
                    if user_data is None:
                        raise ValueError(f"User {user_id} not found")
                    
                    user_data["score"] = score
                    tx.set(f"user:{user_id}", user_data)
                    
                    # 更新排行榜
                    tx.set(f"leaderboard:{user_id}", score)
                
                return f"User {user_id} score updated to {score}"
            
            # 模拟请求处理
            print("模拟Web请求处理:")
            
            # 创建用户
            result = create_user("alice", {"name": "Alice", "email": "alice@example.com", "score": 0})
            print(f"创建用户: {result}")
            
            # 刷新确保数据持久化
            db.flush()
            
            # 获取用户
            user = get_user("alice")
            print(f"获取用户: {user}")
            
            # 更新分数
            result = update_user_score("alice", 95)
            print(f"更新分数: {result}")
            
            # 验证更新
            updated_user = get_user("alice")
            print(f"更新后用户: {updated_user}")


def multithreading_example():
    """多线程使用示例。"""
    print("=" * 50)
    print("多线程使用示例")
    print("=" * 50)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        config = flaxkv.FlaxKVConfig.for_high_performance()
        db = ThreadSafeFlaxKV("multithread", "local", temp_dir, config)
        
        with db:
            # 统计结果
            results = {"writes": 0, "reads": 0, "errors": 0}
            results_lock = threading.Lock()
            
            def worker_write(worker_id: int, count: int):
                """写入工作函数。"""
                try:
                    for i in range(count):
                        key = f"worker_{worker_id}_item_{i}"
                        value = {"worker": worker_id, "item": i, "timestamp": time.time()}
                        db.set(key, value)
                        
                        with results_lock:
                            results["writes"] += 1
                            
                        if i % 20 == 0:  # 每20次操作打印一次进度
                            print(f"Worker {worker_id}: 已写入 {i+1}/{count}")
                
                except Exception as e:
                    print(f"Worker {worker_id} 写入错误: {e}")
                    with results_lock:
                        results["errors"] += 1
            
            def worker_read(worker_id: int, read_keys: list):
                """读取工作函数。"""
                try:
                    for key in read_keys:
                        value = db.get(key)
                        if value is not None:
                            with results_lock:
                                results["reads"] += 1
                    
                    print(f"Reader {worker_id}: 完成读取 {len(read_keys)} 个键")
                
                except Exception as e:
                    print(f"Reader {worker_id} 读取错误: {e}")
                    with results_lock:
                        results["errors"] += 1
            
            # 启动多个写入线程
            write_threads = []
            for i in range(3):
                thread = threading.Thread(
                    target=worker_write, 
                    args=(i, 50),
                    name=f"Writer-{i}"
                )
                write_threads.append(thread)
                thread.start()
            
            # 等待写入完成
            for thread in write_threads:
                thread.join()
            
            print(f"写入阶段完成，共写入 {results['writes']} 条记录")
            
            # 启动多个读取线程
            all_keys = db.keys()
            keys_per_reader = len(all_keys) // 3
            
            read_threads = []
            for i in range(3):
                start_idx = i * keys_per_reader
                end_idx = start_idx + keys_per_reader if i < 2 else len(all_keys)
                reader_keys = all_keys[start_idx:end_idx]
                
                thread = threading.Thread(
                    target=worker_read,
                    args=(i, reader_keys),
                    name=f"Reader-{i}"
                )
                read_threads.append(thread)
                thread.start()
            
            # 等待读取完成
            for thread in read_threads:
                thread.join()
            
            print(f"读取阶段完成，共读取 {results['reads']} 条记录")
            print(f"总错误数: {results['errors']}")
            
            # 最终统计
            final_stats = db.get_stats()
            print(f"最终统计: 总操作数 = {final_stats['total_operations']}")


def performance_comparison():
    """性能对比示例。"""
    print("=" * 50)
    print("性能对比示例")
    print("=" * 50)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        config = flaxkv.FlaxKVConfig.for_high_performance()
        
        # 测试同步封装的性能
        print("测试ThreadSafeFlaxKV性能:")
        sync_db = ThreadSafeFlaxKV("perf_sync", "local", temp_dir, config, timeout=60.0)
        
        with sync_db:
            # 批量写入测试
            start_time = time.time()
            test_data = {f"perf_key_{i}": f"perf_value_{i}" for i in range(1000)}
            sync_db.mset(test_data)
            write_time = time.time() - start_time
            
            # 批量读取测试
            start_time = time.time()
            keys_to_read = list(test_data.keys())[:500]
            results = sync_db.mget(keys_to_read)
            read_time = time.time() - start_time
            
            print(f"同步封装 - 写入1000条记录耗时: {write_time:.3f}s")
            print(f"同步封装 - 读取500条记录耗时: {read_time:.3f}s")
            print(f"同步封装 - 读取成功: {len(results)}/500")
            
            # 连续操作测试
            start_time = time.time()
            for i in range(100):
                sync_db.set(f"seq_{i}", {"index": i, "data": f"sequential_{i}"})
            sequential_time = time.time() - start_time
            
            print(f"同步封装 - 连续写入100条记录耗时: {sequential_time:.3f}s")
            
            # 获取详细统计
            stats = sync_db.get_stats()
            cache_hit_rate = stats.get('cache', {}).get('hit_rate', 0)
            print(f"缓存命中率: {cache_hit_rate:.2%}")


def transaction_example():
    """事务使用示例。"""
    print("=" * 50)
    print("事务使用示例")
    print("=" * 50)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        config = flaxkv.FlaxKVConfig.for_production()
        db = ThreadSafeFlaxKV("transaction_test", "local", temp_dir, config)
        
        with db:
            # 初始化账户
            db.set("account:alice", 100)
            db.set("account:bob", 50)
            
            print(f"初始状态:")
            print(f"  Alice: {db.get('account:alice')}")
            print(f"  Bob: {db.get('account:bob')}")
            
            # 成功的转账事务
            try:
                with db.transaction(isolation_level="SERIALIZABLE") as tx:
                    alice_balance = tx.get("account:alice")
                    bob_balance = tx.get("account:bob")
                    
                    if alice_balance < 30:
                        raise ValueError("余额不足")
                    
                    tx.set("account:alice", alice_balance - 30)
                    tx.set("account:bob", bob_balance + 30)
                    
                    # 记录转账日志
                    tx.set("transfer:1", {
                        "from": "alice",
                        "to": "bob", 
                        "amount": 30,
                        "timestamp": time.time()
                    })
                
                print(f"转账成功后:")
                print(f"  Alice: {db.get('account:alice')}")
                print(f"  Bob: {db.get('account:bob')}")
                print(f"  转账记录: {db.get('transfer:1')}")
                
            except Exception as e:
                print(f"转账失败: {e}")
            
            # 失败的转账事务（余额不足）
            try:
                with db.transaction(isolation_level="SERIALIZABLE") as tx:
                    alice_balance = tx.get("account:alice")
                    bob_balance = tx.get("account:bob")
                    
                    if alice_balance < 100:  # 尝试转账100，但余额只有70
                        raise ValueError("余额不足")
                    
                    tx.set("account:alice", alice_balance - 100)
                    tx.set("account:bob", bob_balance + 100)
                
            except Exception as e:
                print(f"转账失败（预期）: {e}")
                
            print(f"失败转账后余额未变:")
            print(f"  Alice: {db.get('account:alice')}")
            print(f"  Bob: {db.get('account:bob')}")


def main():
    """主函数，运行所有示例。"""
    print("FlaxKV ThreadSafeFlaxKV 同步封装示例")
    print("=" * 80)
    
    try:
        basic_usage_example()
        web_application_example()
        multithreading_example()
        performance_comparison()
        transaction_example()
        
        print("=" * 80)
        print("✅ 所有示例运行完成！")
        
    except KeyboardInterrupt:
        print("\n⚠️ 用户中断执行")
    except Exception as e:
        print(f"❌ 示例执行失败: {e}")
        raise


if __name__ == "__main__":
    main()