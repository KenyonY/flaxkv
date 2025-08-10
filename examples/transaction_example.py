"""
FlaxKV 2.0 事务功能示例

演示FlaxKV的ACID事务特性和不同隔离级别的使用。
"""

import asyncio
import tempfile
from pathlib import Path
import flaxkv
from flaxkv.transaction.manager import IsolationLevel


async def basic_transaction_example():
    """基本事务使用示例"""
    print("=== 基本事务示例 ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        config = flaxkv.FlaxKVConfig.for_development()
        
        async with flaxkv.FlaxKV("tx_basic", "local", temp_dir, config) as db:
            # 初始化账户余额
            await db.mset({
                "account:alice": 1000.0,
                "account:bob": 500.0,
                "account:charlie": 750.0
            })
            await db.flush()  # 确保数据写入存储
            
            print("初始余额:")
            for account in ["alice", "bob", "charlie"]:
                balance = await db.get(f"account:{account}")
                print(f"  {account}: ${balance}")
            
            # 成功的转账事务
            print("\n✅ 执行转账事务: Alice -> Bob $200")
            async with db.transaction() as tx:
                alice_balance = await tx.get("account:alice")
                bob_balance = await tx.get("account:bob")
                
                if alice_balance >= 200:
                    await tx.set("account:alice", alice_balance - 200)
                    await tx.set("account:bob", bob_balance + 200)
                    print("   事务已提交")
                else:
                    raise ValueError("余额不足")
            
            # 验证转账结果
            print("\n转账后余额:")
            for account in ["alice", "bob"]:
                balance = await db.get(f"account:{account}")
                print(f"  {account}: ${balance}")
            
            # 失败的事务 - 演示回滚
            print("\n❌ 尝试无效转账: Alice -> Charlie $2000 (应该失败)")
            try:
                async with db.transaction() as tx:
                    alice_balance = await tx.get("account:alice")
                    if alice_balance < 2000:
                        raise ValueError("余额不足")
                    # 这部分代码不会执行
                    charlie_balance = await tx.get("account:charlie")
                    await tx.set("account:alice", alice_balance - 2000)
                    await tx.set("account:charlie", charlie_balance + 2000)
            except ValueError as e:
                print(f"   事务失败: {e}")
            
            print("\n事务失败后余额 (应该未变):")
            for account in ["alice", "charlie"]:
                balance = await db.get(f"account:{account}")
                print(f"  {account}: ${balance}")


async def isolation_levels_example():
    """事务隔离级别示例"""
    print("\n=== 事务隔离级别示例 ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        config = flaxkv.FlaxKVConfig.for_development()
        
        async with flaxkv.FlaxKV("tx_isolation", "local", temp_dir, config) as db:
            # 初始化数据
            await db.set("counter", 0)
            await db.set("user:1", {"name": "Alice", "status": "active"})
            await db.flush()
            
            # READ_COMMITTED 隔离级别
            print("\n1. READ_COMMITTED 隔离级别:")
            async with db.transaction(IsolationLevel.READ_COMMITTED) as tx:
                counter = await tx.get("counter")
                print(f"   读取 counter: {counter}")
                await tx.set("counter", counter + 1)
                print(f"   设置 counter: {counter + 1}")
            
            final_counter = await db.get("counter")
            print(f"   事务后 counter: {final_counter}")
            
            # REPEATABLE_READ 隔离级别
            print("\n2. REPEATABLE_READ 隔离级别:")
            async with db.transaction(IsolationLevel.REPEATABLE_READ) as tx:
                user = await tx.get("user:1")
                print(f"   读取用户: {user}")
                
                # 在事务中修改用户状态
                user["status"] = "inactive"
                user["last_modified"] = "2024-01-01"
                await tx.set("user:1", user)
                
                # 在同一事务中再次读取，应该看到修改后的值
                updated_user = await tx.get("user:1")
                print(f"   事务内再次读取: {updated_user}")
            
            final_user = await db.get("user:1")
            print(f"   事务后用户: {final_user}")
            
            # SERIALIZABLE 隔离级别
            print("\n3. SERIALIZABLE 隔离级别:")
            async with db.transaction(IsolationLevel.SERIALIZABLE) as tx:
                # 创建一个复杂的事务，涉及多个键
                await tx.set("inventory:item1", {"stock": 100, "reserved": 0})
                await tx.set("inventory:item2", {"stock": 50, "reserved": 0})
                
                # 模拟库存预留操作
                item1 = await tx.get("inventory:item1")
                item2 = await tx.get("inventory:item2")
                
                if item1["stock"] >= 10 and item2["stock"] >= 5:
                    item1["reserved"] = 10
                    item1["stock"] = item1["stock"] - 10
                    item2["reserved"] = 5
                    item2["stock"] = item2["stock"] - 5
                    
                    await tx.set("inventory:item1", item1)
                    await tx.set("inventory:item2", item2)
                    print("   库存预留成功")
                else:
                    raise ValueError("库存不足")
            
            # 验证最终状态
            item1 = await db.get("inventory:item1")
            item2 = await db.get("inventory:item2")
            print(f"   item1 最终状态: {item1}")
            print(f"   item2 最终状态: {item2}")


async def concurrent_transactions_example():
    """并发事务处理示例"""
    print("\n=== 并发事务示例 ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        config = flaxkv.FlaxKVConfig.for_development()
        
        async with flaxkv.FlaxKV("tx_concurrent", "local", temp_dir, config) as db:
            # 初始化共享计数器
            await db.set("shared_counter", 0)
            await db.flush()
            
            async def increment_counter(worker_id: int, increments: int):
                """工作线程：增加计数器"""
                for i in range(increments):
                    async with db.transaction() as tx:
                        counter = await tx.get("shared_counter")
                        await tx.set("shared_counter", counter + 1)
                        # 模拟一些处理时间
                        await asyncio.sleep(0.001)
                print(f"   Worker {worker_id} 完成")
            
            # 启动多个并发任务
            print("启动5个并发工作任务，每个增加10次计数器...")
            tasks = []
            for worker_id in range(5):
                task = asyncio.create_task(increment_counter(worker_id, 10))
                tasks.append(task)
            
            # 等待所有任务完成
            await asyncio.gather(*tasks)
            
            # 验证最终结果
            final_counter = await db.get("shared_counter")
            expected_value = 5 * 10  # 5个worker * 10次增加
            print(f"\n最终计数器值: {final_counter}")
            print(f"期望值: {expected_value}")
            print(f"结果: {'✅ 正确' if final_counter == expected_value else '❌ 不一致'}")


async def manual_transaction_control():
    """手动事务控制示例"""
    print("\n=== 手动事务控制示例 ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        config = flaxkv.FlaxKVConfig.for_development()
        
        async with flaxkv.FlaxKV("tx_manual", "local", temp_dir, config) as db:
            # 初始化数据
            await db.set("order:status", "pending")
            await db.set("payment:status", "pending")
            await db.flush()
            
            print("初始状态:")
            print(f"  订单状态: {await db.get('order:status')}")
            print(f"  支付状态: {await db.get('payment:status')}")
            
            # 手动控制事务
            print("\n开始手动事务...")
            tx = await db.begin_transaction(IsolationLevel.REPEATABLE_READ)
            
            try:
                # 第1步：更新订单状态
                order_status = await tx.get("order:status")
                if order_status == "pending":
                    await tx.set("order:status", "processing")
                    print("   订单状态 -> processing")
                
                # 第2步：模拟可能失败的操作
                payment_status = await tx.get("payment:status")
                if payment_status == "pending":
                    await tx.set("payment:status", "completed")
                    print("   支付状态 -> completed")
                
                # 第3步：模拟业务逻辑检查
                # 这里可以添加复杂的业务逻辑
                import random
                if random.random() < 0.8:  # 80% 成功率
                    await tx.set("order:status", "confirmed")
                    print("   订单确认成功")
                    
                    # 手动提交事务
                    await db.tx_manager.commit_transaction(tx)
                    print("✅ 事务手动提交成功")
                else:
                    # 模拟业务失败
                    raise ValueError("业务逻辑检查失败")
                    
            except Exception as e:
                print(f"❌ 事务失败: {e}")
                await db.tx_manager.abort_transaction(tx)
                print("   事务已回滚")
            
            # 验证最终状态
            print("\n最终状态:")
            print(f"  订单状态: {await db.get('order:status')}")
            print(f"  支付状态: {await db.get('payment:status')}")


async def transaction_performance_example():
    """事务性能测试示例"""
    print("\n=== 事务性能示例 ===")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        config = flaxkv.FlaxKVConfig.for_high_performance()
        
        async with flaxkv.FlaxKV("tx_perf", "local", temp_dir, config) as db:
            # 测试大量小事务的性能
            print("测试1000个小事务的性能...")
            start_time = asyncio.get_event_loop().time()
            
            for i in range(1000):
                async with db.transaction() as tx:
                    await tx.set(f"key_{i:04d}", f"value_{i}")
            
            end_time = asyncio.get_event_loop().time()
            duration = end_time - start_time
            
            print(f"   完成时间: {duration:.2f} 秒")
            print(f"   每秒事务数: {1000/duration:.1f} TPS")
            print(f"   平均事务延迟: {duration*1000/1000:.2f} ms")
            
            # 验证数据
            db_size = await db.size()
            print(f"   数据库大小: {db_size} 键")
            
            # 测试大事务的性能
            print("\n测试单个大事务的性能...")
            start_time = asyncio.get_event_loop().time()
            
            async with db.transaction() as tx:
                for i in range(1000, 2000):
                    await tx.set(f"batch_key_{i:04d}", {"id": i, "data": f"batch_value_{i}"})
            
            end_time = asyncio.get_event_loop().time()
            duration = end_time - start_time
            
            print(f"   完成时间: {duration:.2f} 秒")
            print(f"   每秒操作数: {1000/duration:.1f} ops/s")
            
            final_size = await db.size()
            print(f"   最终数据库大小: {final_size} 键")


async def main():
    """运行所有事务示例"""
    print("FlaxKV 2.0 事务功能示例")
    print("=" * 60)
    
    try:
        await basic_transaction_example()
        await isolation_levels_example()
        await concurrent_transactions_example()
        await manual_transaction_control()
        await transaction_performance_example()
        
        print("\n" + "=" * 60)
        print("✅ 所有事务示例运行完成！")
        
    except Exception as e:
        print(f"\n❌ 示例运行出错: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())