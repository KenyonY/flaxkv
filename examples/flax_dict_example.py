#!/usr/bin/env python3
"""
FlaxDict 使用示例

展示FlaxDict完全兼容Python dict接口的持久化字典功能。
"""

import tempfile
import time
import flaxkv


def basic_dict_usage():
    """基本字典用法示例。"""
    print("=" * 50)
    print("基本字典用法示例")
    print("=" * 50)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # 创建持久化字典
        db = flaxkv.FlaxDict('example_db', path=temp_dir)
        
        with db:
            # 基本操作 - 完全像Python字典一样
            db['key'] = 'value'
            db['number'] = 42
            db['data'] = {'nested': True, 'list': [1, 2, 3]}
            
            print(f"db['key'] = {db['key']}")
            print(f"db['number'] = {db['number']}")
            print(f"db['data'] = {db['data']}")
            
            # 字典方法
            print(f"len(db) = {len(db)}")
            print(f"'key' in db = {'key' in db}")
            print(f"'missing' in db = {'missing' in db}")
            
            # setdefault
            existing = db.setdefault('key', 'new_value')
            new_value = db.setdefault('new_key', 'default')
            print(f"setdefault existing: {existing}")
            print(f"setdefault new: {new_value}")
            
            # update
            db.update({'a': 1, 'b': 2})
            print(f"After update: len(db) = {len(db)}")
            
            # pop
            popped = db.pop('a')
            print(f"Popped 'a': {popped}")
            
            # 迭代
            print("所有键值对:")
            for key, value in db.items():
                print(f"  {key}: {value}")


def persistence_demo():
    """持久化演示。"""
    print("=" * 50)
    print("持久化演示")
    print("=" * 50)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # 第一次使用 - 写入数据
        with flaxkv.FlaxDict('persistent_db', path=temp_dir) as db:
            db['user:123'] = {'name': 'Alice', 'age': 25, 'city': 'New York'}
            db['user:456'] = {'name': 'Bob', 'age': 30, 'city': 'San Francisco'}
            db['config'] = {'theme': 'dark', 'language': 'en'}
            
            print(f"首次写入，数据库大小: {len(db)}")
        
        # 第二次使用 - 数据自动恢复
        with flaxkv.FlaxDict('persistent_db', path=temp_dir) as db:
            print(f"重新打开，数据库大小: {len(db)}")
            print(f"用户Alice: {db['user:123']}")
            print(f"配置信息: {db['config']}")
            
            # 修改数据
            user_alice = db['user:123']
            user_alice['age'] = 26
            db['user:123'] = user_alice
            
            print(f"修改后Alice的年龄: {db['user:123']['age']}")


def advanced_operations():
    """高级操作示例。"""
    print("=" * 50)
    print("高级操作示例")
    print("=" * 50)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        db = flaxkv.FlaxDict('advanced_db', path=temp_dir)
        
        with db:
            # 批量操作
            batch_data = {f'item_{i}': f'value_{i}' for i in range(100)}
            db.mset(batch_data)
            
            # 批量获取
            some_items = db.mget(['item_0', 'item_50', 'item_99'])
            print(f"批量获取结果: {some_items}")
            
            # 批量删除
            to_delete = ['item_0', 'item_1', 'item_2']
            deleted_count = db.mdelete(to_delete)
            print(f"批量删除了 {deleted_count} 个项目")
            
            # TTL支持
            db.setex('temp_data', {'expires': True}, ttl=2)
            print(f"TTL数据: {db['temp_data']}")
            
            # 扫描操作
            print("扫描前10个item_前缀的键:")
            scan_count = 0
            for key, value in db.scan(prefix='item_', limit=10):
                print(f"  {key}: {value}")
                scan_count += 1
            print(f"扫描到 {scan_count} 个项目")
            
            # 字典方法
            copy_dict = db.copy()
            print(f"拷贝到普通字典，大小: {len(copy_dict)}")


def transaction_example():
    """事务示例。"""
    print("=" * 50)
    print("事务示例")
    print("=" * 50)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        db = flaxkv.FlaxDict('transaction_db', path=temp_dir)
        
        with db:
            # 初始化账户
            db['account:alice'] = 1000
            db['account:bob'] = 500
            db.flush()  # 确保数据持久化
            
            print(f"转账前 - Alice: {db['account:alice']}, Bob: {db['account:bob']}")
            
            # 原子转账操作
            with db.transaction() as tx:
                alice_balance = tx.get('account:alice')
                bob_balance = tx.get('account:bob')
                
                transfer_amount = 200
                
                tx.set('account:alice', alice_balance - transfer_amount)
                tx.set('account:bob', bob_balance + transfer_amount)
                
                # 记录转账日志
                tx.set('transfer:latest', {
                    'from': 'alice',
                    'to': 'bob',
                    'amount': transfer_amount,
                    'timestamp': time.time()
                })
            
            # 验证转账结果
            db.flush()
            print(f"转账后 - Alice: {db['account:alice']}, Bob: {db['account:bob']}")
            print(f"转账记录: {db['transfer:latest']}")


def performance_demo():
    """性能演示。"""
    print("=" * 50)
    print("性能演示")
    print("=" * 50)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        db = flaxkv.FlaxDict('perf_db', path=temp_dir)
        
        with db:
            # 大量写入测试
            print("写入10000条记录...")
            start_time = time.time()
            
            # 使用批量操作提高性能
            batch_data = {}
            for i in range(10000):
                batch_data[f'perf_key_{i}'] = {
                    'id': i,
                    'data': f'test_data_{i}',
                    'timestamp': time.time()
                }
                
                # 每1000条批量写入一次
                if len(batch_data) == 1000:
                    db.mset(batch_data)
                    batch_data.clear()
            
            # 写入剩余数据
            if batch_data:
                db.mset(batch_data)
            
            write_time = time.time() - start_time
            print(f"写入完成，耗时: {write_time:.2f}秒")
            print(f"数据库大小: {len(db)} 条记录")
            
            # 读取性能测试
            print("随机读取1000条记录...")
            start_time = time.time()
            
            keys_to_read = [f'perf_key_{i}' for i in range(0, 10000, 10)]
            results = db.mget(keys_to_read)
            
            read_time = time.time() - start_time
            print(f"读取完成，耗时: {read_time:.2f}秒")
            print(f"成功读取: {len(results)} 条记录")
            
            # 统计信息
            stats = db.get_stats()
            cache_hit_rate = stats.get('cache', {}).get('hit_rate', 0)
            total_ops = stats.get('total_operations', 0)
            print(f"总操作数: {total_ops}")
            print(f"缓存命中率: {cache_hit_rate:.2%}")


def dict_compatibility_demo():
    """字典兼容性演示。"""
    print("=" * 50)
    print("字典兼容性演示")
    print("=" * 50)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        db = flaxkv.FlaxDict('compat_db', path=temp_dir)
        
        with db:
            # 演示完全兼容dict的接口
            print("FlaxDict完全兼容Python dict接口:")
            
            # 基本操作
            db['a'] = 1
            db['b'] = 2
            db['c'] = 3
            
            # 所有dict方法都可用
            print(f"keys(): {list(db.keys())}")
            print(f"values(): {list(db.values())}")
            print(f"items(): {list(db.items())}")
            
            # get方法
            print(f"get('a'): {db.get('a')}")
            print(f"get('missing', 'default'): {db.get('missing', 'default')}")
            
            # pop方法
            print(f"pop('b'): {db.pop('b')}")
            print(f"pop('missing', 'default'): {db.pop('missing', 'default')}")
            
            # popitem方法
            item = db.popitem()
            print(f"popitem(): {item}")
            
            # 清空
            db.clear()
            print(f"clear()后长度: {len(db)}")
            
            print("\n所以FlaxDict可以直接替换dict使用!")


def main():
    """运行所有示例。"""
    print("FlaxDict - 持久化字典示例")
    print("=" * 80)
    
    try:
        basic_dict_usage()
        persistence_demo()
        advanced_operations()
        transaction_example()
        performance_demo()
        dict_compatibility_demo()
        
        print("=" * 80)
        print("✅ 所有示例运行完成!")
        
    except KeyboardInterrupt:
        print("\n⚠️ 用户中断执行")
    except Exception as e:
        print(f"❌ 示例执行失败: {e}")
        raise


if __name__ == "__main__":
    main()