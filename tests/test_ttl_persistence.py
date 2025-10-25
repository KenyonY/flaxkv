from flaxkv2 import FlaxKV
import time
import datetime
import os
import shutil
import sys

# 测试数据库路径
DB_PATH = "./test_ttl_data"

def clear_test_db():
    """清除测试数据库"""
    if os.path.exists(DB_PATH):
        shutil.rmtree(DB_PATH)
    os.makedirs(DB_PATH, exist_ok=True)

def test_ttl_persistence():
    """测试TTL持久化功能"""
    print("===== 测试TTL持久化功能 =====")
    
    # 清除测试数据库
    clear_test_db()
    
    # 第一次运行：创建数据库并设置TTL
    print("\n第一次运行: 创建数据库并设置TTL")
    db = FlaxKV("test_db", DB_PATH)
    
    # 设置测试键值对
    db["key1"] = "value1"
    
    # 设置较长的TTL (30秒)
    ttl_seconds = 30
    db.set_ttl("key1", ttl_seconds)
    
    # 获取当前TTL
    current_time = datetime.datetime.now()
    initial_ttl = db.get_ttl("key1")
    print(f"当前时间: {current_time}")
    print(f"键值: {db.get('key1')}")
    print(f"初始TTL: {initial_ttl} 秒")
    
    # 关闭数据库
    db.close()
    
    # 等待几秒
    wait_time = 5
    print(f"\n等待 {wait_time} 秒...")
    time.sleep(wait_time)
    
    # 第二次运行：重新打开数据库并检查TTL
    print("\n第二次运行: 重新打开数据库并检查TTL")
    db = FlaxKV("test_db", DB_PATH)
    
    # 检查键值对和TTL
    current_time = datetime.datetime.now()
    new_ttl = db.get_ttl("key1")
    print(f"当前时间: {current_time}")
    print(f"键值: {db.get('key1')}")
    print(f"当前TTL: {new_ttl} 秒")
    
    # 验证TTL是否减少了大约等待的时间
    ttl_difference = abs((initial_ttl - new_ttl) - wait_time)
    print(f"TTL减少了: {initial_ttl - new_ttl} 秒 (等待了 {wait_time} 秒)")
    
    if ttl_difference < 1.0:
        print("\n✅ 测试通过: TTL持久化正常工作！")
        print(f"TTL减少量与等待时间的差异: {ttl_difference:.2f} 秒 (允许误差范围内)")
    else:
        print("\n❌ 测试失败: TTL没有正确持久化")
        print(f"TTL减少量与等待时间的差异: {ttl_difference:.2f} 秒 (超出允许误差范围)")
    
    # 测试自动过期功能
    print("\n测试键值自动过期...")
    
    # 设置一个短的TTL
    db["temp_key"] = "temp_value"
    db.set_ttl("temp_key", 2)  # 2秒后过期
    print(f"设置temp_key的TTL为2秒，当前TTL: {db.get_ttl('temp_key')}")
    
    # 关闭数据库
    db.close()
    
    # 等待足够的时间让键过期
    print("等待3秒...")
    time.sleep(3)
    
    # 重新打开数据库并检查键是否过期
    db = FlaxKV("test_db", DB_PATH)
    
    key_exists = "temp_key" in db
    print(f"temp_key是否存在: {key_exists}")
    
    if not key_exists:
        print("\n✅ 测试通过: 键在TTL到期后自动过期！")
    else:
        print("\n❌ 测试失败: 键在TTL到期后仍然存在")
        print(f"当前TTL: {db.get_ttl('temp_key')}")
    
    # 清理
    db.close()

if __name__ == "__main__":
    try:
        test_ttl_persistence()
    except Exception as e:
        print(f"测试过程中发生错误: {e}")
        sys.exit(1) 