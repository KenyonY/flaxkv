#!/usr/bin/env python
"""
FlaxKV默认TTL功能演示
"""

from flaxkv2 import FlaxKV
import time
import os
import shutil

# 设置演示数据库路径
DB_PATH = "./demo_default_ttl_data"

def clear_demo_db():
    """清除演示数据库目录"""
    if os.path.exists(DB_PATH):
        shutil.rmtree(DB_PATH)
    os.makedirs(DB_PATH, exist_ok=True)

def print_separator():
    """打印分隔线"""
    print("\n" + "-" * 50)

def main():
    """默认TTL功能演示"""
    # 清除演示目录
    clear_demo_db()
    
    print("FlaxKV默认TTL功能演示")
    print("=====================")
    print("这个演示展示了如何使用FlaxKV的默认TTL功能")
    
    # 创建带有默认TTL的数据库
    print_separator()
    print("1. 创建带有默认TTL的数据库")
    default_ttl = 20  # 20秒
    db = FlaxKV("demo_db", DB_PATH, default_ttl=default_ttl)
    print(f"创建数据库，设置默认TTL为{default_ttl}秒")
    
    # 添加键值对
    print_separator()
    print("2. 添加键值对并查看它们的TTL")
    keys = ["key1", "key2", "key3"]
    for i, key in enumerate(keys):
        db[key] = f"value-{i+1}"
        ttl = db.get_ttl(key)
        print(f"添加: {key} = value-{i+1}, TTL = {ttl}秒")
    
    # 更新默认TTL
    print_separator()
    print("3. 更新默认TTL")
    new_default_ttl = 10  # 10秒
    db.set_default_ttl(new_default_ttl)
    print(f"更新默认TTL为{new_default_ttl}秒")
    
    # 添加新键并查看TTL
    new_key = "key4"
    db[new_key] = "value-4"
    ttl = db.get_ttl(new_key)
    print(f"添加: {new_key} = value-4, TTL = {ttl}秒")
    
    # 重新查看原有键的TTL
    for key in keys:
        ttl = db.get_ttl(key)
        print(f"原有键 {key} 的TTL = {ttl}秒 (未受影响)")
    
    # 禁用默认TTL
    print_separator()
    print("4. 禁用默认TTL")
    db.set_default_ttl(None)
    print("禁用默认TTL")
    
    # 添加新键并查看TTL
    no_ttl_key = "key5"
    db[no_ttl_key] = "value-5"
    ttl = db.get_ttl(no_ttl_key)
    print(f"添加: {no_ttl_key} = value-5, TTL = {ttl}")
    
    # 覆盖默认TTL
    print_separator()
    print("5. 对特定键设置TTL覆盖默认值")
    
    # 重新启用默认TTL
    db.set_default_ttl(30)
    print("重新启用默认TTL = 30秒")
    
    # 添加键值对
    override_key = "key6"
    db[override_key] = "value-6"
    default_ttl_value = db.get_ttl(override_key)
    print(f"添加: {override_key} = value-6, 默认TTL = {default_ttl_value}秒")
    
    # 为特定键覆盖TTL
    custom_ttl = 5
    db.set_ttl(override_key, custom_ttl)
    new_ttl = db.get_ttl(override_key)
    print(f"覆盖: {override_key} 的TTL为{custom_ttl}秒, 新TTL = {new_ttl}秒")
    
    # 等待键过期
    print_separator()
    print("6. 等待键过期")
    
    # 等待特定键过期
    print(f"等待 {custom_ttl + 1} 秒，让 {override_key} 过期...")
    time.sleep(custom_ttl + 1)
    
    # 检查键是否过期
    key_exists = override_key in db
    print(f"键 {override_key} 是否仍然存在: {key_exists}")
    
    if not key_exists:
        print(f"键 {override_key} 已过期")
    else:
        print(f"键 {override_key} 未过期")
    
    # 批量操作
    print_separator()
    print("7. 批量操作也支持默认TTL")
    
    # 使用update方法批量添加键值对
    batch_data = {
        "batch_key1": "batch_value1",
        "batch_key2": "batch_value2"
    }
    
    db.update(batch_data)
    print(f"使用db.update()添加了{len(batch_data)}个键值对")
    
    # 检查批量添加的键的TTL
    for key in batch_data.keys():
        ttl = db.get_ttl(key)
        print(f"批量添加的键 {key} 的TTL = {ttl}秒")
    
    # 完成演示
    print_separator()
    print("演示完成!")
    
    # 关闭数据库
    db.close()

if __name__ == "__main__":
    main() 