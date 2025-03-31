#!/usr/bin/env python
"""
FlaxKV 默认TTL功能测试
"""

from flaxkv2 import FlaxKV
import time
import os
import shutil
import datetime

# 测试数据库路径
DB_PATH = "./test_default_ttl_data"

def clear_test_db():
    """清除测试数据库目录"""
    if os.path.exists(DB_PATH):
        shutil.rmtree(DB_PATH)
    os.makedirs(DB_PATH, exist_ok=True)

def print_header(text):
    """打印带有分隔线的标题"""
    print("\n" + "=" * 60)
    print(text)
    print("=" * 60)

def test_default_ttl():
    """测试默认TTL功能"""
    print_header("FlaxKV 默认TTL功能测试")
    
    # 清除测试目录
    clear_test_db()
    
    # 测试1: 创建带有默认TTL的数据库
    print_header("测试1: 创建带有默认TTL的数据库")
    
    # 创建带有默认TTL=5秒的数据库
    default_ttl = 5
    db = FlaxKV("test_db", DB_PATH, default_ttl=default_ttl)
    
    # 检查默认TTL
    current_default_ttl = db.get_default_ttl()
    print(f"设置的默认TTL: {default_ttl}秒")
    print(f"获取的默认TTL: {current_default_ttl}秒")
    
    if current_default_ttl == default_ttl:
        print("✅ 默认TTL设置成功")
    else:
        print("❌ 默认TTL设置失败")
    
    # 添加几个键值对
    test_keys = ["key1", "key2", "key3"]
    for i, key in enumerate(test_keys):
        db[key] = f"value-{i+1}"
        print(f"添加键值对: {key} = value-{i+1}")
    
    # 检查所有键的TTL
    print("\n检查所有键的TTL:")
    for key in test_keys:
        ttl = db.get_ttl(key)
        print(f"键 {key} 的TTL: {ttl}秒")
        
        if ttl is not None and ttl <= default_ttl:
            print(f"✅ 键 {key} 自动应用了默认TTL")
        else:
            print(f"❌ 键 {key} 未应用默认TTL")
    
    # 测试2: 更新默认TTL
    print_header("测试2: 更新默认TTL")
    
    # 更新默认TTL
    new_default_ttl = 10
    db.set_default_ttl(new_default_ttl)
    print(f"更新默认TTL为: {new_default_ttl}秒")
    
    # 验证更新
    current_default_ttl = db.get_default_ttl()
    print(f"获取的默认TTL: {current_default_ttl}秒")
    
    if current_default_ttl == new_default_ttl:
        print("✅ 默认TTL更新成功")
    else:
        print("❌ 默认TTL更新失败")
    
    # 添加新键值对
    new_key = "key4"
    db[new_key] = "value-4"
    print(f"添加新键值对: {new_key} = value-4")
    
    # 检查新键的TTL
    ttl = db.get_ttl(new_key)
    print(f"新键 {new_key} 的TTL: {ttl}秒")
    
    if ttl is not None and ttl <= new_default_ttl:
        print(f"✅ 新键 {new_key} 应用了新的默认TTL")
    else:
        print(f"❌ 新键 {new_key} 未应用新的默认TTL")
    
    # 测试3: 禁用默认TTL
    print_header("测试3: 禁用默认TTL")
    
    # 禁用默认TTL
    db.set_default_ttl(None)
    print("禁用默认TTL")
    
    # 验证禁用
    current_default_ttl = db.get_default_ttl()
    print(f"获取的默认TTL: {current_default_ttl}")
    
    if current_default_ttl is None:
        print("✅ 默认TTL禁用成功")
    else:
        print("❌ 默认TTL禁用失败")
    
    # 添加新键值对
    new_key2 = "key5"
    db[new_key2] = "value-5"
    print(f"添加新键值对: {new_key2} = value-5")
    
    # 检查新键的TTL
    ttl = db.get_ttl(new_key2)
    print(f"新键 {new_key2} 的TTL: {ttl}")
    
    if ttl is None:
        print(f"✅ 新键 {new_key2} 没有应用TTL，符合预期")
    else:
        print(f"❌ 新键 {new_key2} 不应该有TTL，但却有TTL: {ttl}秒")
    
    # 测试4: 自动过期
    print_header("测试4: 自动过期测试")
    
    # 设置短TTL
    short_ttl = 2
    db.set_default_ttl(short_ttl)
    print(f"设置默认TTL为: {short_ttl}秒")
    
    # 添加测试键
    expire_key = "expire_key"
    db[expire_key] = "will-expire-soon"
    print(f"添加键值对: {expire_key} = will-expire-soon")
    
    # 检查TTL
    ttl = db.get_ttl(expire_key)
    print(f"键 {expire_key} 的TTL: {ttl}秒")
    
    # 等待过期
    wait_time = short_ttl + 0.5
    print(f"等待 {wait_time} 秒让键过期...")
    time.sleep(wait_time)
    
    # 检查键是否过期
    key_exists = expire_key in db
    print(f"键 {expire_key} 是否存在: {key_exists}")
    
    if not key_exists:
        print(f"✅ 键 {expire_key} 已自动过期，符合预期")
    else:
        print(f"❌ 键 {expire_key} 应该已过期，但仍然存在")
    
    # 关闭数据库
    db.close()
    print("\n测试完成!")

if __name__ == "__main__":
    test_default_ttl() 