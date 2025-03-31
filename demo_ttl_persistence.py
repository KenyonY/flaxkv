#!/usr/bin/env python
"""
FlaxKV TTL持久化功能演示
"""

from flaxkv2 import FlaxKV
import time
import datetime
import os
import sys

def print_header(text):
    """打印带有分隔线的标题"""
    print("\n" + "=" * 50)
    print(text)
    print("=" * 50)

def main():
    """演示TTL持久化功能"""
    # 设置数据库路径
    db_path = "./ttl_demo_data"
    os.makedirs(db_path, exist_ok=True)
    
    print_header("FlaxKV TTL持久化功能演示")
    print("该演示展示了FlaxKV如何在程序重启后继续保持TTL计时")
    
    # 第一部分：设置键和TTL
    print_header("第一部分：设置键和TTL")
    
    # 创建/打开数据库
    db = FlaxKV("demo_db", db_path)
    
    # 设置演示键值对
    demo_key = "demo_key"
    db[demo_key] = "这是一个带有TTL的值"
    
    # 设置TTL为30秒
    ttl_seconds = 30
    db.set_ttl(demo_key, ttl_seconds)
    
    # 显示当前信息
    current_time = datetime.datetime.now()
    current_ttl = db.get_ttl(demo_key)
    print(f"当前时间: {current_time}")
    print(f"键: {demo_key}")
    print(f"值: {db[demo_key]}")
    print(f"设置的TTL: {ttl_seconds}秒")
    print(f"当前TTL: {current_ttl}秒")
    
    # 关闭数据库（模拟程序终止）
    db.close()
    print("\n数据库已关闭，模拟程序终止...")
    
    # 第二部分：等待一段时间
    wait_time = 5
    print_header(f"第二部分：等待{wait_time}秒")
    print(f"请等待{wait_time}秒...")
    time.sleep(wait_time)
    
    # 第三部分：重新打开数据库
    print_header("第三部分：重新打开数据库")
    print("现在重新打开数据库，模拟程序重启...")
    
    # 重新打开数据库
    db = FlaxKV("demo_db", db_path)
    
    # 检查TTL
    current_time = datetime.datetime.now()
    new_ttl = db.get_ttl(demo_key)
    print(f"当前时间: {current_time}")
    print(f"键: {demo_key}")
    print(f"值: {db[demo_key]}")
    print(f"当前TTL: {new_ttl}秒")
    
    # 计算TTL减少了多少
    ttl_difference = ttl_seconds - new_ttl
    print(f"\nTTL减少了约 {ttl_difference:.2f}秒，而等待时间为{wait_time}秒")
    
    if abs(ttl_difference - wait_time) < 1.0:
        print("\n✅ 成功！TTL持久化正常工作")
        print("TTL计时器在程序重启后继续工作，并且减少的时间与等待时间相符")
    else:
        print("\n❌ 失败！TTL持久化未正常工作")
        print(f"TTL减少量与等待时间相差太大: {abs(ttl_difference - wait_time):.2f}秒")
    
    # 第四部分：观察剩余TTL
    print_header("第四部分：自动过期演示")
    
    # 设置一个短TTL的键
    temp_key = "temp_key"
    short_ttl = 5
    db[temp_key] = "这个键将很快过期"
    db.set_ttl(temp_key, short_ttl)
    
    print(f"创建了一个新键 '{temp_key}'，TTL为{short_ttl}秒")
    print(f"当前TTL: {db.get_ttl(temp_key)}秒")
    
    print("\n您可以选择立即退出程序，然后在5秒后重新运行来验证键是否过期")
    print("或者等待键在当前会话中过期")
    
    # 询问用户是否等待键过期
    try:
        choice = input("\n是否等待键过期？(y/n): ").strip().lower()
        if choice == 'y':
            print(f"\n等待{short_ttl + 1}秒...")
            time.sleep(short_ttl + 1)  # 多等1秒确保过期
            
            # 检查键是否存在
            key_exists = temp_key in db
            print(f"'{temp_key}'是否仍然存在: {key_exists}")
            
            if not key_exists:
                print("✅ 键已自动过期！")
            else:
                print("❌ 键未过期！")
                print(f"当前TTL: {db.get_ttl(temp_key)}秒")
    except KeyboardInterrupt:
        print("\n操作已取消")
    
    # 关闭数据库
    db.close()
    print("\n演示完成！")
    print(f"数据库位置: {os.path.abspath(db_path)}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"演示过程中发生错误: {e}")
        sys.exit(1) 