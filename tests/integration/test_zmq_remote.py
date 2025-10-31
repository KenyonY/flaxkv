"""
ZeroMQ 基本功能测试
"""

from flaxkv2.server.zmq_server import FlaxKVServer
from flaxkv2.client.zmq_client import RemoteDBDict
import threading
import time
import sys

def main():
    # 创建服务器
    print("启动服务器...")
    server = FlaxKVServer(
        host='127.0.0.1',
        port=5556,
        data_dir='./test_zmq_data',
        max_workers=2
    )
    
    # 在后台线程启动服务器
    def run_server():
        try:
            server.start(register_signals=False)  # 不在后台线程注册信号
            server._server_loop()  # 运行服务器循环
        except Exception as e:
            print(f'服务器错误: {e}')
            import traceback
            traceback.print_exc()
    
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    
    # 等待服务器启动
    time.sleep(2)
    
    if not server.running:
        print("✗ 服务器启动失败")
        return False
    
    print('✓ 服务器已启动在 127.0.0.1:5556')
    print()
    print('=' * 60)
    print('开始客户端测试...')
    print('=' * 60)
    
    success = True
    
    try:
        # 1. 连接测试
        print('\n1. 连接到远程数据库...')
        db = RemoteDBDict('test_db', host='127.0.0.1', port=5556, timeout=5000)
        print('   ✓ 连接成功')
        
        # 2. Ping 测试
        print('\n2. 测试 ping...')
        if db.ping():
            print('   ✓ Ping 成功')
        else:
            print('   ✗ Ping 失败')
            success = False
        
        # 3. 基本写入测试
        print('\n3. 基本写入测试...')
        db['name'] = 'Alice'
        db['age'] = 30
        db['city'] = 'New York'
        print('   ✓ 写入 3 个键值对')
        
        # 4. 基本读取测试
        print('\n4. 基本读取测试...')
        name = db['name']
        age = db['age']
        city = db['city']
        print(f'   name = {name}')
        print(f'   age = {age}')
        print(f'   city = {city}')
        
        if name != 'Alice' or age != 30 or city != 'New York':
            print('   ✗ 读取值不匹配')
            success = False
        else:
            print('   ✓ 读取成功')
        
        # 5. 包含测试
        print('\n5. 包含测试...')
        if 'name' in db and 'nonexistent' not in db:
            print('   ✓ 包含测试通过')
        else:
            print('   ✗ 包含测试失败')
            success = False
        
        # 6. 删除测试
        print('\n6. 删除测试...')
        db['temp'] = 'value'
        if 'temp' not in db:
            print('   ✗ 写入后键不存在')
            success = False
        else:
            del db['temp']
            if 'temp' in db:
                print('   ✗ 删除后键仍存在')
                success = False
            else:
                print('   ✓ 删除测试通过')
        
        # 7. 批量操作测试
        print('\n7. 批量操作测试...')
        data = {f'key{i}': f'value{i}' for i in range(10)}
        db.update(data)
        print('   ✓ 批量写入 10 个键值对')
        
        # 8. keys/values/items 测试
        print('\n8. keys/values/items 测试...')
        keys = db.keys()
        print(f'   keys 数量: {len(keys)}')
        values = db.values()
        print(f'   values 数量: {len(values)}')
        items = db.items()
        print(f'   items 数量: {len(items)}')
        print('   ✓ 列表操作成功')
        
        # 9. len 测试
        print('\n9. len 测试...')
        length = len(db)
        print(f'   数据库大小: {length}')
        if length > 0:
            print('   ✓ len 测试通过')
        else:
            print('   ✗ len 返回 0')
            success = False
        
        # 10. 复杂数据类型测试
        print('\n10. 复杂数据类型测试...')
        db['dict_data'] = {'nested': {'key': 'value'}}
        db['list_data'] = [1, 2, 3, [4, 5, 6]]
        
        dict_data = db['dict_data']
        list_data = db['list_data']
        
        if dict_data == {'nested': {'key': 'value'}} and list_data == [1, 2, 3, [4, 5, 6]]:
            print('   ✓ 字典和列表测试通过')
        else:
            print(f'   ✗ 数据不匹配: dict={dict_data}, list={list_data}')
            success = False
        
        # 11. 关闭连接
        print('\n11. 关闭连接...')
        db.close()
        print('   ✓ 连接已关闭')
        
        if success:
            print('\n' + '=' * 60)
            print('✓ 所有测试通过！')
            print('=' * 60)
        else:
            print('\n' + '=' * 60)
            print('✗ 部分测试失败')
            print('=' * 60)
        
    except Exception as e:
        print(f'\n✗ 测试失败: {e}')
        import traceback
        traceback.print_exc()
        success = False
    
    finally:
        # 停止服务器
        print('\n停止服务器...')
        server.stop()
        time.sleep(0.5)
        print('✓ 服务器已停止')
    
    return success

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)

