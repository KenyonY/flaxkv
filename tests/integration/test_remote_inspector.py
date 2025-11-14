"""
Inspector远程后端集成测试

测试Inspector在远程ZeroMQ后端上的完整功能
"""

import pytest
import time
import subprocess
import sys
import os
import shutil
import tempfile

from flaxkv2 import FlaxKV
from flaxkv2.inspector import Inspector


@pytest.fixture
def remote_server():
    """启动临时ZeroMQ服务器"""
    test_dir = tempfile.mkdtemp()
    port = 15600  # 使用固定端口，避免冲突

    # 启动服务器
    server_process = subprocess.Popen([
        sys.executable, '-m', 'flaxkv2', 'run',
        '--host', '127.0.0.1',
        '--port', str(port),
        '--data-dir', test_dir
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # 等待服务器启动
    time.sleep(3)

    # 检查服务器是否成功启动
    if server_process.poll() is not None:
        stdout, stderr = server_process.communicate()
        pytest.fail(f"Server failed to start:\nSTDOUT: {stdout.decode()}\nSTDERR: {stderr.decode()}")

    yield test_dir, port

    # 清理
    server_process.terminate()
    try:
        server_process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        server_process.kill()
    shutil.rmtree(test_dir)


class TestRemoteInspector:
    """Inspector远程后端测试"""

    def test_remote_with_tcp_url(self, remote_server):
        """测试使用tcp://地址连接远程Inspector"""
        test_dir, port = remote_server

        # 通过远程客户端创建数据
        db = FlaxKV('testdb', f'tcp://127.0.0.1:{port}', backend='remote')
        db['key1'] = 'value1'
        db['key2'] = {'name': 'test'}
        db['key3'] = [1, 2, 3]
        db.close()

        # 使用Inspector with backend='auto'
        with Inspector('testdb', f'tcp://127.0.0.1:{port}', backend='auto') as inspector:
            # 测试键计数
            count = inspector.count_keys()
            assert count == 3

            # 测试列出键
            keys = inspector.list_keys()
            assert set(keys) == {'key1', 'key2', 'key3'}

            # 测试获取值
            info = inspector.get_value_info('key1')
            assert info['value'] == 'value1'
            assert info['type'] == 'string'

    def test_remote_with_explicit_backend(self, remote_server):
        """测试显式指定backend='remote'"""
        test_dir, port = remote_server

        # 创建数据
        db = FlaxKV('testdb2', f'tcp://127.0.0.1:{port}', backend='remote')
        db['test'] = 'data'
        db.close()

        # 使用Inspector with explicit backend
        with Inspector('testdb2', f'127.0.0.1:{port}', backend='remote') as inspector:
            count = inspector.count_keys()
            assert count == 1

            keys = inspector.list_keys()
            assert 'test' in keys

    def test_remote_inspector_search(self, remote_server):
        """测试远程Inspector的搜索功能"""
        test_dir, port = remote_server

        # 创建测试数据
        db = FlaxKV('searchdb', f'tcp://127.0.0.1:{port}', backend='remote')
        db['user:1'] = 'alice'
        db['user:2'] = 'bob'
        db['config:debug'] = True
        db.close()

        # 搜索
        with Inspector('searchdb', f'tcp://127.0.0.1:{port}', backend='auto') as inspector:
            results = inspector.search_keys('user:.*')
            keys = [key for key, info in results]
            assert len(keys) == 2
            assert 'user:1' in keys
            assert 'user:2' in keys

    def test_remote_inspector_stats(self, remote_server):
        """测试远程Inspector的统计功能"""
        test_dir, port = remote_server

        # 创建多样化数据
        db = FlaxKV('statsdb', f'tcp://127.0.0.1:{port}', backend='remote')
        db['str_key'] = 'string'
        db['int_key'] = 42
        db['dict_key'] = {'a': 1}
        db['list_key'] = [1, 2, 3]
        db.close()

        # 获取统计
        with Inspector('statsdb', f'tcp://127.0.0.1:{port}', backend='auto') as inspector:
            stats = inspector.get_stats()
            assert stats['total_keys'] == 4
            assert 'type_distribution' in stats
            assert len(stats['type_distribution']) >= 4  # 至少4种类型

    def test_remote_inspector_modify_data(self, remote_server):
        """测试远程Inspector的数据修改功能"""
        test_dir, port = remote_server

        db = FlaxKV('modifydb', f'tcp://127.0.0.1:{port}', backend='remote')
        db['original'] = 'value'
        db.close()

        # 使用Inspector修改数据
        with Inspector('modifydb', f'tcp://127.0.0.1:{port}', backend='auto') as inspector:
            # 设置新值
            inspector.set_value('new_key', 'new_value')

            # 验证
            info = inspector.get_value_info('new_key')
            assert info['value'] == 'new_value'

            # 删除键
            result = inspector.delete_key('new_key')
            assert result == True

            # 验证删除
            info = inspector.get_value_info('new_key')
            assert info is None


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
