"""
远程数据库写缓冲功能测试
"""

import pytest
import time
import threading
import shutil
import os
import numpy as np
from flaxkv2.server.zmq_server import FlaxKVServer
from flaxkv2.client.zmq_client import RemoteDBDict


class TestRemoteWriteBuffer:
    """远程数据库写缓冲功能集成测试"""

    @pytest.fixture(scope='class')
    def server_setup(self):
        """启动测试服务器"""
        test_dir = './test_remote_write_buffer_data'

        # 清理旧数据
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)
        os.makedirs(test_dir, exist_ok=True)

        # 创建服务器
        server = FlaxKVServer(
            host='127.0.0.1',
            port=5557,  # 使用不同的端口避免冲突
            data_dir=test_dir,
            max_workers=2
        )

        # 在后台线程启动服务器
        def run_server():
            try:
                server.start(register_signals=False)
                server._server_loop()
            except Exception as e:
                print(f'服务器错误: {e}')

        server_thread = threading.Thread(target=run_server, daemon=True)
        server_thread.start()

        # 等待服务器启动
        time.sleep(2)

        if not server.running:
            raise RuntimeError("服务器启动失败")

        yield {'server': server, 'host': '127.0.0.1', 'port': 5557}

        # 停止服务器
        server.stop()
        time.sleep(1)

        # 清理数据
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)

    def test_write_buffer_disabled_by_default(self, server_setup):
        """测试远程数据库写缓冲默认禁用"""
        db = RemoteDBDict(
            'test_db',
            host=server_setup['host'],
            port=server_setup['port']
        )

        assert not db._write_buffer_enabled
        assert db._write_buffer is None

        db.close()

    def test_write_buffer_enabled(self, server_setup):
        """测试远程数据库写缓冲启用"""
        db = RemoteDBDict(
            'test_db',
            host=server_setup['host'],
            port=server_setup['port'],
            enable_write_buffer=True,
            write_buffer_size=10
        )

        assert db._write_buffer_enabled
        assert db._write_buffer is not None
        assert db._write_buffer._max_size == 10

        db.close()

    def test_basic_write_read_with_buffer(self, server_setup):
        """测试基本的写入和读取（带缓冲）"""
        db = RemoteDBDict(
            'test_db_basic',
            host=server_setup['host'],
            port=server_setup['port'],
            enable_write_buffer=True,
            write_buffer_size=10
        )

        # 写入数据
        db['key1'] = 'value1'
        db['key2'] = 'value2'
        db['key3'] = 'value3'

        # 立即读取（应该从缓冲区读取）
        assert db['key1'] == 'value1'
        assert db['key2'] == 'value2'
        assert db['key3'] == 'value3'

        db.close()

    def test_persistence_after_close(self, server_setup):
        """测试关闭后数据持久化"""
        # 写入数据
        db = RemoteDBDict(
            'test_db_persist',
            host=server_setup['host'],
            port=server_setup['port'],
            enable_write_buffer=True,
            write_buffer_size=100
        )

        db['key1'] = 'value1'
        db['key2'] = 'value2'
        db.close()

        # 重新连接，检查数据
        time.sleep(0.5)  # 等待服务器处理

        db2 = RemoteDBDict(
            'test_db_persist',
            host=server_setup['host'],
            port=server_setup['port'],
            enable_write_buffer=False
        )

        assert db2['key1'] == 'value1'
        assert db2['key2'] == 'value2'

        db2.close()

    def test_auto_flush_on_size_threshold(self, server_setup):
        """测试达到大小阈值时自动刷新"""
        db = RemoteDBDict(
            'test_db_autoflush',
            host=server_setup['host'],
            port=server_setup['port'],
            enable_write_buffer=True,
            write_buffer_size=5  # 小阈值
        )

        # 写入6条数据，应该触发至少一次刷新
        for i in range(6):
            db[f'key_{i}'] = f'value_{i}'

        # 等待刷新
        time.sleep(0.5)

        # 验证数据可读
        for i in range(6):
            assert db[f'key_{i}'] == f'value_{i}'

        db.close()

    def test_manual_flush(self, server_setup):
        """测试手动刷新"""
        db = RemoteDBDict(
            'test_db_manual',
            host=server_setup['host'],
            port=server_setup['port'],
            enable_write_buffer=True,
            write_buffer_size=100
        )

        # 写入数据
        db['key1'] = 'value1'
        db['key2'] = 'value2'

        # 手动刷新
        db.flush()
        time.sleep(0.5)  # 等待服务器处理

        # 关闭后重新连接，数据应该存在
        db.close()

        db2 = RemoteDBDict(
            'test_db_manual',
            host=server_setup['host'],
            port=server_setup['port'],
            enable_write_buffer=False
        )

        assert db2['key1'] == 'value1'
        assert db2['key2'] == 'value2'

        db2.close()

    def test_delete_with_buffer(self, server_setup):
        """测试删除操作（带缓冲）"""
        db = RemoteDBDict(
            'test_db_delete',
            host=server_setup['host'],
            port=server_setup['port'],
            enable_write_buffer=True,
            write_buffer_size=10
        )

        # 写入和删除
        db['key1'] = 'value1'
        db['key2'] = 'value2'
        db.flush()  # 先刷新写入
        time.sleep(0.5)

        del db['key1']

        # 检查删除
        with pytest.raises(KeyError):
            _ = db['key1']
        assert db['key2'] == 'value2'

        db.close()

    def test_numpy_array_with_buffer(self, server_setup):
        """测试 numpy 数组（带缓冲）"""
        db = RemoteDBDict(
            'test_db_numpy',
            host=server_setup['host'],
            port=server_setup['port'],
            enable_write_buffer=True,
            write_buffer_size=10
        )

        # 写入 numpy 数组
        arr1 = np.array([1, 2, 3, 4, 5])
        arr2 = np.random.rand(10, 10)

        db['arr1'] = arr1
        db['arr2'] = arr2

        # 读取并验证
        assert np.array_equal(db['arr1'], arr1)
        assert np.array_equal(db['arr2'], arr2)

        db.close()

    def test_ttl_with_buffer(self, server_setup):
        """测试 TTL（带缓冲）"""
        db = RemoteDBDict(
            'test_db_ttl',
            host=server_setup['host'],
            port=server_setup['port'],
            enable_write_buffer=True,
            write_buffer_size=10
        )

        # 写入带TTL的数据
        db.set('key1', 'value1', ttl=2)  # 2秒过期

        # 立即读取应该成功
        assert db['key1'] == 'value1'

        # 刷新并等待过期
        db.flush()
        time.sleep(2.5)

        # 读取应该失败
        with pytest.raises(KeyError):
            _ = db['key1']

        db.close()

    def test_buffer_with_read_cache(self, server_setup):
        """测试写缓冲与读缓存的交互"""
        db = RemoteDBDict(
            'test_db_cache',
            host=server_setup['host'],
            port=server_setup['port'],
            enable_write_buffer=True,
            write_buffer_size=10,
            read_cache_size=100  # 同时启用读缓存
        )

        # 写入数据
        db['key1'] = 'value1'

        # 读取（应该从写缓冲区读取）
        assert db['key1'] == 'value1'

        # 刷新
        db.flush()
        time.sleep(0.5)

        # 再次读取（应该从服务器或读缓存读取）
        assert db['key1'] == 'value1'

        db.close()

    def test_large_batch_write(self, server_setup):
        """测试大批量写入"""
        db = RemoteDBDict(
            'test_db_batch',
            host=server_setup['host'],
            port=server_setup['port'],
            enable_write_buffer=True,
            write_buffer_size=100
        )

        # 写入500条数据
        n = 500
        for i in range(n):
            db[f'key_{i}'] = f'value_{i}'

        # 验证所有数据
        for i in range(n):
            assert db[f'key_{i}'] == f'value_{i}'

        db.close()
        time.sleep(1)

        # 重新连接验证持久化
        db2 = RemoteDBDict(
            'test_db_batch',
            host=server_setup['host'],
            port=server_setup['port']
        )

        for i in range(n):
            assert db2[f'key_{i}'] == f'value_{i}'

        db2.close()

    def test_concurrent_clients_with_buffer(self, server_setup):
        """测试多个客户端并发写入（带缓冲）"""
        def writer(client_id, start, end):
            db = RemoteDBDict(
                f'test_db_concurrent_{client_id}',
                host=server_setup['host'],
                port=server_setup['port'],
                enable_write_buffer=True,
                write_buffer_size=50
            )

            for i in range(start, end):
                db[f'key_{i}'] = f'value_{i}_client_{client_id}'

            db.close()

        # 启动多个客户端
        threads = []
        for i in range(3):
            t = threading.Thread(target=writer, args=(i, i*100, (i+1)*100))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        time.sleep(1)

        # 验证每个客户端的数据
        for client_id in range(3):
            db = RemoteDBDict(
                f'test_db_concurrent_{client_id}',
                host=server_setup['host'],
                port=server_setup['port']
            )

            for i in range(client_id*100, (client_id+1)*100):
                assert db[f'key_{i}'] == f'value_{i}_client_{client_id}'

            db.close()


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
