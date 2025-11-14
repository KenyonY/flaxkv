"""
Inspector 模块测试
"""

import pytest
import tempfile
import shutil
from pathlib import Path

from flaxkv2 import FlaxKV
from flaxkv2.inspector import Inspector


@pytest.fixture
def temp_db():
    """创建临时测试数据库"""
    temp_dir = tempfile.mkdtemp()
    db_name = "test_inspector_db"
    db_path = temp_dir

    # 创建数据库并填充测试数据
    db = FlaxKV(db_name, db_path, backend='local')

    # 添加各种类型的测试数据
    db['string_key'] = 'Hello World'
    db['int_key'] = 42
    db['float_key'] = 3.14
    db['list_key'] = [1, 2, 3, 4, 5]
    db['dict_key'] = {'name': 'test', 'value': 100}
    db['bool_key'] = True

    # 添加带 TTL 的数据
    db.set('ttl_key', 'expires soon', ttl=3600)

    db.close()

    yield db_name, db_path

    # 清理
    shutil.rmtree(temp_dir)


class TestInspector:
    """Inspector 核心功能测试"""

    def test_count_keys(self, temp_db):
        """测试键计数"""
        db_name, db_path = temp_db

        with Inspector(db_name, db_path) as inspector:
            count = inspector.count_keys()
            assert count == 7  # 我们添加了 7 个键

    def test_list_keys(self, temp_db):
        """测试列出键"""
        db_name, db_path = temp_db

        with Inspector(db_name, db_path) as inspector:
            keys = inspector.list_keys(limit=10)
            assert len(keys) == 7
            assert 'string_key' in keys
            assert 'int_key' in keys

    def test_list_keys_with_pattern(self, temp_db):
        """测试带模式的键列表"""
        db_name, db_path = temp_db

        with Inspector(db_name, db_path) as inspector:
            # 搜索以 'string' 开头的键
            keys = inspector.list_keys(pattern='^string.*')
            assert len(keys) == 1
            assert keys[0] == 'string_key'

    def test_list_keys_pagination(self, temp_db):
        """测试分页"""
        db_name, db_path = temp_db

        with Inspector(db_name, db_path) as inspector:
            # 第一页
            keys_page1 = inspector.list_keys(limit=3, offset=0)
            assert len(keys_page1) == 3

            # 第二页
            keys_page2 = inspector.list_keys(limit=3, offset=3)
            assert len(keys_page2) == 3

            # 确保两页的键不重复
            assert not set(keys_page1) & set(keys_page2)

    def test_get_value_info(self, temp_db):
        """测试获取键详情"""
        db_name, db_path = temp_db

        with Inspector(db_name, db_path) as inspector:
            # 测试字符串类型
            info = inspector.get_value_info('string_key')
            assert info is not None
            assert info['key'] == 'string_key'
            assert info['type'] == 'string'
            assert info['value'] == 'Hello World'

            # 测试整数类型
            info = inspector.get_value_info('int_key')
            assert info['type'] == 'integer'
            assert info['value'] == 42

            # 测试列表类型
            info = inspector.get_value_info('list_key')
            assert info['type'] == 'list'
            assert info['value'] == [1, 2, 3, 4, 5]

            # 测试字典类型
            info = inspector.get_value_info('dict_key')
            assert info['type'] == 'dict'
            assert info['value'] == {'name': 'test', 'value': 100}

    def test_get_value_info_nonexistent(self, temp_db):
        """测试获取不存在的键"""
        db_name, db_path = temp_db

        with Inspector(db_name, db_path) as inspector:
            info = inspector.get_value_info('nonexistent_key')
            assert info is None

    def test_get_stats(self, temp_db):
        """测试统计信息"""
        db_name, db_path = temp_db

        with Inspector(db_name, db_path) as inspector:
            stats = inspector.get_stats()

            assert stats['total_keys'] == 7
            assert 'type_distribution' in stats
            assert 'size_distribution' in stats
            assert 'ttl_status' in stats
            assert stats['total_size'] > 0

            # 检查类型分布
            assert stats['type_distribution']['string'] >= 1
            assert stats['type_distribution']['integer'] >= 1
            assert stats['type_distribution']['list'] >= 1

    def test_search_keys(self, temp_db):
        """测试搜索键"""
        db_name, db_path = temp_db

        with Inspector(db_name, db_path) as inspector:
            # 搜索包含 'key' 的键
            results = inspector.search_keys('.*_key$')
            assert len(results) > 0

            # 搜索特定模式
            results = inspector.search_keys('^int.*')
            assert len(results) == 1
            assert results[0][0] == 'int_key'

    def test_delete_key(self, temp_db):
        """测试删除键"""
        db_name, db_path = temp_db

        with Inspector(db_name, db_path) as inspector:
            # 确认键存在
            assert inspector.get_value_info('string_key') is not None

            # 删除键
            result = inspector.delete_key('string_key')
            assert result is True

            # 确认键已删除
            assert inspector.get_value_info('string_key') is None

            # 删除不存在的键
            result = inspector.delete_key('nonexistent_key')
            assert result is False

    def test_set_value(self, temp_db):
        """测试设置键值"""
        db_name, db_path = temp_db

        with Inspector(db_name, db_path) as inspector:
            # 设置新键
            result = inspector.set_value('new_key', 'new_value')
            assert result is True

            # 验证设置成功
            info = inspector.get_value_info('new_key')
            assert info is not None
            assert info['value'] == 'new_value'

            # 更新已存在的键
            result = inspector.set_value('new_key', 'updated_value')
            assert result is True

            info = inspector.get_value_info('new_key')
            assert info['value'] == 'updated_value'

    def test_set_value_with_ttl(self, temp_db):
        """测试带 TTL 的设置"""
        db_name, db_path = temp_db

        with Inspector(db_name, db_path) as inspector:
            # 设置带 TTL 的键
            result = inspector.set_value('ttl_test_key', 'expires', ttl=10)
            assert result is True

            # 验证 TTL 信息（这个测试可能需要访问内部实现）
            info = inspector.get_value_info('ttl_test_key')
            assert info is not None
            assert info['value'] == 'expires'

    def test_export_data(self, temp_db):
        """测试导出数据"""
        db_name, db_path = temp_db

        with Inspector(db_name, db_path) as inspector:
            # 导出所有数据
            data = inspector.export_data()
            assert len(data) == 7
            assert data['string_key'] == 'Hello World'
            assert data['int_key'] == 42

            # 导出指定键
            data = inspector.export_data(['string_key', 'int_key'])
            assert len(data) == 2
            assert 'string_key' in data
            assert 'int_key' in data


class TestInspectorWithCache:
    """测试 Inspector 与缓存后端的集成"""

    def test_inspector_with_cached_backend(self):
        """测试 Inspector 与缓存后端"""
        temp_dir = tempfile.mkdtemp()
        db_name = "cached_test_db"

        try:
            # 创建缓存后端的数据库
            db = FlaxKV(db_name, temp_dir, backend='local', use_cache=True)
            db['test_key'] = 'test_value'
            db.close()

            # 使用 Inspector 访问
            with Inspector(db_name, temp_dir, use_cache=True) as inspector:
                info = inspector.get_value_info('test_key')
                assert info is not None
                assert info['value'] == 'test_value'

        finally:
            shutil.rmtree(temp_dir)


class TestInspectorBackendAuto:
    """测试 Inspector 的 backend='auto' 功能"""

    def test_backend_auto_with_local_path(self):
        """测试 backend='auto' 对本地路径的处理"""
        temp_dir = tempfile.mkdtemp()
        db_name = "test_auto_local"

        try:
            # 创建测试数据
            db = FlaxKV(db_name, temp_dir)
            db['test1'] = 'value1'
            db['test2'] = 'value2'
            db.close()

            # 使用 backend='auto' 连接
            with Inspector(db_name, temp_dir, backend='auto') as inspector:
                count = inspector.count_keys()
                assert count == 2

                keys = inspector.list_keys()
                assert 'test1' in keys
                assert 'test2' in keys

                info = inspector.get_value_info('test1')
                assert info is not None
                assert info['value'] == 'value1'

        finally:
            shutil.rmtree(temp_dir)

    def test_backend_auto_with_tcp_url(self):
        """测试 backend='auto' 对 tcp:// URL 的自动检测

        注意：此测试需要 ZeroMQ 服务器运行，暂时跳过
        """
        pytest.skip("需要运行的 ZeroMQ 服务器")

    def test_backend_auto_parameter_conversion(self):
        """测试 Inspector 正确将 'auto' 转换为 None"""
        temp_dir = tempfile.mkdtemp()
        db_name = "test_conversion"

        try:
            # 创建测试数据
            db = FlaxKV(db_name, temp_dir)
            db['key1'] = 'value1'
            db.close()

            # 使用 backend='auto'，Inspector 应该正确处理
            inspector = Inspector(db_name, temp_dir, backend='auto')

            # 验证 Inspector 保存了原始 backend 值
            assert inspector.backend == 'auto'

            # 验证数据库正常工作（说明 'auto' 被正确转换为 None）
            count = inspector.count_keys()
            assert count == 1

            inspector.close()

        finally:
            shutil.rmtree(temp_dir)


class TestInspectorNestedSerialization:
    """测试 Inspector 对 NestedDB 类型的 JSON 序列化"""

    def test_nested_list_json_serialization(self):
        """测试 NestedDBList 可以被 JSON 序列化"""
        import json
        temp_dir = tempfile.mkdtemp()
        db_name = "test_nested_list"

        try:
            # 创建包含嵌套列表的数据
            db = FlaxKV(db_name, temp_dir)
            db['nested_list'] = [{'id': 1, 'name': 'A'}, {'id': 2, 'name': 'B'}]
            db.close()

            # 使用 Inspector 读取
            with Inspector(db_name, temp_dir, backend='auto') as inspector:
                info = inspector.get_value_info('nested_list')

                # 验证类型
                assert info is not None
                assert info['type'] == 'list'

                # 验证可以 JSON 序列化
                json_str = json.dumps(info)
                assert json_str is not None

                # 验证可以反序列化
                parsed = json.loads(json_str)
                assert parsed['type'] == 'list'
                assert isinstance(parsed['value'], list)

        finally:
            shutil.rmtree(temp_dir)

    def test_nested_dict_json_serialization(self):
        """测试 NestedDBDict 可以被 JSON 序列化"""
        import json
        temp_dir = tempfile.mkdtemp()
        db_name = "test_nested_dict"

        try:
            # 创建包含嵌套字典的数据
            db = FlaxKV(db_name, temp_dir)
            db['nested_dict'] = {
                'config': {'timeout': 30, 'retries': 3},
                'metadata': {'version': '1.0', 'author': 'test'}
            }
            db.close()

            # 使用 Inspector 读取
            with Inspector(db_name, temp_dir, backend='auto') as inspector:
                info = inspector.get_value_info('nested_dict')

                # 验证类型
                assert info is not None
                assert info['type'] == 'dict'

                # 验证可以 JSON 序列化
                json_str = json.dumps(info)
                assert json_str is not None

                # 验证可以反序列化
                parsed = json.loads(json_str)
                assert parsed['type'] == 'dict'
                assert isinstance(parsed['value'], dict)

        finally:
            shutil.rmtree(temp_dir)

    def test_mixed_nested_data_json_serialization(self):
        """测试混合嵌套数据的 JSON 序列化"""
        import json
        temp_dir = tempfile.mkdtemp()
        db_name = "test_mixed"

        try:
            # 创建各种类型的数据
            db = FlaxKV(db_name, temp_dir)
            db['str'] = 'string'
            db['num'] = 42
            db['list'] = [1, 2, 3]
            db['dict'] = {'a': 1, 'b': 2}
            db['nested_list'] = [{'x': 1}, {'x': 2}]
            db['nested_dict'] = {'inner': {'y': 3}}
            db.close()

            # 使用 Inspector 读取所有键
            with Inspector(db_name, temp_dir, backend='auto') as inspector:
                keys = inspector.list_keys()

                # 验证每个键都可以 JSON 序列化
                for key in keys:
                    info = inspector.get_value_info(key)
                    assert info is not None

                    # 尝试 JSON 序列化
                    json_str = json.dumps(info)
                    assert json_str is not None

                    # 验证可以反序列化
                    parsed = json.loads(json_str)
                    assert parsed['key'] == key

        finally:
            shutil.rmtree(temp_dir)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
