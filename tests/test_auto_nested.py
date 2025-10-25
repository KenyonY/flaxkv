"""
测试自动嵌套功能

包含:
1. LevelDBDict 的 auto_nested 功能
2. RawLevelDBDict 的 nested() 方法
3. auto_nested=True/False 的行为差异
"""

import pytest
import tempfile
import shutil
from flaxkv2.core.leveldb_dict import LevelDBDict
from flaxkv2.core.raw_leveldb_dict import RawLevelDBDict
from flaxkv2.core.nested_dict import NestedDBDict


class TestAutoNested:
    """测试 LevelDBDict 的自动嵌套功能"""
    
    @pytest.fixture
    def db(self):
        """创建临时数据库"""
        tmpdir = tempfile.mkdtemp()
        db = LevelDBDict('test', path=tmpdir, rebuild=True, auto_nested=True)
        yield db
        db.close()
        shutil.rmtree(tmpdir)
    
    def test_auto_detect_dict(self, db):
        """测试自动检测字典并使用嵌套存储"""
        # 写入字典，自动使用嵌套存储
        db['user:1'] = {
            'name': 'Alice',
            'age': 30,
            'email': 'alice@example.com'
        }
        
        # 读取，应该返回 NestedDBDict
        user = db['user:1']
        assert isinstance(user, NestedDBDict)
        assert user['name'] == 'Alice'
        assert user['age'] == 30
        assert user['email'] == 'alice@example.com'
    
    def test_modify_nested_field(self, db):
        """测试修改嵌套字段"""
        db['user:1'] = {'name': 'Alice', 'age': 30}
        
        # 修改单个字段
        user = db['user:1']
        user['age'] = 31
        
        # 验证修改
        assert db['user:1']['age'] == 31
        assert db['user:1']['name'] == 'Alice'
    
    def test_nested_dict_operations(self, db):
        """测试嵌套字典的各种操作"""
        db['config'] = {
            'host': 'localhost',
            'port': 8080,
            'debug': True
        }
        
        config = db['config']
        
        # 测试 keys()
        keys = list(config.keys())
        assert 'host' in keys
        assert 'port' in keys
        assert 'debug' in keys
        
        # 测试 items()
        items = dict(config.items())
        assert items['host'] == 'localhost'
        assert items['port'] == 8080
        
        # 测试 in 操作
        assert 'host' in config
        assert 'nonexistent' not in config
        
        # 测试删除
        del config['debug']
        assert 'debug' not in config
    
    def test_deeply_nested_dict(self, db):
        """测试深层嵌套字典"""
        db['app'] = {
            'database': {
                'host': 'localhost',
                'port': 5432,
                'credentials': {
                    'user': 'admin',
                    'password': 'secret'
                }
            },
            'cache': {
                'redis': 'localhost:6379'
            }
        }
        
        app = db['app']
        assert isinstance(app, NestedDBDict)
        
        # 访问深层嵌套
        database = app['database']
        assert isinstance(database, NestedDBDict)
        assert database['host'] == 'localhost'
        
        credentials = database['credentials']
        assert isinstance(credentials, NestedDBDict)
        assert credentials['user'] == 'admin'
    
    def test_overwrite_dict_with_value(self, db):
        """测试用普通值覆盖字典"""
        # 先存储字典
        db['key'] = {'a': 1, 'b': 2}
        assert isinstance(db['key'], NestedDBDict)
        
        # 用普通值覆盖
        db['key'] = 'simple value'
        assert db['key'] == 'simple value'
        assert not isinstance(db['key'], NestedDBDict)
    
    def test_overwrite_value_with_dict(self, db):
        """测试用字典覆盖普通值"""
        # 先存储普通值
        db['key'] = 'simple value'
        assert db['key'] == 'simple value'
        
        # 用字典覆盖
        db['key'] = {'a': 1, 'b': 2}
        assert isinstance(db['key'], NestedDBDict)
        assert db['key']['a'] == 1


class TestAutoNestedModes:
    """测试 auto_nested 参数的不同模式"""
    
    def test_auto_nested_true(self):
        """测试 auto_nested=True 模式（默认）"""
        tmpdir = tempfile.mkdtemp()
        try:
            db = LevelDBDict("test", path=tmpdir, rebuild=True, auto_nested=True)
            
            # 写入字典 - 应该自动嵌套
            db['config'] = {
                'database': {'host': 'localhost', 'port': 5432},
                'cache': {'redis': 'localhost:6379'}
            }
            
            # 验证是否自动嵌套
            config = db['config']
            assert isinstance(config, NestedDBDict)
            
            # 可以访问嵌套字段
            database = config['database']
            assert isinstance(database, NestedDBDict)
            assert database['host'] == 'localhost'
            
            db.close()
        finally:
            shutil.rmtree(tmpdir)
    
    def test_auto_nested_false(self):
        """测试 auto_nested=False 模式"""
        tmpdir = tempfile.mkdtemp()
        try:
            db = LevelDBDict("test", path=tmpdir, rebuild=True, auto_nested=False)
            
            # 写入字典 - 不应该自动嵌套
            original_dict = {
                'database': {'host': 'localhost', 'port': 5432},
                'cache': {'redis': 'localhost:6379'}
            }
            db['config'] = original_dict
            
            # 验证不是 NestedDBDict
            config = db['config']
            assert not isinstance(config, NestedDBDict)
            assert isinstance(config, dict)
            
            # 整个字典被序列化存储
            assert config == original_dict
            
            db.close()
        finally:
            shutil.rmtree(tmpdir)
    
    def test_manual_nested_with_auto_false(self):
        """测试在 auto_nested=False 时手动使用 nested()"""
        tmpdir = tempfile.mkdtemp()
        try:
            db = LevelDBDict("test", path=tmpdir, rebuild=True, auto_nested=False)
            
            # 手动创建嵌套字典
            user = db.nested('user:1')
            user['name'] = 'Alice'
            user['age'] = 30
            
            # 验证可以访问
            assert user['name'] == 'Alice'
            assert user['age'] == 30
            
            # 通过 db.nested() 再次访问
            user2 = db.nested('user:1')
            assert user2['name'] == 'Alice'
            
            db.close()
        finally:
            shutil.rmtree(tmpdir)


class TestRawNestedDict:
    """测试 RawLevelDBDict 的 nested() 功能"""
    
    @pytest.fixture
    def db(self):
        """创建临时数据库"""
        tmpdir = tempfile.mkdtemp()
        db = RawLevelDBDict('test', path=tmpdir, rebuild=True)
        yield db
        db.close()
        shutil.rmtree(tmpdir)
    
    def test_create_nested(self, db):
        """测试创建嵌套字典"""
        user = db.nested('user:1')
        assert isinstance(user, NestedDBDict)
    
    def test_nested_read_write(self, db):
        """测试嵌套字典的读写"""
        user = db.nested('user:1')
        
        # 写入数据
        user['name'] = 'Alice'
        user['age'] = 30
        user['email'] = 'alice@example.com'
        
        # 读取数据
        assert user['name'] == 'Alice'
        assert user['age'] == 30
        assert user['email'] == 'alice@example.com'
    
    def test_nested_modify(self, db):
        """测试修改嵌套字典"""
        user = db.nested('user:1')
        user['age'] = 30
        
        # 修改
        user['age'] = 31
        assert user['age'] == 31
    
    def test_nested_iteration(self, db):
        """测试迭代嵌套字典"""
        user = db.nested('user:1')
        user['name'] = 'Alice'
        user['age'] = 30
        user['email'] = 'alice@example.com'
        
        # 测试 keys()
        keys = list(user.keys())
        assert 'name' in keys
        assert 'age' in keys
        assert 'email' in keys
        
        # 测试 items()
        items = dict(user.items())
        assert items['name'] == 'Alice'
        assert items['age'] == 30
    
    def test_nested_persistence(self, db):
        """测试嵌套字典的持久化"""
        # 写入数据
        user = db.nested('user:1')
        user['name'] = 'Alice'
        user['age'] = 30
        
        # 关闭并重新打开
        db_path = db.db_path
        db.close()
        
        db2 = RawLevelDBDict('test', path=db_path.rsplit('/', 1)[0])
        user2 = db2.nested('user:1')
        
        # 验证数据仍然存在
        assert user2['name'] == 'Alice'
        assert user2['age'] == 30
        
        db2.close()
    
    def test_nested_delete(self, db):
        """测试删除嵌套字段"""
        user = db.nested('user:1')
        user['name'] = 'Alice'
        user['age'] = 30
        
        # 删除字段
        del user['age']
        assert 'age' not in user
        assert 'name' in user
    
    def test_nested_clear(self, db):
        """测试清空嵌套字典"""
        user = db.nested('user:1')
        user['name'] = 'Alice'
        user['age'] = 30
        user['email'] = 'alice@example.com'
        
        # 清空
        user.clear()
        assert len(user) == 0
        assert 'name' not in user


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

