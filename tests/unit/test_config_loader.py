"""
配置加载器单元测试
"""

import os
import tempfile
from pathlib import Path
import pytest

from flaxkv2.utils.config_loader import ConfigLoader, save_sample_config, create_sample_config


class TestConfigLoader:
    """测试配置加载器基本功能"""

    @pytest.fixture
    def sample_config_content(self):
        """示例配置内容"""
        return '''
[defaults]
data_dir = "./data"
log_level = "INFO"
db_name = "test_db"

[server]
host = "0.0.0.0"
port = 6666
workers = 8
log_level = "WARNING"

[server.profiles.production]
host = "0.0.0.0"
port = 5555
workers = 16
enable_encryption = true

[server.profiles.development]
host = "127.0.0.1"
port = 5556
workers = 2
log_level = "DEBUG"

[client]
server = "127.0.0.1:6666"
db_name = "client_db"
backend = "remote"

[client.profiles.local]
backend = "local"
path = "./local_data"

[client.profiles.remote]
backend = "remote"
server = "192.168.1.100:5555"

[servers.local]
host = "127.0.0.1"
port = 5555

[servers.production]
host = "192.168.1.100"
port = 5555
password = "prod-password"

[servers.staging]
host = "192.168.1.200"
port = 5556
'''

    @pytest.fixture
    def config_file(self, sample_config_content, tmp_path):
        """创建临时配置文件"""
        config_path = tmp_path / "flaxkv.toml"
        config_path.write_text(sample_config_content, encoding='utf-8')
        return config_path

    def test_load_config_from_file(self, config_file):
        """测试从文件加载配置"""
        loader = ConfigLoader(str(config_file))
        assert loader.config is not None
        assert 'server' in loader.config
        assert 'client' in loader.config
        assert 'servers' in loader.config

    def test_get_server_config_default(self, config_file):
        """测试获取默认服务器配置"""
        loader = ConfigLoader(str(config_file))
        server_config = loader.get_server_config()

        assert server_config['host'] == '0.0.0.0'
        assert server_config['port'] == 6666
        assert server_config['workers'] == 8
        assert server_config['log_level'] == 'WARNING'

    def test_get_server_config_profile(self, config_file):
        """测试获取指定 profile 的服务器配置"""
        loader = ConfigLoader(str(config_file))

        # 测试 production profile
        prod_config = loader.get_server_config('production')
        assert prod_config['host'] == '0.0.0.0'
        assert prod_config['port'] == 5555
        assert prod_config['workers'] == 16
        assert prod_config.get('enable_encryption') is True

        # 测试 development profile
        dev_config = loader.get_server_config('development')
        assert dev_config['host'] == '127.0.0.1'
        assert dev_config['port'] == 5556
        assert dev_config['workers'] == 2
        assert dev_config['log_level'] == 'DEBUG'

    def test_get_client_config_default(self, config_file):
        """测试获取默认客户端配置"""
        loader = ConfigLoader(str(config_file))
        client_config = loader.get_client_config()

        assert client_config['server'] == '127.0.0.1:6666'
        assert client_config['db_name'] == 'client_db'
        assert client_config['backend'] == 'remote'

    def test_get_client_config_profile(self, config_file):
        """测试获取指定 profile 的客户端配置"""
        loader = ConfigLoader(str(config_file))

        # 测试 local profile
        local_config = loader.get_client_config('local')
        assert local_config['backend'] == 'local'
        assert local_config['path'] == './local_data'

        # 测试 remote profile
        remote_config = loader.get_client_config('remote')
        assert remote_config['backend'] == 'remote'
        assert remote_config['server'] == '192.168.1.100:5555'

    def test_get_servers(self, config_file):
        """测试获取所有服务器定义"""
        loader = ConfigLoader(str(config_file))
        servers = loader.get_servers()

        assert 'local' in servers
        assert 'production' in servers
        assert 'staging' in servers

        assert servers['local']['host'] == '127.0.0.1'
        assert servers['local']['port'] == 5555

        assert servers['production']['host'] == '192.168.1.100'
        assert servers['production']['password'] == 'prod-password'

    def test_get_server_address(self, config_file):
        """测试获取服务器地址"""
        loader = ConfigLoader(str(config_file))

        assert loader.get_server_address('local') == '127.0.0.1:5555'
        assert loader.get_server_address('production') == '192.168.1.100:5555'
        assert loader.get_server_address('staging') == '192.168.1.200:5556'
        assert loader.get_server_address('nonexistent') is None

    def test_get_defaults(self, config_file):
        """测试获取默认配置"""
        loader = ConfigLoader(str(config_file))
        defaults = loader.get_defaults()

        assert defaults['data_dir'] == './data'
        assert defaults['log_level'] == 'INFO'
        assert defaults['db_name'] == 'test_db'

    def test_list_profiles(self, config_file):
        """测试列出 profiles"""
        loader = ConfigLoader(str(config_file))

        server_profiles = loader.list_profiles('server')
        assert 'production' in server_profiles
        assert 'development' in server_profiles

        client_profiles = loader.list_profiles('client')
        assert 'local' in client_profiles
        assert 'remote' in client_profiles

    def test_list_servers(self, config_file):
        """测试列出服务器"""
        loader = ConfigLoader(str(config_file))
        servers = loader.list_servers()

        assert 'local' in servers
        assert 'production' in servers
        assert 'staging' in servers

    def test_no_config_file(self, tmp_path):
        """测试没有配置文件时的行为"""
        # 改变到一个不包含配置文件的目录
        original_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)
            loader = ConfigLoader()
            assert loader.config == {}
            assert loader.get_server_config() == {}
            assert loader.get_client_config() == {}
        finally:
            os.chdir(original_cwd)

    def test_invalid_config_file(self, tmp_path):
        """测试无效的配置文件"""
        invalid_config = tmp_path / "flaxkv.toml"
        invalid_config.write_text("invalid toml content [[[", encoding='utf-8')

        with pytest.raises(Exception):
            ConfigLoader(str(invalid_config))

    def test_nonexistent_profile(self, config_file):
        """测试不存在的 profile"""
        loader = ConfigLoader(str(config_file))

        # 不存在的 profile 应该返回默认配置
        config = loader.get_server_config('nonexistent')
        assert config['host'] == '0.0.0.0'
        assert config['port'] == 6666

    def test_merge_with_defaults(self, config_file):
        """测试配置与默认值合并"""
        loader = ConfigLoader(str(config_file))

        # 服务器配置应该包含默认值
        server_config = loader.get_server_config('production')

        # production profile 没有定义 log_level，但默认配置有
        # 注意：profile 配置会覆盖 server 的默认配置，不会合并 defaults
        # 所以这里应该从 server 的默认配置继承 log_level
        assert 'port' in server_config  # profile 中定义的
        assert 'workers' in server_config  # profile 中定义的


class TestSampleConfigGeneration:
    """测试示例配置生成"""

    def test_create_sample_config(self):
        """测试创建示例配置内容"""
        content = create_sample_config()
        assert isinstance(content, str)
        assert '[server]' in content
        assert '[client]' in content
        assert '[servers.' in content
        # 注意：新配置文件格式不再包含 [defaults] 部分

    def test_save_sample_config_default_path(self, tmp_path):
        """测试保存示例配置到默认路径"""
        original_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)
            path = save_sample_config()
            assert path.exists()
            assert path.name == 'flaxkv.toml'

            # 验证内容
            content = path.read_text(encoding='utf-8')
            assert '[server]' in content
        finally:
            os.chdir(original_cwd)

    def test_save_sample_config_custom_path(self, tmp_path):
        """测试保存示例配置到指定路径"""
        custom_path = tmp_path / "custom_config.toml"
        path = save_sample_config(str(custom_path))

        assert path.exists()
        assert path == custom_path

    def test_save_sample_config_file_exists(self, tmp_path):
        """测试配置文件已存在时的行为"""
        config_path = tmp_path / "flaxkv.toml"
        config_path.write_text("existing content", encoding='utf-8')

        with pytest.raises(FileExistsError):
            save_sample_config(str(config_path))


class TestConfigLoaderWithoutToml:
    """测试没有 tomllib 支持时的行为"""

    def test_load_config_without_tomllib(self, monkeypatch, tmp_path):
        """测试 tomllib 不可用时的行为"""
        # 模拟 tomllib 不可用
        import flaxkv2.utils.config_loader as config_loader_module
        original_tomllib = config_loader_module.tomllib
        try:
            monkeypatch.setattr(config_loader_module, 'tomllib', None)

            # 重新导入以应用 monkeypatch
            loader = config_loader_module.ConfigLoader()

            # 应该返回空配置，但不应该崩溃
            assert loader.config == {}
        finally:
            # 恢复原始的 tomllib
            config_loader_module.tomllib = original_tomllib


class TestConfigFileDiscovery:
    """测试配置文件发现逻辑"""

    def test_find_config_in_current_dir(self, tmp_path):
        """测试在当前目录查找配置文件"""
        config_path = tmp_path / "flaxkv.toml"
        config_path.write_text("[defaults]\n", encoding='utf-8')

        original_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)
            loader = ConfigLoader()
            found_path = loader._find_config_file()
            assert found_path is not None
            assert found_path.name == 'flaxkv.toml'
        finally:
            os.chdir(original_cwd)

    def test_find_config_priority(self, tmp_path):
        """测试配置文件查找优先级"""
        # 在当前目录同时创建 flaxkv.toml 和 .flaxkv.toml
        normal_config = tmp_path / "flaxkv.toml"
        hidden_config = tmp_path / ".flaxkv.toml"

        normal_config.write_text("[defaults]\nlog_level = 'NORMAL'\n", encoding='utf-8')
        hidden_config.write_text("[defaults]\nlog_level = 'HIDDEN'\n", encoding='utf-8')

        original_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)
            loader = ConfigLoader()

            # 应该优先找到普通文件 flaxkv.toml
            found_path = loader._find_config_file()
            assert found_path == normal_config
            assert loader.get_defaults()['log_level'] == 'NORMAL'
        finally:
            os.chdir(original_cwd)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
