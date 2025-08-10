"""
配置管理系统单元测试
"""

import pytest
import tempfile
from pathlib import Path

from flaxkv.core.config import FlaxKVConfig, parse_size


class TestParseSizeFunction:
    """测试 parse_size 函数。"""
    
    def test_parse_bytes(self):
        """测试字节解析。"""
        assert parse_size("1024") == 1024
        assert parse_size("1024B") == 1024
    
    def test_parse_kilobytes(self):
        """测试KB解析。"""
        assert parse_size("1K") == 1024
        assert parse_size("1KB") == 1024
        assert parse_size("2K") == 2048
    
    def test_parse_megabytes(self):
        """测试MB解析。"""
        assert parse_size("1M") == 1024 * 1024
        assert parse_size("1MB") == 1024 * 1024
        assert parse_size("100MB") == 100 * 1024 * 1024
    
    def test_parse_gigabytes(self):
        """测试GB解析。"""
        assert parse_size("1G") == 1024 * 1024 * 1024
        assert parse_size("1GB") == 1024 * 1024 * 1024
        assert parse_size("2GB") == 2 * 1024 * 1024 * 1024
    
    def test_case_insensitive(self):
        """测试大小写不敏感。"""
        assert parse_size("100mb") == 100 * 1024 * 1024
        assert parse_size("1gb") == 1024 * 1024 * 1024
        assert parse_size("512kb") == 512 * 1024


class TestFlaxKVConfig:
    """测试 FlaxKVConfig 类。"""
    
    def test_default_config(self):
        """测试默认配置。"""
        config = FlaxKVConfig()
        
        assert config.buffer_size == 1000
        assert config.buffer_timeout == 5.0
        assert config.cache_size == "100MB"
        assert config.cache_policy == "lru"
        assert config.sync_mode == "async"
        assert config.compression is True
        assert config.network_protocol == "zmq"
    
    def test_config_validation(self):
        """测试配置验证。"""
        # 有效配置
        config = FlaxKVConfig(
            buffer_size=2000,
            cache_policy="lfu",
            sync_mode="sync"
        )
        assert config.buffer_size == 2000
        
        # 无效buffer_size
        with pytest.raises(ValueError, match="buffer_size必须大于0"):
            FlaxKVConfig(buffer_size=0)
        
        # 无效cache_policy
        with pytest.raises(ValueError, match="cache_policy必须是lru/lfu/arc之一"):
            FlaxKVConfig(cache_policy="invalid")
        
        # 无效sync_mode
        with pytest.raises(ValueError, match="sync_mode必须是async/sync/manual之一"):
            FlaxKVConfig(sync_mode="invalid")
    
    def test_cache_size_bytes_calculation(self):
        """测试缓存大小字节计算。"""
        config = FlaxKVConfig(cache_size="256MB")
        assert config.cache_size_bytes == 256 * 1024 * 1024
        
        config = FlaxKVConfig(cache_size="1GB")
        assert config.cache_size_bytes == 1024 * 1024 * 1024
    
    def test_properties(self):
        """测试配置属性。"""
        # 未加密配置
        config = FlaxKVConfig()
        assert not config.is_encrypted
        
        # 加密配置
        config = FlaxKVConfig(encryption_key="secret_key")
        assert config.is_encrypted
        
        # 缓存大小MB
        config = FlaxKVConfig(cache_size="512MB")
        assert config.cache_size_mb == 512
    
    def test_development_preset(self):
        """测试开发环境预设。"""
        config = FlaxKVConfig.for_development()
        
        assert config.buffer_size == 100
        assert config.buffer_timeout == 1.0
        assert config.cache_size == "64MB"
        assert config.sync_mode == "sync"
        assert config.backup_interval == 0
        assert config.metrics_enabled is True
        assert config.slow_query_threshold == 0.1
    
    def test_production_preset(self):
        """测试生产环境预设。"""
        config = FlaxKVConfig.for_production()
        
        assert config.buffer_size == 5000
        assert config.buffer_timeout == 10.0
        assert config.cache_size == "1GB"
        assert config.cache_policy == "arc"
        assert config.sync_mode == "async"
        assert config.compression is True
        assert config.backup_interval == 1800
        assert config.network_protocol == "zmq"
    
    def test_high_performance_preset(self):
        """测试高性能预设。"""
        config = FlaxKVConfig.for_high_performance()
        
        assert config.buffer_size == 10000
        assert config.buffer_timeout == 60.0
        assert config.cache_size == "2GB"
        assert config.sync_mode == "manual"
        assert config.compression is False
        assert config.backup_interval == 0
        assert config.integrity_check is False
        assert config.metrics_enabled is False
    
    def test_testing_preset(self):
        """测试测试环境预设。"""
        config = FlaxKVConfig.for_testing()
        
        assert config.buffer_size == 10
        assert config.buffer_timeout == 0.1
        assert config.cache_size == "16MB"
        assert config.sync_mode == "sync"
        assert config.compression is False
        assert config.backup_interval == 0
        assert config.metrics_enabled is False
    
    def test_to_dict(self):
        """测试转换为字典。"""
        config = FlaxKVConfig(buffer_size=2000, cache_size="256MB")
        config_dict = config.to_dict()
        
        assert isinstance(config_dict, dict)
        assert config_dict["buffer_size"] == 2000
        assert config_dict["cache_size"] == "256MB"
    
    def test_from_dict(self):
        """测试从字典创建配置。"""
        config_dict = {
            "buffer_size": 3000,
            "cache_size": "512MB",
            "sync_mode": "sync"
        }
        config = FlaxKVConfig.from_dict(config_dict)
        
        assert config.buffer_size == 3000
        assert config.cache_size == "512MB"
        assert config.sync_mode == "sync"
    
    def test_from_dict_filters_invalid_fields(self):
        """测试从字典创建时过滤无效字段。"""
        config_dict = {
            "buffer_size": 2000,
            "invalid_field": "should_be_ignored",
            "cache_size": "128MB"
        }
        config = FlaxKVConfig.from_dict(config_dict)
        
        assert config.buffer_size == 2000
        assert config.cache_size == "128MB"
        assert not hasattr(config, "invalid_field")
    
    def test_file_operations(self):
        """测试配置文件读写。"""
        config = FlaxKVConfig(
            buffer_size=4000,
            cache_size="1GB",
            compression=False
        )
        
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False) as f:
            temp_path = f.name
        
        try:
            # 保存配置
            config.save_to_file(temp_path)
            
            # 加载配置
            loaded_config = FlaxKVConfig.from_file(temp_path)
            
            assert loaded_config.buffer_size == 4000
            assert loaded_config.cache_size == "1GB"
            assert loaded_config.compression is False
            
        finally:
            Path(temp_path).unlink(missing_ok=True)
    
    def test_file_not_found(self):
        """测试加载不存在的配置文件。"""
        with pytest.raises(FileNotFoundError):
            FlaxKVConfig.from_file("nonexistent_file.yaml")
    
    def test_repr(self):
        """测试字符串表示。"""
        config = FlaxKVConfig(
            buffer_size=1500,
            cache_size="256MB",
            network_protocol="http",
            sync_mode="sync"
        )
        
        repr_str = repr(config)
        assert "FlaxKVConfig(" in repr_str
        assert "buffer=1500" in repr_str
        assert "cache=256MB" in repr_str
        assert "protocol=http" in repr_str
        assert "sync=sync" in repr_str