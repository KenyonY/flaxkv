"""
测试 Config 模块的性能配置功能
"""

import unittest
from flaxkv2.config import PerformanceProfiles, create_leveldb_options


class TestPerformanceProfiles(unittest.TestCase):
    """测试 PerformanceProfiles 类"""

    def test_balanced_profile(self):
        """测试 balanced 配置文件"""
        config = PerformanceProfiles.get_profile('balanced')

        # 验证关键参数存在
        self.assertIn('lru_cache_size', config)
        self.assertIn('bloom_filter_bits', config)
        self.assertIn('block_size', config)
        self.assertIn('write_buffer_size', config)
        self.assertIn('max_open_files', config)

        # 验证值的合理性
        self.assertEqual(config['lru_cache_size'], 256 * 1024 * 1024)  # 256 MB
        self.assertEqual(config['bloom_filter_bits'], 10)
        self.assertEqual(config['block_size'], 16 * 1024)  # 16 KB
        self.assertEqual(config['write_buffer_size'], 128 * 1024 * 1024)  # 128 MB
        self.assertEqual(config['max_open_files'], 500)

    def test_read_optimized_profile(self):
        """测试 read_optimized 配置文件"""
        config = PerformanceProfiles.get_profile('read_optimized')

        # 读优化应该有更大的缓存
        self.assertEqual(config['lru_cache_size'], 512 * 1024 * 1024)  # 512 MB
        self.assertEqual(config['bloom_filter_bits'], 12)  # 更精确的布隆过滤器
        self.assertEqual(config['max_open_files'], 1000)  # 支持更多文件

    def test_write_optimized_profile(self):
        """测试 write_optimized 配置文件"""
        config = PerformanceProfiles.get_profile('write_optimized')

        # 写优化应该有更大的写缓冲
        self.assertEqual(config['write_buffer_size'], 256 * 1024 * 1024)  # 256 MB
        self.assertEqual(config['lru_cache_size'], 128 * 1024 * 1024)  # 较小的读缓存

    def test_memory_constrained_profile(self):
        """测试 memory_constrained 配置文件"""
        config = PerformanceProfiles.get_profile('memory_constrained')

        # 内存受限应该使用最小值
        self.assertEqual(config['lru_cache_size'], 64 * 1024 * 1024)  # 64 MB
        self.assertEqual(config['write_buffer_size'], 32 * 1024 * 1024)  # 32 MB
        self.assertEqual(config['max_open_files'], 200)  # 较少的文件描述符

    def test_large_database_profile(self):
        """测试 large_database 配置文件"""
        config = PerformanceProfiles.get_profile('large_database')

        # 大数据库应该使用大容量配置
        self.assertEqual(config['lru_cache_size'], 1024 * 1024 * 1024)  # 1 GB
        self.assertEqual(config['bloom_filter_bits'], 12)
        self.assertEqual(config['max_open_files'], 2000)

    def test_ml_workload_profile(self):
        """测试 ml_workload 配置文件"""
        config = PerformanceProfiles.get_profile('ml_workload')

        # ML工作负载应该优化大对象
        self.assertEqual(config['block_size'], 64 * 1024)  # 64 KB块大小
        self.assertEqual(config['lru_cache_size'], 512 * 1024 * 1024)  # 512 MB

    def test_unknown_profile(self):
        """测试未知配置文件应该抛出异常"""
        with self.assertRaises(ValueError) as cm:
            PerformanceProfiles.get_profile('unknown_profile')

        # 验证错误消息包含有用信息
        error_msg = str(cm.exception)
        self.assertIn('unknown_profile', error_msg)
        self.assertIn('balanced', error_msg)  # 应该列出可用的配置

    def test_list_profiles(self):
        """测试列出所有配置文件"""
        profiles_str = PerformanceProfiles.list_profiles()

        # 验证输出包含所有配置文件
        self.assertIn('balanced', profiles_str)
        self.assertIn('read_optimized', profiles_str)
        self.assertIn('write_optimized', profiles_str)
        self.assertIn('memory_constrained', profiles_str)
        self.assertIn('large_database', profiles_str)
        self.assertIn('ml_workload', profiles_str)

        # 验证包含描述信息
        self.assertIn('MB', profiles_str)
        self.assertIn('bits', profiles_str)

    def test_profile_copy(self):
        """测试 get_profile 返回副本而非引用"""
        config1 = PerformanceProfiles.get_profile('balanced')
        config2 = PerformanceProfiles.get_profile('balanced')

        # 修改其中一个不应该影响另一个
        config1['lru_cache_size'] = 999
        self.assertNotEqual(config1['lru_cache_size'], config2['lru_cache_size'])
        self.assertEqual(config2['lru_cache_size'], 256 * 1024 * 1024)

    def test_merge_with_custom(self):
        """测试自定义参数覆盖"""
        config = PerformanceProfiles.merge_with_custom(
            'balanced',
            lru_cache_size=1024 * 1024 * 1024,  # 自定义1GB
            bloom_filter_bits=15  # 自定义15 bits
        )

        # 验证自定义参数生效
        self.assertEqual(config['lru_cache_size'], 1024 * 1024 * 1024)
        self.assertEqual(config['bloom_filter_bits'], 15)

        # 验证其他参数保持不变
        self.assertEqual(config['block_size'], 16 * 1024)
        self.assertEqual(config['write_buffer_size'], 128 * 1024 * 1024)

    def test_merge_with_none_values(self):
        """测试 None 值不会覆盖默认值"""
        config = PerformanceProfiles.merge_with_custom(
            'balanced',
            lru_cache_size=None,  # None 不应该覆盖
            bloom_filter_bits=15  # 只有非None值生效
        )

        # 验证None没有覆盖默认值
        self.assertEqual(config['lru_cache_size'], 256 * 1024 * 1024)
        # 验证非None值生效
        self.assertEqual(config['bloom_filter_bits'], 15)

    def test_all_profiles_have_same_keys(self):
        """测试所有配置文件都有相同的键"""
        expected_keys = {'lru_cache_size', 'bloom_filter_bits', 'block_size',
                        'write_buffer_size', 'max_open_files'}

        for profile_name in PerformanceProfiles.PROFILES.keys():
            config = PerformanceProfiles.get_profile(profile_name)
            self.assertEqual(set(config.keys()), expected_keys,
                           f"{profile_name} 配置文件缺少必要的键")

    def test_profile_values_are_positive(self):
        """测试所有配置文件的值都是正数"""
        for profile_name in PerformanceProfiles.PROFILES.keys():
            config = PerformanceProfiles.get_profile(profile_name)
            for key, value in config.items():
                self.assertGreater(value, 0,
                                 f"{profile_name}.{key} 应该是正数，实际为 {value}")


class TestCreateLevelDBOptions(unittest.TestCase):
    """测试 create_leveldb_options 函数"""

    def test_default_options(self):
        """测试默认配置"""
        options = create_leveldb_options()

        # 应该使用 balanced 配置
        self.assertEqual(options['lru_cache_size'], 256 * 1024 * 1024)
        self.assertEqual(options['bloom_filter_bits'], 10)

        # 验证默认参数
        self.assertTrue(options['create_if_missing'])
        self.assertEqual(options['compression'], 'snappy')

    def test_custom_profile(self):
        """测试使用自定义配置文件"""
        options = create_leveldb_options(performance_profile='read_optimized')

        # 应该使用 read_optimized 配置
        self.assertEqual(options['lru_cache_size'], 512 * 1024 * 1024)
        self.assertEqual(options['bloom_filter_bits'], 12)

    def test_override_profile_params(self):
        """测试覆盖配置文件参数"""
        options = create_leveldb_options(
            performance_profile='balanced',
            lru_cache_size=1024 * 1024 * 1024,  # 覆盖为1GB
            compression='none'  # 覆盖压缩类型
        )

        # 验证覆盖生效
        self.assertEqual(options['lru_cache_size'], 1024 * 1024 * 1024)
        self.assertEqual(options['compression'], 'none')

        # 其他参数应该保持配置文件的值
        self.assertEqual(options['bloom_filter_bits'], 10)

    def test_create_if_missing_option(self):
        """测试 create_if_missing 选项"""
        options1 = create_leveldb_options(create_if_missing=True)
        options2 = create_leveldb_options(create_if_missing=False)

        self.assertTrue(options1['create_if_missing'])
        self.assertFalse(options2['create_if_missing'])

    def test_compression_options(self):
        """测试压缩选项"""
        for compression_type in ['snappy', 'none']:
            options = create_leveldb_options(compression=compression_type)
            self.assertEqual(options['compression'], compression_type)

    def test_none_params_dont_override(self):
        """测试 None 参数不会覆盖配置文件值"""
        options = create_leveldb_options(
            performance_profile='balanced',
            lru_cache_size=None,  # None 不应该覆盖
            bloom_filter_bits=15  # 非None应该覆盖
        )

        # 验证None没有覆盖
        self.assertEqual(options['lru_cache_size'], 256 * 1024 * 1024)
        # 验证非None覆盖生效
        self.assertEqual(options['bloom_filter_bits'], 15)

    def test_all_profiles_work_with_create_options(self):
        """测试所有配置文件都能正常工作"""
        for profile_name in PerformanceProfiles.PROFILES.keys():
            try:
                options = create_leveldb_options(performance_profile=profile_name)
                # 验证基本键都存在
                self.assertIn('lru_cache_size', options)
                self.assertIn('compression', options)
                self.assertIn('create_if_missing', options)
            except Exception as e:
                self.fail(f"配置文件 {profile_name} 应该能正常工作，但抛出异常: {e}")

    def test_parameter_types(self):
        """测试参数类型正确"""
        options = create_leveldb_options()

        # 验证整数类型
        self.assertIsInstance(options['lru_cache_size'], int)
        self.assertIsInstance(options['bloom_filter_bits'], int)
        self.assertIsInstance(options['block_size'], int)
        self.assertIsInstance(options['write_buffer_size'], int)
        self.assertIsInstance(options['max_open_files'], int)

        # 验证布尔类型
        self.assertIsInstance(options['create_if_missing'], bool)

        # 验证字符串类型
        self.assertIsInstance(options['compression'], str)


if __name__ == '__main__':
    unittest.main()
