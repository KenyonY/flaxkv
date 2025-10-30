"""
FlaxKV2 配置管理模块

提供预定义的性能配置文件(profiles)，用于不同的使用场景。
用户可以选择预设配置或自定义参数。
"""

from typing import Dict, Any, Optional


class PerformanceProfiles:
    """
    LevelDB 性能配置文件

    每个配置文件针对特定的使用场景优化，包含以下参数：
    - lru_cache_size: LRU块缓存大小（影响读性能）
    - bloom_filter_bits: 布隆过滤器位数（影响不存在key的查询性能）
    - block_size: 数据块大小（影响读取粒度）
    - write_buffer_size: MemTable大小（影响写性能）
    - max_open_files: 最大打开文件数（影响资源占用）

    详细说明参见: doc/LEVELDB_CONFIGURATION_GUIDE.md
    """

    # 通用平衡配置（默认推荐）
    BALANCED = {
        'lru_cache_size': 256 * 1024 * 1024,      # 256 MB 读缓存
        'bloom_filter_bits': 10,                   # 1% 假阳性率
        'block_size': 16 * 1024,                   # 16 KB
        'write_buffer_size': 128 * 1024 * 1024,   # 128 MB 写缓冲
        'max_open_files': 500,                     # 500 个文件描述符
    }

    # 读密集型优化
    READ_OPTIMIZED = {
        'lru_cache_size': 512 * 1024 * 1024,      # 512 MB（加大缓存）
        'bloom_filter_bits': 12,                   # 0.5% 假阳性率（更精确）
        'block_size': 16 * 1024,                   # 16 KB
        'write_buffer_size': 64 * 1024 * 1024,    # 64 MB（减小写缓冲）
        'max_open_files': 1000,                    # 1000（支持更多文件）
    }

    # 写密集型优化
    WRITE_OPTIMIZED = {
        'lru_cache_size': 128 * 1024 * 1024,      # 128 MB（减小缓存）
        'bloom_filter_bits': 10,                   # 10 bits
        'block_size': 32 * 1024,                   # 32 KB（减少块数量）
        'write_buffer_size': 256 * 1024 * 1024,   # 256 MB（加大写缓冲）
        'max_open_files': 500,                     # 500
    }

    # 内存受限配置
    MEMORY_CONSTRAINED = {
        'lru_cache_size': 64 * 1024 * 1024,       # 64 MB（最小推荐）
        'bloom_filter_bits': 8,                    # 8 bits（节省内存）
        'block_size': 8 * 1024,                    # 8 KB
        'write_buffer_size': 32 * 1024 * 1024,    # 32 MB
        'max_open_files': 200,                     # 200（减少FD占用）
    }

    # 大数据库配置（>100GB）
    LARGE_DATABASE = {
        'lru_cache_size': 1024 * 1024 * 1024,     # 1 GB（大容量缓存）
        'bloom_filter_bits': 12,                   # 12 bits（精确过滤）
        'block_size': 32 * 1024,                   # 32 KB
        'write_buffer_size': 256 * 1024 * 1024,   # 256 MB
        'max_open_files': 2000,                    # 2000（支持大量文件）
    }

    # 机器学习/科学计算配置
    ML_WORKLOAD = {
        'lru_cache_size': 512 * 1024 * 1024,      # 512 MB
        'bloom_filter_bits': 10,                   # 10 bits
        'block_size': 64 * 1024,                   # 64 KB（大对象优化）
        'write_buffer_size': 256 * 1024 * 1024,   # 256 MB
        'max_open_files': 500,                     # 500
    }

    # 所有可用的配置文件
    PROFILES = {
        'balanced': BALANCED,
        'read_optimized': READ_OPTIMIZED,
        'write_optimized': WRITE_OPTIMIZED,
        'memory_constrained': MEMORY_CONSTRAINED,
        'large_database': LARGE_DATABASE,
        'ml_workload': ML_WORKLOAD,
    }

    # 配置文件说明
    PROFILE_DESCRIPTIONS = {
        'balanced': '通用平衡配置（推荐默认）- 适合混合读写工作负载',
        'read_optimized': '读密集型优化 - 适合缓存服务、API查询（90%+读操作）',
        'write_optimized': '写密集型优化 - 适合日志收集、批量导入（70%+写操作）',
        'memory_constrained': '内存受限配置 - 适合嵌入式设备、容器环境（<4GB内存）',
        'large_database': '大数据库配置 - 适合数据库>100GB、热数据>1GB的场景',
        'ml_workload': '机器学习/科学计算 - 适合存储NumPy数组、模型参数（大对象为主）',
    }

    @classmethod
    def get_profile(cls, profile_name: str) -> Dict[str, Any]:
        """
        获取指定的配置文件

        Args:
            profile_name: 配置文件名称

        Returns:
            配置字典

        Raises:
            ValueError: 如果配置文件不存在
        """
        if profile_name not in cls.PROFILES:
            available = ', '.join(cls.PROFILES.keys())
            raise ValueError(
                f"未知的配置文件: '{profile_name}'\n"
                f"可用的配置文件: {available}\n"
                f"使用 PerformanceProfiles.list_profiles() 查看详细说明"
            )
        return cls.PROFILES[profile_name].copy()

    @classmethod
    def list_profiles(cls) -> str:
        """
        列出所有可用的配置文件及其说明

        Returns:
            格式化的配置文件列表
        """
        lines = ["可用的性能配置文件:\n"]
        for name, profile in cls.PROFILES.items():
            desc = cls.PROFILE_DESCRIPTIONS.get(name, "")
            lines.append(f"  • {name:20s} - {desc}")

            # 显示关键参数
            cache_mb = profile['lru_cache_size'] / (1024 * 1024)
            write_mb = profile['write_buffer_size'] / (1024 * 1024)
            lines.append(f"    {'':20s}   缓存: {cache_mb:.0f}MB, "
                        f"写缓冲: {write_mb:.0f}MB, "
                        f"布隆过滤器: {profile['bloom_filter_bits']} bits")

        lines.append("\n详细说明参见: doc/LEVELDB_CONFIGURATION_GUIDE.md")
        return "\n".join(lines)

    @classmethod
    def merge_with_custom(
        cls,
        profile_name: str,
        **custom_params
    ) -> Dict[str, Any]:
        """
        合并预设配置和自定义参数

        Args:
            profile_name: 基础配置文件名称
            **custom_params: 自定义参数（会覆盖配置文件中的值）

        Returns:
            合并后的配置字典
        """
        config = cls.get_profile(profile_name)

        # 自定义参数覆盖预设值
        for key, value in custom_params.items():
            if value is not None:
                config[key] = value

        return config


def create_leveldb_options(
    create_if_missing: bool = True,
    compression: str = 'snappy',
    performance_profile: str = 'balanced',
    lru_cache_size: Optional[int] = None,
    bloom_filter_bits: Optional[int] = None,
    block_size: Optional[int] = None,
    write_buffer_size: Optional[int] = None,
    max_open_files: Optional[int] = None,
) -> Dict[str, Any]:
    """
    创建 LevelDB 配置选项

    Args:
        create_if_missing: 数据库不存在时是否创建
        compression: 压缩算法 ('snappy', 'zlib', None)
        performance_profile: 性能配置文件名称（默认 'balanced'）
        lru_cache_size: LRU缓存大小（字节），覆盖profile中的值
        bloom_filter_bits: 布隆过滤器位数，覆盖profile中的值
        block_size: 数据块大小（字节），覆盖profile中的值
        write_buffer_size: 写缓冲大小（字节），覆盖profile中的值
        max_open_files: 最大打开文件数，覆盖profile中的值

    Returns:
        完整的 LevelDB 配置字典

    Examples:
        >>> # 使用默认配置
        >>> opts = create_leveldb_options()

        >>> # 使用读优化配置
        >>> opts = create_leveldb_options(performance_profile='read_optimized')

        >>> # 在默认配置基础上自定义缓存大小
        >>> opts = create_leveldb_options(lru_cache_size=512*1024*1024)

        >>> # 完全自定义
        >>> opts = create_leveldb_options(
        ...     lru_cache_size=300*1024*1024,
        ...     bloom_filter_bits=12,
        ...     block_size=20*1024
        ... )
    """
    # 获取性能配置
    perf_config = PerformanceProfiles.merge_with_custom(
        performance_profile,
        lru_cache_size=lru_cache_size,
        bloom_filter_bits=bloom_filter_bits,
        block_size=block_size,
        write_buffer_size=write_buffer_size,
        max_open_files=max_open_files,
    )

    # 合并所有配置
    options = {
        'create_if_missing': create_if_missing,
        'compression': compression,
        **perf_config
    }

    return options


# 便捷函数：打印配置信息
def print_config_info(config: Dict[str, Any]) -> None:
    """
    打印配置信息（便于调试）

    Args:
        config: 配置字典
    """
    print("LevelDB 配置:")
    print(f"  • LRU缓存: {config.get('lru_cache_size', 0) / (1024*1024):.0f} MB")
    print(f"  • 写缓冲: {config.get('write_buffer_size', 0) / (1024*1024):.0f} MB")
    print(f"  • 布隆过滤器: {config.get('bloom_filter_bits', 0)} bits")
    print(f"  • 数据块大小: {config.get('block_size', 0) / 1024:.0f} KB")
    print(f"  • 最大文件数: {config.get('max_open_files', 0)}")
    print(f"  • 压缩算法: {config.get('compression', 'none')}")

    # 估算内存占用
    total_memory = (
        config.get('lru_cache_size', 0) +
        config.get('write_buffer_size', 0)
    ) / (1024 * 1024)
    print(f"  • 预计内存占用: ~{total_memory:.0f} MB")


if __name__ == '__main__':
    # 示例：打印所有配置文件
    print(PerformanceProfiles.list_profiles())
    print("\n" + "="*70 + "\n")

    # 示例：创建配置
    print("示例1: 默认配置")
    opts1 = create_leveldb_options()
    print_config_info(opts1)

    print("\n" + "="*70 + "\n")

    print("示例2: 读优化配置")
    opts2 = create_leveldb_options(performance_profile='read_optimized')
    print_config_info(opts2)

    print("\n" + "="*70 + "\n")

    print("示例3: 自定义配置")
    opts3 = create_leveldb_options(
        performance_profile='balanced',
        lru_cache_size=512*1024*1024
    )
    print_config_info(opts3)
