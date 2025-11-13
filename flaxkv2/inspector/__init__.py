"""
FlaxKV2 Inspector - 数据库可视化和检查工具核心模块
"""

import sys
from typing import Any, Dict, List, Optional, Iterator, Tuple
from datetime import datetime, timezone
import re

from flaxkv2 import FlaxKV
from flaxkv2.serialization.value_meta import ValueWithMeta


class Inspector:
    """
    FlaxKV 数据库检查器核心类

    提供数据浏览、统计分析、搜索等功能，供 CLI 和 Web UI 共同使用
    """

    def __init__(self, db_name: str, path: str, backend: str = 'local', **kwargs):
        """
        初始化 Inspector

        Args:
            db_name: 数据库名称
            path: 数据库路径（本地路径或远程地址）
            backend: 后端类型 ('local', 'remote', 'auto')
            **kwargs: 传递给 FlaxKV 的其他参数
        """
        self.db_name = db_name
        self.path = path
        self.backend = backend
        self.db = FlaxKV(db_name, path, backend=backend, **kwargs)

    def close(self):
        """关闭数据库连接"""
        if self.db:
            self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def count_keys(self) -> int:
        """获取键总数"""
        try:
            return len(self.db)
        except Exception:
            # 如果 len() 不支持，则遍历计数
            return sum(1 for _ in self.db.keys())

    def list_keys(self, pattern: Optional[str] = None, limit: int = 100,
                  offset: int = 0) -> List[str]:
        """
        列出所有键（支持分页和模式匹配）

        Args:
            pattern: 正则表达式模式（可选）
            limit: 返回的最大数量
            offset: 跳过的键数量

        Returns:
            键列表
        """
        keys = []
        regex = re.compile(pattern) if pattern else None

        count = 0
        for key in self.db.keys():
            # 模式匹配
            if regex and not regex.search(key):
                continue

            # 分页
            if count < offset:
                count += 1
                continue

            if len(keys) >= limit:
                break

            keys.append(key)
            count += 1

        return keys

    def get_value_info(self, key: str) -> Optional[Dict[str, Any]]:
        """
        获取键的详细信息

        Args:
            key: 键名

        Returns:
            包含类型、大小、TTL、值等信息的字典，键不存在返回 None
        """
        if key not in self.db:
            return None

        try:
            # 获取原始值
            raw_value = self.db[key]

            # 获取值的基本信息
            info = {
                'key': key,
                'exists': True,
                'type': type(raw_value).__name__,
                'size': sys.getsizeof(raw_value),
                'value': None,
                'ttl': None,
                'expires_at': None,
            }

            # 尝试获取更详细的类型信息
            type_name = self._get_type_name(raw_value)
            info['type'] = type_name

            # 获取值的预览
            info['value'] = self._get_value_preview(raw_value)

            # 检查是否有 TTL 信息
            ttl_info = self._get_ttl_info(key)
            if ttl_info:
                info.update(ttl_info)

            return info

        except Exception as e:
            return {
                'key': key,
                'exists': True,
                'error': str(e),
                'type': 'unknown',
                'size': 0,
            }

    def _get_type_name(self, value: Any) -> str:
        """获取值的类型名称"""
        type_map = {
            'int': 'integer',
            'float': 'float',
            'str': 'string',
            'bool': 'boolean',
            'list': 'list',
            'dict': 'dict',
            'bytes': 'bytes',
            'NoneType': 'null',
        }

        type_name = type(value).__name__

        # 特殊类型检测
        if type_name == 'ndarray':
            return 'numpy.ndarray'
        elif type_name == 'DataFrame':
            return 'pandas.DataFrame'
        elif type_name == 'Series':
            return 'pandas.Series'
        elif type_name == 'NestedDBList':
            return 'list'
        elif type_name == 'NestedDBDict':
            return 'dict'

        return type_map.get(type_name, type_name)

    def _get_value_preview(self, value: Any, max_length: int = 100) -> Any:
        """获取值的预览（截断长内容）"""
        if isinstance(value, (str, bytes)):
            if len(value) > max_length:
                preview = value[:max_length]
                if isinstance(value, str):
                    return preview + '...'
                else:
                    return preview + b'...'
            return value
        elif isinstance(value, (list, tuple)):
            if len(value) > 10:
                return list(value[:10]) + ['...']
            return value
        elif isinstance(value, dict):
            if len(value) > 10:
                items = list(value.items())[:10]
                preview = dict(items)
                preview['...'] = f'... ({len(value) - 10} more items)'
                return preview
            return value
        else:
            # 其他类型，尝试转为字符串
            str_repr = str(value)
            if len(str_repr) > max_length:
                return str_repr[:max_length] + '...'
            return value

    def _get_ttl_info(self, key: str) -> Optional[Dict[str, Any]]:
        """获取键的 TTL 信息"""
        try:
            # 尝试从数据库获取原始字节
            if hasattr(self.db, '_get_raw'):
                raw_bytes = self.db._get_raw(key)
            elif hasattr(self.db, 'db') and hasattr(self.db.db, 'get'):
                # RawLevelDBDict
                raw_bytes = self.db.db.get(key.encode('utf-8'))
            else:
                return None

            if not raw_bytes:
                return None

            # 尝试解析为 ValueWithMeta
            try:
                value_meta = ValueWithMeta.from_bytes(raw_bytes)
                if value_meta.expires_at:
                    now = datetime.now(timezone.utc).timestamp()
                    ttl = value_meta.expires_at - now
                    return {
                        'ttl': max(0, ttl),
                        'expires_at': datetime.fromtimestamp(value_meta.expires_at).isoformat(),
                        'expired': ttl <= 0,
                    }
            except Exception:
                pass

            return None

        except Exception:
            return None

    def delete_key(self, key: str) -> bool:
        """
        删除指定键

        Args:
            key: 键名

        Returns:
            成功返回 True，失败返回 False
        """
        try:
            if key in self.db:
                del self.db[key]
                return True
            return False
        except Exception:
            return False

    def set_value(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        设置键值

        Args:
            key: 键名
            value: 值
            ttl: 过期时间（秒）

        Returns:
            成功返回 True，失败返回 False
        """
        try:
            if ttl:
                self.db.set(key, value, ttl=ttl)
            else:
                self.db[key] = value
            return True
        except Exception:
            return False

    def get_stats(self) -> Dict[str, Any]:
        """
        获取数据库统计信息

        Returns:
            统计信息字典
        """
        stats = {
            'total_keys': 0,
            'type_distribution': {},
            'size_distribution': {
                'tiny': 0,      # < 1KB
                'small': 0,     # 1KB - 10KB
                'medium': 0,    # 10KB - 100KB
                'large': 0,     # 100KB - 1MB
                'huge': 0,      # > 1MB
            },
            'ttl_status': {
                'with_ttl': 0,
                'without_ttl': 0,
                'expired': 0,
            },
            'total_size': 0,
        }

        try:
            for key in self.db.keys():
                stats['total_keys'] += 1

                try:
                    value = self.db[key]

                    # 类型分布
                    type_name = self._get_type_name(value)
                    stats['type_distribution'][type_name] = \
                        stats['type_distribution'].get(type_name, 0) + 1

                    # 大小分布
                    size = sys.getsizeof(value)
                    stats['total_size'] += size

                    if size < 1024:
                        stats['size_distribution']['tiny'] += 1
                    elif size < 10 * 1024:
                        stats['size_distribution']['small'] += 1
                    elif size < 100 * 1024:
                        stats['size_distribution']['medium'] += 1
                    elif size < 1024 * 1024:
                        stats['size_distribution']['large'] += 1
                    else:
                        stats['size_distribution']['huge'] += 1

                    # TTL 状态
                    ttl_info = self._get_ttl_info(key)
                    if ttl_info:
                        stats['ttl_status']['with_ttl'] += 1
                        if ttl_info.get('expired'):
                            stats['ttl_status']['expired'] += 1
                    else:
                        stats['ttl_status']['without_ttl'] += 1

                except Exception:
                    continue

            return stats

        except Exception as e:
            return {
                'error': str(e),
                'total_keys': 0,
            }

    def search_keys(self, pattern: str, limit: int = 100) -> List[Tuple[str, Any]]:
        """
        搜索键（返回键和部分信息）

        Args:
            pattern: 正则表达式模式
            limit: 最大返回数量

        Returns:
            (键名, 简要信息) 的列表
        """
        results = []
        regex = re.compile(pattern)

        for key in self.db.keys():
            if not regex.search(key):
                continue

            if len(results) >= limit:
                break

            try:
                value = self.db[key]
                info = {
                    'type': self._get_type_name(value),
                    'size': sys.getsizeof(value),
                }
                results.append((key, info))
            except Exception:
                results.append((key, {'type': 'error', 'size': 0}))

        return results

    def export_data(self, keys: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        导出数据

        Args:
            keys: 要导出的键列表，None 表示导出所有

        Returns:
            包含所有键值对的字典
        """
        data = {}
        key_list = keys if keys else list(self.db.keys())

        for key in key_list:
            try:
                data[key] = self.db[key]
            except Exception:
                data[key] = None

        return data
