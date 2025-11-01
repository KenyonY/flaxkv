"""
基于 prefixed_db 的嵌套字典实现

解决嵌套数据频繁序列化的性能问题
"""
from typing import Any, Iterator, Optional
import threading
from collections.abc import MutableMapping
from flaxkv2.serialization import encoder, decoder
from flaxkv2.utils.log import get_logger

logger = get_logger(__name__)


class NestedDBDict(MutableMapping):
    """
    基于 LevelDB prefixed_db 的嵌套字典实现

    核心优势：
    - 修改单个字段只序列化该字段的值，而不是整个字典
    - 读取单个字段只反序列化该字段的值
    - 利用 LevelDB 的前缀查询能力高效迭代

    使用示例：
        db = LevelDBDict('mydb')

        # 创建嵌套字典
        nested = db.nested('user:1')

        # 像普通字典一样使用（但每个字段独立存储）
        nested['name'] = 'Alice'
        nested['age'] = 30
        nested['city'] = 'NYC'

        # 高效修改单个字段（只序列化 age 的值）
        nested['age'] = 31

        # 高效读取单个字段（只反序列化 name 的值）
        print(nested['name'])

        # 迭代所有字段
        for key, value in nested.items():
            print(key, value)
    """

    def __init__(self, prefixed_db, prefix: str, parent_db=None, root_db=None):
        """
        初始化嵌套字典

        Args:
            prefixed_db: plyvel 的 PrefixedDB 对象
            prefix: 前缀字符串（用于日志和显示）
            parent_db: 父数据库对象（用于访问缓冲区）
            root_db: 根数据库对象（用于递归创建嵌套字典）
        """
        self._prefixed_db = prefixed_db
        self._prefix = prefix
        self._parent_db = parent_db
        self._root_db = root_db if root_db is not None else parent_db  # 根数据库
        self._lock = threading.RLock()

    def __getitem__(self, key: str) -> Any:
        """获取字段值（支持递归嵌套字典）"""
        if not isinstance(key, str):
            raise TypeError(f"Key must be str, not {type(key).__name__}")

        # 检查是否是嵌套字典
        full_key = f"{self._prefix}{key}"
        if self._root_db is not None:
            try:
                # 检查标记键
                is_nested = self._root_db.get(f'__nested__:{full_key}')
                if is_nested:
                    # 返回更深层的 NestedDBDict
                    sub_prefix = f"{full_key}:"
                    sub_prefix_bytes = sub_prefix.encode('utf-8')
                    sub_prefixed_db = self._root_db._db.prefixed_db(sub_prefix_bytes)
                    return NestedDBDict(sub_prefixed_db, sub_prefix,
                                       parent_db=None, root_db=self._root_db)
            except (KeyError, AttributeError):
                pass

        # 叶子值：直接读取
        key_bytes = key.encode('utf-8')
        value_bytes = self._prefixed_db.get(key_bytes)

        if value_bytes is None:
            raise KeyError(key)

        # 反序列化单个值
        return decoder.decode(value_bytes)

    def __setitem__(self, key: str, value: Any) -> None:
        """设置字段值（支持递归嵌套字典）"""
        if not isinstance(key, str):
            raise TypeError(f"Key must be str, not {type(key).__name__}")

        full_key = f"{self._prefix}{key}"

        # 检测是否是字典类型
        if isinstance(value, dict):
            # 1. 标记为嵌套字典
            if self._root_db is not None:
                self._root_db[f'__nested__:{full_key}'] = True

            # 2. 创建更深层的 nested
            sub_prefix = f"{full_key}:"
            sub_prefix_bytes = sub_prefix.encode('utf-8')
            if self._root_db is not None:
                sub_prefixed_db = self._root_db._db.prefixed_db(sub_prefix_bytes)
            else:
                # fallback: 使用当前 prefixed_db（不推荐，但保持兼容）
                raise RuntimeError("Cannot create nested dict without root_db")

            sub_nested = NestedDBDict(sub_prefixed_db, sub_prefix,
                                     parent_db=None, root_db=self._root_db)

            # 3. 清空旧数据并递归写入
            sub_nested.clear()
            for k, v in value.items():
                sub_nested[k] = v  # 递归！
        else:
            # 非字典：取消嵌套标记（如果有）
            if self._root_db is not None:
                try:
                    del self._root_db[f'__nested__:{full_key}']
                except KeyError:
                    pass

            # 直接写入叶子值
            key_bytes = key.encode('utf-8')
            value_bytes = encoder.encode(value)
            self._prefixed_db.put(key_bytes, value_bytes)

    def __delitem__(self, key: str) -> None:
        """删除字段（支持递归删除嵌套字典）"""
        if not isinstance(key, str):
            raise TypeError(f"Key must be str, not {type(key).__name__}")

        full_key = f"{self._prefix}{key}"

        # 检查是否是嵌套字典
        if self._root_db is not None:
            try:
                is_nested = self._root_db.get(f'__nested__:{full_key}')
                if is_nested:
                    # 递归删除所有子键
                    sub_prefix = f"{full_key}:"
                    sub_prefix_bytes = sub_prefix.encode('utf-8')
                    sub_prefixed_db = self._root_db._db.prefixed_db(sub_prefix_bytes)
                    sub_nested = NestedDBDict(sub_prefixed_db, sub_prefix,
                                             parent_db=None, root_db=self._root_db)
                    sub_nested.clear()

                    # 删除标记
                    try:
                        del self._root_db[f'__nested__:{full_key}']
                    except KeyError:
                        pass
                    return
            except (KeyError, AttributeError):
                pass

        # 叶子值：直接删除
        key_bytes = key.encode('utf-8')

        # 检查键是否存在
        if self._prefixed_db.get(key_bytes) is None:
            raise KeyError(key)

        # 删除
        self._prefixed_db.delete(key_bytes)

    def __contains__(self, key: str) -> bool:
        """检查字段是否存在"""
        if not isinstance(key, str):
            return False

        # 直接检查 prefixed_db
        key_bytes = key.encode('utf-8')
        return self._prefixed_db.get(key_bytes) is not None

    def __len__(self) -> int:
        """返回字段数量"""
        count = 0
        for _ in self._prefixed_db:
            count += 1
        return count

    def __iter__(self) -> Iterator[str]:
        """迭代所有字段名（只返回第一层的键）"""
        seen_keys = set()
        for key_bytes, _ in self._prefixed_db:
            key = key_bytes.decode('utf-8')
            # 只取第一层的键（冒号分隔的第一部分）
            first_level_key = key.split(':')[0]
            if first_level_key not in seen_keys:
                seen_keys.add(first_level_key)
                yield first_level_key

    def keys(self) -> Iterator[str]:
        """返回所有字段名"""
        return iter(self)

    def values(self) -> Iterator[Any]:
        """返回所有字段值（只返回第一层的值）"""
        for key in self.keys():
            yield self[key]  # 通过 __getitem__ 获取（可能是 NestedDBDict）

    def items(self) -> Iterator[tuple[str, Any]]:
        """返回所有字段的键值对（只返回第一层）"""
        for key in self.keys():
            yield key, self[key]  # 通过 __getitem__ 获取（可能是 NestedDBDict）

    def get(self, key: str, default=None) -> Any:
        """获取字段值，如果不存在返回默认值"""
        try:
            return self[key]
        except KeyError:
            return default

    def set(self, key: str, value: Any) -> None:
        """
        设置字段值（等同于 __setitem__）

        为了与 RawLevelDBDict 保持 API 一致性
        """
        self[key] = value

    def pop(self, key: str, default=None) -> Any:
        """删除并返回字段值"""
        try:
            value = self[key]
            del self[key]
            return value
        except KeyError:
            if default is None:
                raise
            return default

    def popitem(self) -> tuple[str, Any]:
        """
        删除并返回一个键值对

        如果字典为空，抛出 KeyError
        """
        try:
            # 获取第一个键
            key = next(iter(self))
            value = self[key]
            del self[key]
            return (key, value)
        except StopIteration:
            raise KeyError("popitem(): dictionary is empty")

    def update(self, *args, **kwargs) -> None:
        """批量更新字段"""
        if args:
            if len(args) > 1:
                raise TypeError(f"update expected at most 1 arguments, got {len(args)}")
            other = args[0]
            if hasattr(other, "items"):
                for key, value in other.items():
                    self[key] = value
            else:
                for key, value in other:
                    self[key] = value

        for key, value in kwargs.items():
            self[key] = value

    def clear(self) -> None:
        """清空所有字段"""
        keys_to_delete = list(self.keys())
        for key in keys_to_delete:
            del self[key]

    def setdefault(self, key: str, default=None) -> Any:
        """如果字段不存在则设置默认值"""
        try:
            return self[key]
        except KeyError:
            self[key] = default
            return default

    def to_dict(self) -> dict:
        """
        递归转换为普通 Python 字典

        注意：这会反序列化所有字段，对于大型嵌套字典可能较慢
        """
        result = {}
        for key in self.keys():
            value = self[key]  # 通过 __getitem__ 获取（可能是 NestedDBDict）
            if isinstance(value, NestedDBDict):
                # 递归转换
                result[key] = value.to_dict()
            else:
                result[key] = value
        return result

    def copy(self) -> dict:
        """
        创建浅拷贝（返回普通字典）

        等同于 to_dict()，符合 dict.copy() 的习惯用法
        """
        return self.to_dict()

    def __eq__(self, other) -> bool:
        """
        相等比较

        支持与普通 dict 或其他 NestedDBDict 比较
        """
        if isinstance(other, NestedDBDict):
            # 与另一个 NestedDBDict 比较
            return self.to_dict() == other.to_dict()
        elif isinstance(other, dict):
            # 与普通 dict 比较
            return self.to_dict() == other
        else:
            return False

    def __ne__(self, other) -> bool:
        """不等比较"""
        return not self.__eq__(other)

    def __or__(self, other) -> dict:
        """
        字典合并操作符 (|)

        返回一个新的普通字典，包含两个字典的合并结果
        如果有重复的键，使用 other 的值

        示例：
            result = nested_dict | {'new_key': 'value'}
        """
        result = self.to_dict()
        if isinstance(other, NestedDBDict):
            result.update(other.to_dict())
        elif isinstance(other, dict):
            result.update(other)
        else:
            return NotImplemented
        return result

    def __ior__(self, other):
        """
        字典合并赋值操作符 (|=)

        就地更新字典，相当于 update()

        示例：
            nested_dict |= {'new_key': 'value'}
        """
        self.update(other)
        return self

    def __repr__(self) -> str:
        """字符串表示"""
        # 计算总字段数
        total_count = len(self)

        # 只显示前几个字段，避免大型字典的性能问题
        items = []
        count = 0
        max_display = 5

        for key, value in self.items():
            if count >= max_display:
                remaining = total_count - max_display
                items.append(f"... ({remaining} more)")
                break
            items.append(f"{key!r}: {value!r}")
            count += 1

        items_str = ", ".join(items)
        return f"NestedDBDict({{{items_str}}})"

    def __str__(self) -> str:
        """字符串表示"""
        return self.__repr__()
