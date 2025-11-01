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


class NestedDBList:
    """
    基于 LevelDB prefixed_db 的嵌套列表实现

    核心优势：
    - 修改单个元素只序列化该元素的值，而不是整个列表
    - 读取单个元素只反序列化该元素的值
    - 利用 LevelDB 的前缀查询能力高效迭代

    使用示例：
        db = LevelDBDict('mydb')

        # 创建嵌套列表
        nested = db.nested_list('items')

        # 像普通列表一样使用（但每个元素独立存储）
        nested.append('item1')
        nested.append('item2')
        nested.append('item3')

        # 高效修改单个元素（只序列化该元素的值）
        nested[1] = 'modified_item2'

        # 高效读取单个元素（只反序列化该元素的值）
        print(nested[1])

        # 迭代所有元素
        for item in nested:
            print(item)
    """

    def __init__(self, prefixed_db, prefix: str, parent_db=None, root_db=None):
        """
        初始化嵌套列表

        Args:
            prefixed_db: plyvel 的 PrefixedDB 对象
            prefix: 前缀字符串（用于日志和显示）
            parent_db: 父数据库对象（用于访问缓冲区）
            root_db: 根数据库对象（用于递归创建嵌套对象）
        """
        self._prefixed_db = prefixed_db
        self._prefix = prefix
        self._parent_db = parent_db
        self._root_db = root_db if root_db is not None else parent_db  # 根数据库
        self._lock = threading.RLock()
        self._length_key = b'__len__'

    def _get_length(self) -> int:
        """获取列表长度"""
        length_bytes = self._prefixed_db.get(self._length_key)
        if length_bytes is None:
            return 0
        return decoder.decode(length_bytes)

    def _set_length(self, length: int) -> None:
        """设置列表长度"""
        length_bytes = encoder.encode(length)
        self._prefixed_db.put(self._length_key, length_bytes)

    def _index_to_key(self, index: int) -> bytes:
        """将索引转换为存储键"""
        # 使用固定宽度的索引格式，确保排序正确
        # 例如：0 -> "0000000000", 1 -> "0000000001"
        return f"{index:010d}".encode('utf-8')

    def _key_to_index(self, key_bytes: bytes) -> int:
        """将存储键转换为索引"""
        if key_bytes == self._length_key:
            return -1  # 特殊键
        return int(key_bytes.decode('utf-8'))

    def __len__(self) -> int:
        """返回列表长度"""
        return self._get_length()

    def __getitem__(self, index: int) -> Any:
        """获取元素值"""
        if not isinstance(index, int):
            raise TypeError(f"List indices must be integers, not {type(index).__name__}")

        length = self._get_length()

        # 支持负索引
        if index < 0:
            index = length + index

        if index < 0 or index >= length:
            raise IndexError("list index out of range")

        # 读取元素
        key_bytes = self._index_to_key(index)
        value_bytes = self._prefixed_db.get(key_bytes)

        if value_bytes is None:
            raise IndexError(f"list index {index} not found in storage")

        # 检查是否是嵌套字典
        full_key = f"{self._prefix}{index}"
        if self._root_db is not None:
            try:
                is_nested_dict = self._root_db.get(f'__nested__:{full_key}')
                if is_nested_dict:
                    # 返回嵌套字典
                    sub_prefix = f"{full_key}:"
                    sub_prefix_bytes = sub_prefix.encode('utf-8')
                    sub_prefixed_db = self._root_db._db.prefixed_db(sub_prefix_bytes)
                    return NestedDBDict(sub_prefixed_db, sub_prefix,
                                       parent_db=None, root_db=self._root_db)
            except (KeyError, AttributeError):
                pass

            try:
                is_nested_list = self._root_db.get(f'__list__:{full_key}')
                if is_nested_list:
                    # 返回嵌套列表
                    sub_prefix = f"{full_key}:"
                    sub_prefix_bytes = sub_prefix.encode('utf-8')
                    sub_prefixed_db = self._root_db._db.prefixed_db(sub_prefix_bytes)
                    return NestedDBList(sub_prefixed_db, sub_prefix,
                                       parent_db=None, root_db=self._root_db)
            except (KeyError, AttributeError):
                pass

        # 反序列化单个值
        return decoder.decode(value_bytes)

    def __setitem__(self, index: int, value: Any) -> None:
        """设置元素值"""
        if not isinstance(index, int):
            raise TypeError(f"List indices must be integers, not {type(index).__name__}")

        length = self._get_length()

        # 支持负索引
        if index < 0:
            index = length + index

        if index < 0 or index >= length:
            raise IndexError("list assignment index out of range")

        full_key = f"{self._prefix}{index}"

        # 检测是否是字典类型
        if isinstance(value, dict):
            # 1. 标记为嵌套字典
            if self._root_db is not None:
                self._root_db[f'__nested__:{full_key}'] = True
                # 清除可能存在的列表标记
                try:
                    del self._root_db[f'__list__:{full_key}']
                except KeyError:
                    pass

            # 2. 创建嵌套字典
            sub_prefix = f"{full_key}:"
            sub_prefix_bytes = sub_prefix.encode('utf-8')
            if self._root_db is not None:
                sub_prefixed_db = self._root_db._db.prefixed_db(sub_prefix_bytes)
            else:
                raise RuntimeError("Cannot create nested dict without root_db")

            sub_nested = NestedDBDict(sub_prefixed_db, sub_prefix,
                                     parent_db=None, root_db=self._root_db)

            # 3. 清空旧数据并递归写入
            sub_nested.clear()
            for k, v in value.items():
                sub_nested[k] = v

            # 4. 在列表的 prefixed_db 中存储占位符（用于检测该索引是否存在）
            key_bytes = self._index_to_key(index)
            placeholder_bytes = encoder.encode(True)  # 存储一个布尔值作为占位符
            self._prefixed_db.put(key_bytes, placeholder_bytes)
        elif isinstance(value, list):
            # 1. 标记为嵌套列表
            if self._root_db is not None:
                self._root_db[f'__list__:{full_key}'] = True
                # 清除可能存在的字典标记
                try:
                    del self._root_db[f'__nested__:{full_key}']
                except KeyError:
                    pass

            # 2. 创建嵌套列表
            sub_prefix = f"{full_key}:"
            sub_prefix_bytes = sub_prefix.encode('utf-8')
            if self._root_db is not None:
                sub_prefixed_db = self._root_db._db.prefixed_db(sub_prefix_bytes)
            else:
                raise RuntimeError("Cannot create nested list without root_db")

            sub_nested = NestedDBList(sub_prefixed_db, sub_prefix,
                                     parent_db=None, root_db=self._root_db)

            # 3. 清空旧数据并递归写入
            sub_nested.clear()
            for item in value:
                sub_nested.append(item)

            # 4. 在列表的 prefixed_db 中存储占位符（用于检测该索引是否存在）
            key_bytes = self._index_to_key(index)
            placeholder_bytes = encoder.encode(True)  # 存储一个布尔值作为占位符
            self._prefixed_db.put(key_bytes, placeholder_bytes)
        else:
            # 非字典非列表：取消嵌套标记（如果有）
            if self._root_db is not None:
                try:
                    del self._root_db[f'__nested__:{full_key}']
                except KeyError:
                    pass
                try:
                    del self._root_db[f'__list__:{full_key}']
                except KeyError:
                    pass

            # 直接写入叶子值
            key_bytes = self._index_to_key(index)
            value_bytes = encoder.encode(value)
            self._prefixed_db.put(key_bytes, value_bytes)

    def __delitem__(self, index: int) -> None:
        """删除元素（通过移动后续元素实现）"""
        if not isinstance(index, int):
            raise TypeError(f"List indices must be integers, not {type(index).__name__}")

        length = self._get_length()

        # 支持负索引
        if index < 0:
            index = length + index

        if index < 0 or index >= length:
            raise IndexError("list index out of range")

        full_key = f"{self._prefix}{index}"

        # 检查是否是嵌套对象，如果是则递归删除
        if self._root_db is not None:
            try:
                is_nested_dict = self._root_db.get(f'__nested__:{full_key}')
                if is_nested_dict:
                    # 递归删除嵌套字典
                    sub_prefix = f"{full_key}:"
                    sub_prefix_bytes = sub_prefix.encode('utf-8')
                    sub_prefixed_db = self._root_db._db.prefixed_db(sub_prefix_bytes)
                    sub_nested = NestedDBDict(sub_prefixed_db, sub_prefix,
                                             parent_db=None, root_db=self._root_db)
                    sub_nested.clear()
                    del self._root_db[f'__nested__:{full_key}']
            except (KeyError, AttributeError):
                pass

            try:
                is_nested_list = self._root_db.get(f'__list__:{full_key}')
                if is_nested_list:
                    # 递归删除嵌套列表
                    sub_prefix = f"{full_key}:"
                    sub_prefix_bytes = sub_prefix.encode('utf-8')
                    sub_prefixed_db = self._root_db._db.prefixed_db(sub_prefix_bytes)
                    sub_nested = NestedDBList(sub_prefixed_db, sub_prefix,
                                             parent_db=None, root_db=self._root_db)
                    sub_nested.clear()
                    del self._root_db[f'__list__:{full_key}']
            except (KeyError, AttributeError):
                pass

        # 将后续元素前移
        for i in range(index, length - 1):
            next_key = self._index_to_key(i + 1)
            next_value = self._prefixed_db.get(next_key)
            if next_value is not None:
                current_key = self._index_to_key(i)
                self._prefixed_db.put(current_key, next_value)

                # 同时移动嵌套标记
                if self._root_db is not None:
                    next_full_key = f"{self._prefix}{i + 1}"
                    current_full_key = f"{self._prefix}{i}"

                    try:
                        is_nested_dict = self._root_db.get(f'__nested__:{next_full_key}')
                        if is_nested_dict:
                            self._root_db[f'__nested__:{current_full_key}'] = True
                            del self._root_db[f'__nested__:{next_full_key}']
                    except (KeyError, AttributeError):
                        pass

                    try:
                        is_nested_list = self._root_db.get(f'__list__:{next_full_key}')
                        if is_nested_list:
                            self._root_db[f'__list__:{current_full_key}'] = True
                            del self._root_db[f'__list__:{next_full_key}']
                    except (KeyError, AttributeError):
                        pass

        # 删除最后一个元素
        last_key = self._index_to_key(length - 1)
        self._prefixed_db.delete(last_key)

        # 更新长度
        self._set_length(length - 1)

    def insert(self, index: int, value: Any) -> None:
        """在指定位置插入元素"""
        length = self._get_length()

        # 支持负索引
        if index < 0:
            index = max(0, length + index)
        else:
            index = min(index, length)

        # 将后续元素后移
        for i in range(length - 1, index - 1, -1):
            current_key = self._index_to_key(i)
            current_value = self._prefixed_db.get(current_key)
            if current_value is not None:
                next_key = self._index_to_key(i + 1)
                self._prefixed_db.put(next_key, current_value)

                # 同时移动嵌套标记
                if self._root_db is not None:
                    current_full_key = f"{self._prefix}{i}"
                    next_full_key = f"{self._prefix}{i + 1}"

                    try:
                        is_nested_dict = self._root_db.get(f'__nested__:{current_full_key}')
                        if is_nested_dict:
                            self._root_db[f'__nested__:{next_full_key}'] = True
                            del self._root_db[f'__nested__:{current_full_key}']
                    except (KeyError, AttributeError):
                        pass

                    try:
                        is_nested_list = self._root_db.get(f'__list__:{current_full_key}')
                        if is_nested_list:
                            self._root_db[f'__list__:{next_full_key}'] = True
                            del self._root_db[f'__list__:{current_full_key}']
                    except (KeyError, AttributeError):
                        pass

        # 更新长度
        self._set_length(length + 1)

        # 设置新元素
        self[index] = value

    def append(self, value: Any) -> None:
        """在列表末尾添加元素"""
        length = self._get_length()
        self._set_length(length + 1)
        self[length] = value

    def extend(self, values) -> None:
        """扩展列表"""
        for value in values:
            self.append(value)

    def pop(self, index: int = -1) -> Any:
        """删除并返回指定位置的元素"""
        if self._get_length() == 0:
            raise IndexError("pop from empty list")

        value = self[index]
        del self[index]
        return value

    def clear(self) -> None:
        """清空列表"""
        length = self._get_length()

        # 删除所有元素（包括嵌套对象）
        for i in range(length):
            full_key = f"{self._prefix}{i}"

            # 删除嵌套标记
            if self._root_db is not None:
                try:
                    is_nested_dict = self._root_db.get(f'__nested__:{full_key}')
                    if is_nested_dict:
                        sub_prefix = f"{full_key}:"
                        sub_prefix_bytes = sub_prefix.encode('utf-8')
                        sub_prefixed_db = self._root_db._db.prefixed_db(sub_prefix_bytes)
                        sub_nested = NestedDBDict(sub_prefixed_db, sub_prefix,
                                                 parent_db=None, root_db=self._root_db)
                        sub_nested.clear()
                        del self._root_db[f'__nested__:{full_key}']
                except (KeyError, AttributeError):
                    pass

                try:
                    is_nested_list = self._root_db.get(f'__list__:{full_key}')
                    if is_nested_list:
                        sub_prefix = f"{full_key}:"
                        sub_prefix_bytes = sub_prefix.encode('utf-8')
                        sub_prefixed_db = self._root_db._db.prefixed_db(sub_prefix_bytes)
                        sub_nested = NestedDBList(sub_prefixed_db, sub_prefix,
                                                 parent_db=None, root_db=self._root_db)
                        sub_nested.clear()
                        del self._root_db[f'__list__:{full_key}']
                except (KeyError, AttributeError):
                    pass

            # 删除元素
            key_bytes = self._index_to_key(i)
            try:
                self._prefixed_db.delete(key_bytes)
            except:
                pass

        # 重置长度
        self._set_length(0)

    def __iter__(self):
        """迭代列表元素"""
        length = self._get_length()
        for i in range(length):
            yield self[i]

    def __contains__(self, value: Any) -> bool:
        """检查元素是否在列表中"""
        for item in self:
            if item == value:
                return True
        return False

    def index(self, value: Any, start: int = 0, stop: Optional[int] = None) -> int:
        """返回值的索引"""
        length = self._get_length()
        if stop is None:
            stop = length

        for i in range(start, min(stop, length)):
            if self[i] == value:
                return i

        raise ValueError(f"{value} is not in list")

    def count(self, value: Any) -> int:
        """计算值出现的次数"""
        count = 0
        for item in self:
            if item == value:
                count += 1
        return count

    def reverse(self) -> None:
        """反转列表"""
        length = self._get_length()
        for i in range(length // 2):
            j = length - 1 - i
            # 交换元素
            temp = self[i]
            self[i] = self[j]
            self[j] = temp

    def to_list(self) -> list:
        """
        递归转换为普通 Python 列表

        注意：这会反序列化所有元素，对于大型列表可能较慢
        """
        result = []
        for item in self:
            if isinstance(item, NestedDBList):
                result.append(item.to_list())
            elif isinstance(item, NestedDBDict):
                result.append(item.to_dict())
            else:
                result.append(item)
        return result

    def __eq__(self, other) -> bool:
        """相等比较"""
        if isinstance(other, NestedDBList):
            return self.to_list() == other.to_list()
        elif isinstance(other, list):
            return self.to_list() == other
        else:
            return False

    def __ne__(self, other) -> bool:
        """不等比较"""
        return not self.__eq__(other)

    def __repr__(self) -> str:
        """
        字符串表示

        提供简洁的表示，显示前几个元素和总长度
        """
        length = self._get_length()

        if length == 0:
            return "NestedDBList([])"

        # 只显示前几个元素，避免大型列表的性能问题
        items = []
        count = 0
        max_display = 5

        for i in range(length):
            if count >= max_display:
                remaining = length - max_display
                items.append(f"... ({remaining} more)")
                break

            # 获取元素
            item = self[i]

            # 对嵌套对象使用简化表示
            if isinstance(item, NestedDBDict):
                item_repr = f"NestedDBDict(...{len(item)} keys)"
            elif isinstance(item, NestedDBList):
                item_repr = f"NestedDBList(...{len(item)} items)"
            else:
                item_repr = repr(item)

            items.append(item_repr)
            count += 1

        items_str = ", ".join(items)
        return f"NestedDBList([{items_str}])"

    def __str__(self) -> str:
        """
        用户友好的字符串表示

        类似于普通 Python 列表的显示方式
        """
        length = self._get_length()

        if length == 0:
            return "[]"

        # 只显示前几个元素，避免大型列表的性能问题
        items = []
        count = 0
        max_display = 10  # str 模式下显示更多元素

        for i in range(length):
            if count >= max_display:
                remaining = length - max_display
                items.append(f"... and {remaining} more items")
                break

            # 获取元素
            item = self[i]

            # 对嵌套对象使用简化表示
            if isinstance(item, NestedDBDict):
                item_str = f"{{...{len(item)} keys}}"
            elif isinstance(item, NestedDBList):
                item_str = f"[...{len(item)} items]"
            else:
                item_str = repr(item)

            items.append(item_str)
            count += 1

        items_str = ", ".join(items)
        return f"[{items_str}]"
