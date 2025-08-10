"""
混合序列化器单元测试
"""

import pytest
from unittest.mock import patch

from flaxkv.core.serializer import (
    HybridSerializer, get_serializer, encode_value, decode_value,
    get_serialization_stats, test_pandas_support, test_numpy_support,
    SerializationMethod
)


class TestHybridSerializer:
    """测试混合序列化器。"""
    
    def test_basic_data_types(self):
        """测试基本数据类型。"""
        serializer = HybridSerializer()
        
        test_values = [
            "string",
            42,
            3.14159,
            True,
            False,
            None,
            [1, 2, 3, "four"],
            {"nested": {"data": [1, 2, 3]}},
            (1, 2, "tuple"),
        ]
        
        for original_value in test_values:
            # 序列化和反序列化
            encoded = serializer.encode(original_value)
            decoded = serializer.decode(encoded)
            
            # 验证数据正确性
            assert decoded == original_value
            assert type(decoded) == type(original_value)
            
            # 验证包含方法标记
            assert len(encoded) >= 2
            method = encoded[0]
            assert method in [SerializationMethod.MSGSPEC, SerializationMethod.PICKLE]
    
    def test_complex_data_structures(self):
        """测试复杂数据结构。"""
        serializer = HybridSerializer()
        
        # 创建复杂嵌套结构
        complex_data = {
            "users": [
                {"id": 1, "name": "Alice", "scores": [95.5, 87.2, 92.0]},
                {"id": 2, "name": "Bob", "scores": [88.1, 91.5, 89.7]}
            ],
            "metadata": {
                "version": "2.0",
                "created": "2025-01-01",
                "config": {
                    "debug": True,
                    "max_users": 1000,
                    "features": ["auth", "cache", "backup"]
                }
            },
            "stats": {
                "total_operations": 12345,
                "cache_hit_rate": 0.857,
                "errors": None
            }
        }
        
        encoded = serializer.encode(complex_data)
        decoded = serializer.decode(encoded)
        
        assert decoded == complex_data
    
    def test_pandas_dataframe(self):
        """测试pandas DataFrame支持。"""
        pytest.importorskip("pandas")
        import pandas as pd
        
        serializer = HybridSerializer()
        
        # 创建测试DataFrame
        df = pd.DataFrame({
            'A': [1, 2, 3, 4],
            'B': ['a', 'b', 'c', 'd'],
            'C': [1.1, 2.2, 3.3, 4.4],
            'D': [True, False, True, False]
        })
        
        encoded = serializer.encode(df)
        decoded = serializer.decode(encoded)
        
        # 验证DataFrame相等
        assert isinstance(decoded, pd.DataFrame)
        assert df.equals(decoded)
        
        # 应该使用pickle方法
        assert encoded[0] == SerializationMethod.PICKLE
    
    def test_pandas_series(self):
        """测试pandas Series支持。"""
        pytest.importorskip("pandas")
        import pandas as pd
        
        serializer = HybridSerializer()
        
        series = pd.Series([1, 2, 3, 4, 5], name="test_series")
        
        encoded = serializer.encode(series)
        decoded = serializer.decode(encoded)
        
        assert isinstance(decoded, pd.Series)
        assert series.equals(decoded)
        assert decoded.name == "test_series"
    
    def test_numpy_arrays(self):
        """测试numpy数组支持。"""
        pytest.importorskip("numpy")
        import numpy as np
        
        serializer = HybridSerializer()
        
        # 1D数组
        arr_1d = np.array([1, 2, 3, 4, 5])
        encoded = serializer.encode(arr_1d)
        decoded = serializer.decode(encoded)
        assert np.array_equal(arr_1d, decoded)
        
        # 2D数组
        arr_2d = np.array([[1, 2, 3], [4, 5, 6]])
        encoded = serializer.encode(arr_2d)
        decoded = serializer.decode(encoded)
        assert np.array_equal(arr_2d, decoded)
        
        # 不同数据类型
        arr_float = np.array([1.1, 2.2, 3.3], dtype=np.float64)
        encoded = serializer.encode(arr_float)
        decoded = serializer.decode(encoded)
        assert np.array_equal(arr_float, decoded)
        assert decoded.dtype == arr_float.dtype
    
    def test_method_selection(self):
        """测试序列化方法选择。"""
        serializer = HybridSerializer()
        
        # 基本类型应该使用msgspec（如果可用）
        basic_value = {"key": "value"}
        method = serializer.get_method_for_value(basic_value)
        
        # 如果msgspec可用，应该选择msgspec
        if serializer.msgspec_encoder is not None:
            assert method == SerializationMethod.MSGSPEC
        else:
            assert method == SerializationMethod.PICKLE
    
    def test_msgspec_fallback(self):
        """测试msgspec不可用时的回退。"""
        # 模拟msgspec不可用
        with patch('flaxkv.core.serializer.MSGSPEC_AVAILABLE', False):
            serializer = HybridSerializer()
            
            assert serializer.msgspec_encoder is None
            assert serializer.msgspec_decoder is None
            
            # 所有值都应该使用pickle
            test_value = {"test": "value"}
            encoded = serializer.encode(test_value)
            decoded = serializer.decode(encoded)
            
            assert decoded == test_value
            assert encoded[0] == SerializationMethod.PICKLE
    
    def test_error_handling(self):
        """测试错误处理。"""
        serializer = HybridSerializer()
        
        # 测试解码空数据
        with pytest.raises(ValueError, match="数据太短"):
            serializer.decode(b"")
        
        # 测试解码单字节数据
        with pytest.raises(ValueError, match="数据太短"):
            serializer.decode(b"x")
        
        # 测试未知序列化方法
        with pytest.raises(ValueError, match="未知的序列化方法标记"):
            serializer.decode(b"\x99some_data")
        
        # 测试损坏的数据
        with pytest.raises(ValueError):
            serializer.decode(bytes([SerializationMethod.PICKLE]) + b"corrupted")
    
    def test_get_stats(self):
        """测试获取统计信息。"""
        serializer = HybridSerializer()
        stats = serializer.get_stats()
        
        assert isinstance(stats, dict)
        assert 'msgspec_available' in stats
        assert 'supports_pandas' in stats
        assert 'supports_numpy' in stats
        assert 'high_performance_types' in stats
        assert 'fallback_types' in stats
        
        assert stats['supports_pandas'] is True
        assert stats['supports_numpy'] is True


class TestGlobalFunctions:
    """测试全局函数。"""
    
    def test_global_serializer_singleton(self):
        """测试全局序列化器单例。"""
        serializer1 = get_serializer()
        serializer2 = get_serializer()
        
        # 应该是同一个实例
        assert serializer1 is serializer2
    
    def test_convenience_functions(self):
        """测试便利函数。"""
        test_data = {"convenience": "test"}
        
        # 使用便利函数
        encoded = encode_value(test_data)
        decoded = decode_value(encoded)
        
        assert decoded == test_data
    
    def test_get_serialization_stats(self):
        """测试获取序列化统计。"""
        stats = get_serialization_stats()
        
        assert isinstance(stats, dict)
        assert 'msgspec_available' in stats


class TestCompatibilityTests:
    """测试兼容性测试函数。"""
    
    def test_pandas_support_test(self):
        """测试pandas支持检测。"""
        try:
            import pandas as pd  # noqa: F401
            # pandas可用，应该返回True
            assert test_pandas_support() is True
        except ImportError:
            # pandas不可用，应该返回False
            assert test_pandas_support() is False
    
    def test_numpy_support_test(self):
        """测试numpy支持检测。"""
        try:
            import numpy as np  # noqa: F401
            # numpy可用，应该返回True
            assert test_numpy_support() is True
        except ImportError:
            # numpy不可用，应该返回False
            assert test_numpy_support() is False
    
    def test_pandas_support_with_mock_import_error(self):
        """测试模拟pandas不可用的情况。"""
        with patch('flaxkv.core.serializer.pd', side_effect=ImportError):
            assert test_pandas_support() is False
    
    def test_numpy_support_with_mock_import_error(self):
        """测试模拟numpy不可用的情况。"""
        with patch('flaxkv.core.serializer.np', side_effect=ImportError):
            assert test_numpy_support() is False


class TestRealWorldScenarios:
    """测试真实世界场景。"""
    
    def test_mixed_data_batch(self):
        """测试混合数据批处理。"""
        serializer = HybridSerializer()
        
        # 创建包含各种数据类型的批次
        batch_data = {
            "user_profile": {
                "id": 12345,
                "name": "John Doe",
                "preferences": ["dark_mode", "notifications"],
                "scores": [95.5, 87.2, 92.0]
            },
            "session_data": {
                "start_time": "2025-01-01T10:00:00Z",
                "actions": [
                    {"type": "login", "timestamp": 1609459200},
                    {"type": "view", "page": "dashboard", "timestamp": 1609459260},
                    {"type": "click", "element": "button", "timestamp": 1609459300}
                ]
            },
            "metrics": {
                "cpu_usage": 45.7,
                "memory_usage": 78.2,
                "disk_usage": 23.1,
                "network_io": {"in": 1024, "out": 2048}
            }
        }
        
        # 序列化整个批次
        encoded = serializer.encode(batch_data)
        decoded = serializer.decode(encoded)
        
        assert decoded == batch_data
        
        # 验证嵌套结构完整性
        assert decoded["user_profile"]["preferences"] == ["dark_mode", "notifications"]
        assert decoded["session_data"]["actions"][1]["page"] == "dashboard"
        assert decoded["metrics"]["network_io"]["out"] == 2048
    
    def test_dataframe_integration(self):
        """测试DataFrame集成场景。"""
        pytest.importorskip("pandas")
        import pandas as pd
        
        serializer = HybridSerializer()
        
        # 创建真实的数据分析场景
        sales_data = pd.DataFrame({
            'date': pd.date_range('2025-01-01', periods=100, freq='D'),
            'product': ['A', 'B', 'C'] * 33 + ['A'],
            'sales': [100 + i * 2 + (i % 7) * 5 for i in range(100)],
            'profit': [20 + i * 0.5 + (i % 5) * 2 for i in range(100)]
        })
        
        # 添加计算列
        sales_data['profit_margin'] = sales_data['profit'] / sales_data['sales']
        
        # 包装在更大的数据结构中
        analytics_data = {
            "report_id": "SALES_2025_Q1",
            "generated_at": "2025-01-01T12:00:00Z",
            "data": sales_data,
            "summary": {
                "total_sales": int(sales_data['sales'].sum()),
                "avg_profit": float(sales_data['profit'].mean()),
                "top_product": sales_data.groupby('product')['sales'].sum().idxmax()
            }
        }
        
        # 序列化和反序列化
        encoded = serializer.encode(analytics_data)
        decoded = serializer.decode(encoded)
        
        # 验证结构完整性
        assert decoded["report_id"] == "SALES_2025_Q1"
        assert isinstance(decoded["data"], pd.DataFrame)
        assert decoded["data"].equals(sales_data)
        assert decoded["summary"]["total_sales"] == int(sales_data['sales'].sum())
    
    def test_large_data_handling(self):
        """测试大数据处理。"""
        serializer = HybridSerializer()
        
        # 创建大数据集
        large_data = {
            "metadata": {"size": "large", "version": "1.0"},
            "data": {
                f"item_{i}": {
                    "id": i,
                    "value": f"data_{'x' * 100}_{i}",  # 100字符字符串
                    "scores": [j * 0.1 for j in range(10)],
                    "active": i % 2 == 0
                }
                for i in range(1000)  # 1000个项目
            }
        }
        
        # 序列化和反序列化
        encoded = serializer.encode(large_data)
        decoded = serializer.decode(encoded)
        
        # 验证完整性
        assert decoded["metadata"]["size"] == "large"
        assert len(decoded["data"]) == 1000
        assert decoded["data"]["item_500"]["id"] == 500
        assert len(decoded["data"]["item_999"]["value"]) > 100
    
    def test_backward_compatibility(self):
        """测试向后兼容性。"""
        serializer = HybridSerializer()
        
        # 创建各种可能在旧版本中存储的数据
        legacy_data = [
            # 基本类型
            "legacy_string",
            42,
            {"legacy": "dict"},
            
            # 列表和嵌套结构
            [1, 2, {"nested": "value"}],
            
            # 可能的类实例（通过pickle序列化）
            complex(1, 2),
        ]
        
        for data in legacy_data:
            encoded = serializer.encode(data)
            decoded = serializer.decode(encoded)
            assert decoded == data