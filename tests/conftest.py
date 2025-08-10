"""
测试配置和固定装置
"""

import pytest
import asyncio
import tempfile
import shutil
from pathlib import Path

import flaxkv



@pytest.fixture
def temp_db_path():
    """创建临时数据库路径。
    
    使用更安全的清理机制确保临时目录总是被删除。
    """
    temp_dir = tempfile.mkdtemp(prefix="flaxkv_test_")
    temp_path = Path(temp_dir)
    
    try:
        yield temp_path
    finally:
        # 强制清理，即使测试失败也要清理
        try:
            if temp_path.exists():
                shutil.rmtree(temp_path, ignore_errors=True)
                # 双重检查确保删除成功
                if temp_path.exists():
                    import os
                    import stat
                    # 改变权限后再次尝试删除
                    for root, dirs, files in os.walk(temp_path):
                        for d in dirs:
                            os.chmod(os.path.join(root, d), stat.S_IWRITE)
                        for f in files:
                            os.chmod(os.path.join(root, f), stat.S_IWRITE)
                    shutil.rmtree(temp_path, ignore_errors=True)
        except Exception as e:
            # 记录清理失败，但不影响测试结果
            print(f"Warning: Failed to cleanup temp directory {temp_path}: {e}")


@pytest.fixture
def test_config():
    """测试配置。"""
    return flaxkv.FlaxKVConfig.for_testing()


@pytest.fixture(scope="function")
async def flaxkv_db(temp_db_path, test_config):
    """创建测试用FlaxKV实例。"""
    db = flaxkv.FlaxKV("test_db", "local", temp_db_path, test_config)
    await db.initialize()
    yield db
    await db.close()


@pytest.fixture
async def sample_data():
    """测试数据样本。"""
    return {
        "str_key": "string_value",
        "int_key": 42,
        "float_key": 3.14,
        "list_key": [1, 2, 3, "four"],
        "dict_key": {"nested": "value", "number": 100},
        "bool_key": True,
        "none_key": None,
    }