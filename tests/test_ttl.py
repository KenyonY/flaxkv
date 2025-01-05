import os
import shutil
import time
from pathlib import Path

import pytest

from flaxkv import LevelDBDict


@pytest.fixture(params=["leveldb"])
def db(request):
    """Fixture that provides LevelDB instances for testing"""
    db_type = request.param
    test_dir = Path("test_db")
    if test_dir.exists():
        shutil.rmtree(test_dir)
    test_dir.mkdir()
    
    db = LevelDBDict("test", str(test_dir))
    
    yield db
    
    # 确保数据库正确关闭
    db.destroy()


def test_set_with_ttl(db):
    """Test setting keys with TTL"""
    # Set key with 1 second expiry
    db.set("key1", "value1", ex=1)
    assert "key1" in db
    assert db["key1"] == "value1"
    
    # Wait for expiry
    time.sleep(1.1)
    print(db)
    assert "key1" not in db
    
    # Set multiple keys with different TTLs
    db.set("key2", "value2", ex=2)
    db.set("key3", "value3", ex=0.5)
    
    assert "key2" in db and "key3" in db
    time.sleep(0.6)
    assert "key2" in db
    assert "key3" not in db


def test_expire(db):
    """Test expire method"""
    # Set key without TTL
    db["key1"] = "value1"
    assert "key1" in db
    
    # Add expiry
    assert db.expire("key1", 1) is True
    assert "key1" in db
    
    # Wait for expiry
    time.sleep(1.1)
    assert "key1" not in db
    
    # Try to expire non-existent key
    assert db.expire("non_existent", 1) is False


def test_ttl(db):
    """Test ttl method"""
    # Set key with TTL
    db.set("key1", "value1", ex=2)
    
    # Check TTL
    ttl = db.ttl("key1")
    assert ttl is not None
    assert 0 < ttl <= 2
    
    # Wait and check decreasing TTL
    time.sleep(0.5)
    new_ttl = db.ttl("key1")
    assert new_ttl < ttl
    
    # Check TTL for non-existent key
    assert db.ttl("non_existent") is None
    
    # Check TTL for key without expiry
    db["key2"] = "value2"
    assert db.ttl("key2") is None


def test_persist(db):
    """Test persist method"""
    # Set key with TTL
    db.set("key1", "value1", ex=2)
    assert db.ttl("key1") is not None
    
    # Remove expiry
    assert db.persist("key1") is True
    assert db.ttl("key1") is None
    
    # Key should still exist after original expiry time
    time.sleep(2.1)
    assert "key1" in db
    assert db["key1"] == "value1"
    
    # Try to persist non-existent key
    assert db.persist("non_existent") is False


def test_multiple_ttl_operations(db):
    """Test multiple TTL operations on the same key"""
    # Set key with initial TTL
    db.set("key1", "value1", ex=5)
    initial_ttl = db.ttl("key1")
    assert initial_ttl is not None
    
    # Update TTL to shorter time
    assert db.expire("key1", 1)
    new_ttl = db.ttl("key1")
    assert new_ttl is not None
    assert new_ttl < initial_ttl
    
    # Remove TTL
    assert db.persist("key1")
    assert db.ttl("key1") is None
    
    # Add TTL again
    assert db.expire("key1", 1)
    assert db.ttl("key1") is not None
    
    # Wait for expiry
    time.sleep(1.1)
    assert "key1" not in db


def test_ttl_with_updates(db):
    """Test TTL behavior when updating values"""
    # Set key with TTL
    db.set("key1", "value1", ex=2)
    
    # Update value without changing TTL
    db["key1"] = "new_value1"
    assert db.ttl("key1") is not None  # TTL should still exist
    assert db["key1"] == "new_value1"
    
    # Wait for expiry
    time.sleep(2.1)
    assert "key1" not in db


def test_bulk_ttl_operations(db):
    """Test TTL operations with multiple keys"""
    # Set multiple keys with different TTLs
    test_data = {
        "key1": (1, "value1"),
        "key2": (2, "value2"),
        "key3": (3, "value3"),
    }
    
    for key, (ttl, value) in test_data.items():
        db.set(key, value, ex=ttl)
    
    # Verify initial state
    for key in test_data:
        assert key in db
        assert db.ttl(key) is not None
    
    # Wait and check expiry order
    time.sleep(1.1)
    assert "key1" not in db
    assert "key2" in db
    assert "key3" in db
    
    time.sleep(1)
    assert "key2" not in db
    assert "key3" in db 