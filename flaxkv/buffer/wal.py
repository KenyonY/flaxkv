"""
FlaxKV 2.0 写前日志 (WAL) 实现

提供写前日志功能，确保数据不丢失，即使在系统故障时也能恢复。
"""

import os
import time
import pickle
import threading
import logging
from typing import Any, Dict, List
from pathlib import Path

from ..core.config import FlaxKVConfig

logger = logging.getLogger(__name__)


class WriteAheadLog:
    """写前日志（WAL）实现。
    
    确保数据不丢失，即使在系统故障时也能恢复。
    """
    
    def __init__(self, wal_path: str, config: FlaxKVConfig):
        self.wal_path = Path(wal_path)
        self.config = config
        self._lock = threading.Lock()
        self._wal_file = None
        self._sequence_number = 0
        
        # 确保WAL目录存在
        self.wal_path.parent.mkdir(parents=True, exist_ok=True)
    
    async def initialize(self) -> None:
        """初始化WAL。"""
        try:
            # 打开WAL文件（追加模式）
            self._wal_file = open(self.wal_path, 'ab')
            
            # 恢复序列号
            if self.wal_path.exists():
                self._sequence_number = self._get_last_sequence_number()
            
            logger.info(f"WAL初始化完成: {self.wal_path}")
            
        except Exception as e:
            logger.error(f"WAL初始化失败: {e}")
            raise
    
    async def append(self, key: str, value: Any, operation: str = "set") -> int:
        """追加WAL记录。
        
        Args:
            key: 键
            value: 值
            operation: 操作类型
            
        Returns:
            序列号
        """
        with self._lock:
            self._sequence_number += 1
            
            # 创建WAL记录
            record = {
                'sequence': self._sequence_number,
                'timestamp': time.time(),
                'key': key,
                'value': value,
                'operation': operation
            }
            
            try:
                # 序列化并写入
                record_data = pickle.dumps(record)
                record_size = len(record_data)
                
                # 写入记录大小（4字节）+ 记录数据
                self._wal_file.write(record_size.to_bytes(4, 'big'))
                self._wal_file.write(record_data)
                
                # 根据配置决定是否立即同步
                if self.config.sync_mode == 'sync':
                    self._wal_file.flush()
                    os.fsync(self._wal_file.fileno())
                
                return self._sequence_number
                
            except Exception as e:
                logger.error(f"WAL写入失败: {e}")
                raise
    
    async def checkpoint(self) -> None:
        """检查点操作，清空WAL。"""
        with self._lock:
            if self._wal_file:
                self._wal_file.close()
            
            # 创建新的WAL文件
            if self.wal_path.exists():
                # 备份旧的WAL
                backup_path = self.wal_path.with_suffix('.wal.backup')
                self.wal_path.rename(backup_path)
            
            self._wal_file = open(self.wal_path, 'ab')
            logger.debug("WAL检查点完成")
    
    def _get_last_sequence_number(self) -> int:
        """获取最后的序列号。"""
        if not self.wal_path.exists():
            return 0
        
        try:
            last_seq = 0
            with open(self.wal_path, 'rb') as f:
                while True:
                    # 读取记录大小
                    size_data = f.read(4)
                    if not size_data:
                        break
                    
                    record_size = int.from_bytes(size_data, 'big')
                    
                    # 读取记录数据
                    record_data = f.read(record_size)
                    if len(record_data) != record_size:
                        break
                    
                    record = pickle.loads(record_data)
                    last_seq = max(last_seq, record.get('sequence', 0))
            
            return last_seq
            
        except Exception as e:
            logger.error(f"读取WAL序列号失败: {e}")
            return 0
    
    async def recover(self) -> List[Dict[str, Any]]:
        """恢复WAL记录。
        
        Returns:
            WAL记录列表
        """
        if not self.wal_path.exists():
            return []
        
        records = []
        try:
            with open(self.wal_path, 'rb') as f:
                while True:
                    # 读取记录大小
                    size_data = f.read(4)
                    if not size_data:
                        break
                    
                    record_size = int.from_bytes(size_data, 'big')
                    
                    # 读取记录数据
                    record_data = f.read(record_size)
                    if len(record_data) != record_size:
                        logger.warning("WAL记录不完整，停止恢复")
                        break
                    
                    record = pickle.loads(record_data)
                    records.append(record)
            
            logger.info(f"WAL恢复完成，共{len(records)}条记录")
            return records
            
        except Exception as e:
            logger.error(f"WAL恢复失败: {e}")
            return []
    
    async def close(self) -> None:
        """关闭WAL。"""
        with self._lock:
            if self._wal_file:
                try:
                    self._wal_file.close()
                    logger.info("WAL已关闭")
                except Exception as e:
                    logger.error(f"关闭WAL失败: {e}")
                finally:
                    self._wal_file = None


class WALCompressionMixin:
    """WAL压缩混入类。
    
    为WAL提供压缩功能以减少磁盘占用。
    """
    
    def __init__(self, enable_compression: bool = False):
        self.enable_compression = enable_compression
    
    def _compress_record(self, record_data: bytes) -> bytes:
        """压缩WAL记录数据。"""
        if not self.enable_compression:
            return record_data
        
        try:
            import gzip
            return gzip.compress(record_data)
        except ImportError:
            logger.warning("gzip模块不可用，禁用压缩")
            self.enable_compression = False
            return record_data
    
    def _decompress_record(self, compressed_data: bytes) -> bytes:
        """解压WAL记录数据。"""
        if not self.enable_compression:
            return compressed_data
        
        try:
            import gzip
            return gzip.decompress(compressed_data)
        except ImportError:
            logger.warning("gzip模块不可用，无法解压")
            return compressed_data
        except Exception as e:
            logger.error(f"WAL记录解压失败: {e}")
            # 可能是未压缩的数据，直接返回
            return compressed_data


class CompressedWriteAheadLog(WriteAheadLog, WALCompressionMixin):
    """带压缩功能的WAL实现。"""
    
    def __init__(self, wal_path: str, config: FlaxKVConfig, enable_compression: bool = True):
        WriteAheadLog.__init__(self, wal_path, config)
        WALCompressionMixin.__init__(self, enable_compression)
    
    async def append(self, key: str, value: Any, operation: str = "set") -> int:
        """追加压缩的WAL记录。"""
        with self._lock:
            self._sequence_number += 1
            
            # 创建WAL记录
            record = {
                'sequence': self._sequence_number,
                'timestamp': time.time(),
                'key': key,
                'value': value,
                'operation': operation,
                'compressed': self.enable_compression
            }
            
            try:
                # 序列化并压缩
                record_data = pickle.dumps(record)
                if self.enable_compression:
                    record_data = self._compress_record(record_data)
                
                record_size = len(record_data)
                
                # 写入记录大小（4字节）+ 记录数据
                self._wal_file.write(record_size.to_bytes(4, 'big'))
                self._wal_file.write(record_data)
                
                # 根据配置决定是否立即同步
                if self.config.sync_mode == 'sync':
                    self._wal_file.flush()
                    os.fsync(self._wal_file.fileno())
                
                return self._sequence_number
                
            except Exception as e:
                logger.error(f"压缩WAL写入失败: {e}")
                raise
    
    async def recover(self) -> List[Dict[str, Any]]:
        """恢复压缩的WAL记录。"""
        if not self.wal_path.exists():
            return []
        
        records = []
        try:
            with open(self.wal_path, 'rb') as f:
                while True:
                    # 读取记录大小
                    size_data = f.read(4)
                    if not size_data:
                        break
                    
                    record_size = int.from_bytes(size_data, 'big')
                    
                    # 读取记录数据
                    record_data = f.read(record_size)
                    if len(record_data) != record_size:
                        logger.warning("压缩WAL记录不完整，停止恢复")
                        break
                    
                    # 尝试解压（如果需要）
                    try:
                        record = pickle.loads(record_data)
                        # 检查是否为压缩记录
                        if not record.get('compressed', False):
                            records.append(record)
                            continue
                    except:
                        # 可能是压缩数据，尝试解压
                        pass
                    
                    # 尝试解压缩
                    try:
                        decompressed_data = self._decompress_record(record_data)
                        record = pickle.loads(decompressed_data)
                        records.append(record)
                    except Exception as e:
                        logger.error(f"压缩WAL记录解析失败: {e}")
                        continue
            
            logger.info(f"压缩WAL恢复完成，共{len(records)}条记录")
            return records
            
        except Exception as e:
            logger.error(f"压缩WAL恢复失败: {e}")
            return []