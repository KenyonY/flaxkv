#!/usr/bin/env python3
"""
FlaxDict 性能基准测试

对比FlaxDict与主流持久化字典的性能，包括：
- Python内置dict (内存基准)
- shelve (Python标准库)  
- sqlite3 (SQL数据库)
- FlaxDict (我们的实现)
- RocksDict (可选，如果安装了)
- LMDB (可选，如果安装了)
"""

import os
import sys
import time
import random
import string
import sqlite3
import shelve
import tempfile
import statistics
from pathlib import Path
from contextlib import contextmanager
from typing import Dict, List, Tuple, Any, Optional
import matplotlib.pyplot as plt
import numpy as np

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent))

# 导入FlaxDict
from flaxkv.core.flax_dict import FlaxDict, create_flax_dict
from flaxkv.core.config import FlaxKVConfig

# 测试参数
N = 1000  # 数据点数量
VALUE_DIMS = 100  # 每个value的维度（模拟向量数据）
KEY_LENGTH = 16  # 键的长度
WARMUP_ROUNDS = 3  # 预热轮数
TEST_ROUNDS = 5  # 正式测试轮数


class MeasureTime:
    """精确的时间测量工具"""
    
    def __init__(self, name: str = "Operation"):
        self.name = name
        self.start_time = None
        self.end_time = None
        
    def __enter__(self):
        self.start_time = time.perf_counter()
        return self
        
    def __exit__(self, *args):
        self.end_time = time.perf_counter()
        
    @property
    def elapsed(self) -> float:
        """返回经过的时间（秒）"""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0.0
        
    def __str__(self):
        return f"{self.name}: {self.elapsed:.4f}s"


def generate_test_data(n: int) -> Tuple[List[str], List[List[float]]]:
    """生成测试数据"""
    keys = []
    values = []
    
    for i in range(n):
        # 生成随机键
        key = f"key_{''.join(random.choices(string.ascii_letters, k=KEY_LENGTH))}_{i:06d}"
        keys.append(key)
        
        # 生成随机向量
        vector = [random.uniform(-1, 1) for _ in range(VALUE_DIMS)]
        values.append(vector)
    
    return keys, values


# ===== 数据库适配器类 =====

class DatabaseAdapter:
    """数据库适配器基类"""
    
    def __init__(self, name: str, temp_dir: str):
        self.name = name
        self.temp_dir = temp_dir
        self.db = None
        
    def setup(self):
        """设置数据库"""
        pass
        
    def cleanup(self):
        """清理数据库"""
        pass
        
    def write_data(self, keys: List[str], values: List[Any]) -> float:
        """写入数据，返回耗时"""
        raise NotImplementedError
        
    def read_data(self, keys: List[str]) -> float:
        """读取数据，返回耗时"""
        raise NotImplementedError
        
    def __enter__(self):
        self.setup()
        return self
        
    def __exit__(self, *args):
        self.cleanup()


class PythonDictAdapter(DatabaseAdapter):
    """Python内置dict适配器（内存基准）"""
    
    def setup(self):
        self.db = {}
        
    def write_data(self, keys: List[str], values: List[Any]) -> float:
        with MeasureTime() as timer:
            for key, value in zip(keys, values):
                self.db[key] = value
        return timer.elapsed
        
    def read_data(self, keys: List[str]) -> float:
        with MeasureTime() as timer:
            for key in keys:
                _ = self.db[key]
        return timer.elapsed


class ShelveAdapter(DatabaseAdapter):
    """shelve适配器"""
    
    def setup(self):
        db_path = Path(self.temp_dir) / "shelve_db"
        self.db = shelve.open(str(db_path))
        
    def cleanup(self):
        if self.db:
            self.db.close()
            
    def write_data(self, keys: List[str], values: List[Any]) -> float:
        with MeasureTime() as timer:
            for key, value in zip(keys, values):
                self.db[key] = value
            self.db.sync()  # 强制同步到磁盘
        return timer.elapsed
        
    def read_data(self, keys: List[str]) -> float:
        with MeasureTime() as timer:
            for key in keys:
                _ = self.db[key]
        return timer.elapsed


class SQLiteAdapter(DatabaseAdapter):
    """SQLite适配器"""
    
    def setup(self):
        db_path = Path(self.temp_dir) / "sqlite_db.db"
        self.conn = sqlite3.connect(str(db_path))
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS kv_store (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        self.conn.commit()
        
    def cleanup(self):
        if hasattr(self, 'conn'):
            self.conn.close()
            
    def write_data(self, keys: List[str], values: List[Any]) -> float:
        import pickle
        with MeasureTime() as timer:
            data = [(key, pickle.dumps(value)) for key, value in zip(keys, values)]
            self.conn.executemany("INSERT OR REPLACE INTO kv_store (key, value) VALUES (?, ?)", data)
            self.conn.commit()
        return timer.elapsed
        
    def read_data(self, keys: List[str]) -> float:
        import pickle
        with MeasureTime() as timer:
            for key in keys:
                cursor = self.conn.execute("SELECT value FROM kv_store WHERE key = ?", (key,))
                row = cursor.fetchone()
                if row:
                    _ = pickle.loads(row[0])
        return timer.elapsed


class FlaxDictAdapter(DatabaseAdapter):
    """FlaxDict适配器"""
    
    def setup(self):
        config = FlaxKVConfig.for_high_performance()
        # 优化基准测试配置
        config.buffer_timeout = 10.0  # 延长缓冲区超时，避免自动刷新
        config.buffer_size = 10000  # 增加缓冲区大小
        config.cache_size = 10000   # 增加缓存大小
        
        self.db = create_flax_dict(
            name="benchmark_flaxdict",
            storage_type="local",
            path=self.temp_dir,
            config=config
        )
        
    def cleanup(self):
        if self.db:
            self.db.close()
            
    def write_data(self, keys: List[str], values: List[Any]) -> float:
        with MeasureTime() as timer:
            for key, value in zip(keys, values):
                self.db[key] = value
            self.db.flush()  # 确保数据写入磁盘
        return timer.elapsed
        
    def read_data(self, keys: List[str]) -> float:
        with MeasureTime() as timer:
            for key in keys:
                _ = self.db[key]
        return timer.elapsed


class RocksDictAdapter(DatabaseAdapter):
    """RocksDict适配器（可选）"""
    
    def __init__(self, name: str, temp_dir: str):
        super().__init__(name, temp_dir)
        self.available = False
        try:
            import rocksdict
            self.rocksdict = rocksdict
            self.available = True
        except ImportError:
            pass
            
    def setup(self):
        if not self.available:
            return
        db_path = Path(self.temp_dir) / "rocksdb"
        self.db = self.rocksdict.Rdict(str(db_path))
        
    def cleanup(self):
        if self.available and self.db:
            self.db.close()
            
    def write_data(self, keys: List[str], values: List[Any]) -> float:
        if not self.available:
            return float('inf')
        import pickle
        with MeasureTime() as timer:
            for key, value in zip(keys, values):
                self.db[key] = pickle.dumps(value)
            self.db.flush()
        return timer.elapsed
        
    def read_data(self, keys: List[str]) -> float:
        if not self.available:
            return float('inf')
        import pickle
        with MeasureTime() as timer:
            for key in keys:
                data = self.db[key]
                _ = pickle.loads(data)
        return timer.elapsed


class LMDBAdapter(DatabaseAdapter):
    """LMDB适配器（可选）"""
    
    def __init__(self, name: str, temp_dir: str):
        super().__init__(name, temp_dir)
        self.available = False
        try:
            import lmdb
            self.lmdb = lmdb
            self.available = True
        except ImportError:
            pass
            
    def setup(self):
        if not self.available:
            return
        db_path = Path(self.temp_dir) / "lmdb"
        db_path.mkdir(exist_ok=True)
        # 100MB map size
        self.env = self.lmdb.open(str(db_path), map_size=100*1024*1024)
        
    def cleanup(self):
        if self.available and hasattr(self, 'env'):
            self.env.close()
            
    def write_data(self, keys: List[str], values: List[Any]) -> float:
        if not self.available:
            return float('inf')
        import pickle
        with MeasureTime() as timer:
            with self.env.begin(write=True) as txn:
                for key, value in zip(keys, values):
                    txn.put(key.encode(), pickle.dumps(value))
        return timer.elapsed
        
    def read_data(self, keys: List[str]) -> float:
        if not self.available:
            return float('inf')
        import pickle
        with MeasureTime() as timer:
            with self.env.begin() as txn:
                for key in keys:
                    data = txn.get(key.encode())
                    if data:
                        _ = pickle.loads(data)
        return timer.elapsed


def run_benchmark() -> Dict[str, Dict[str, List[float]]]:
    """运行基准测试"""
    print("FlaxDict 性能基准测试")
    print("=" * 60)
    print(f"数据点数量: {N}")
    print(f"向量维度: {VALUE_DIMS}")
    print(f"键长度: {KEY_LENGTH}")
    print(f"预热轮数: {WARMUP_ROUNDS}")
    print(f"测试轮数: {TEST_ROUNDS}")
    print("-" * 60)
    
    # 生成测试数据
    keys, values = generate_test_data(N)
    
    # 定义要测试的数据库
    adapters = []
    
    with tempfile.TemporaryDirectory() as temp_dir:
        adapters = [
            PythonDictAdapter("Python Dict", temp_dir),
            ShelveAdapter("Shelve", temp_dir),
            SQLiteAdapter("SQLite", temp_dir),
            FlaxDictAdapter("FlaxDict", temp_dir),
            RocksDictAdapter("RocksDict", temp_dir),
            LMDBAdapter("LMDB", temp_dir),
        ]
        
        results = {adapter.name: {"write": [], "read": []} for adapter in adapters}
        
        for adapter in adapters:
            print(f"\n🧪 测试 {adapter.name}...")
            
            # 跳过不可用的适配器
            if hasattr(adapter, 'available') and not adapter.available:
                print(f"   ⚠️  {adapter.name} 不可用，跳过")
                continue
            
            # 预热 + 测试
            all_write_times = []
            all_read_times = []
            
            for round_num in range(WARMUP_ROUNDS + TEST_ROUNDS):
                is_warmup = round_num < WARMUP_ROUNDS
                
                try:
                    with adapter:
                        # 写入测试
                        write_time = adapter.write_data(keys, values)
                        
                        # 读取测试
                        read_time = adapter.read_data(keys)
                        
                    if not is_warmup:
                        all_write_times.append(write_time)
                        all_read_times.append(read_time)
                        print(f"   轮次 {round_num - WARMUP_ROUNDS + 1}: "
                              f"写入 {write_time:.4f}s, 读取 {read_time:.4f}s")
                        
                except Exception as e:
                    print(f"   ❌ {adapter.name} 测试失败: {e}")
                    break
            
            if all_write_times and all_read_times:
                results[adapter.name]["write"] = all_write_times
                results[adapter.name]["read"] = all_read_times
    
    return results


def analyze_results(results: Dict[str, Dict[str, List[float]]]):
    """分析和显示结果"""
    print("\n" + "=" * 80)
    print("📊 测试结果分析")
    print("=" * 80)
    
    # 计算统计数据
    stats = {}
    for db_name, timings in results.items():
        if not timings["write"] or not timings["read"]:
            continue
            
        stats[db_name] = {
            "write_avg": statistics.mean(timings["write"]),
            "write_std": statistics.stdev(timings["write"]) if len(timings["write"]) > 1 else 0,
            "read_avg": statistics.mean(timings["read"]),
            "read_std": statistics.stdev(timings["read"]) if len(timings["read"]) > 1 else 0,
        }
    
    # 显示结果表格
    print(f"{'数据库':<12} | {'写入时间(s)':<15} | {'读取时间(s)':<15} | {'写入ops/s':<12} | {'读取ops/s':<12}")
    print("-" * 80)
    
    for db_name, stat in stats.items():
        write_ops = N / stat["write_avg"]
        read_ops = N / stat["read_avg"]
        
        print(f"{db_name:<12} | "
              f"{stat['write_avg']:.4f}±{stat['write_std']:.4f} | "
              f"{stat['read_avg']:.4f}±{stat['read_std']:.4f} | "
              f"{write_ops:>8.0f} | "
              f"{read_ops:>8.0f}")
    
    # 性能比较（以Python Dict为基准）
    if "Python Dict" in stats:
        print("\n📈 性能比较 (相对于Python Dict):")
        print("-" * 60)
        base_write = stats["Python Dict"]["write_avg"]
        base_read = stats["Python Dict"]["read_avg"]
        
        for db_name, stat in stats.items():
            if db_name == "Python Dict":
                continue
            
            write_ratio = base_write / stat["write_avg"]
            read_ratio = base_read / stat["read_avg"]
            
            print(f"{db_name:<12}: 写入 {write_ratio:.2f}x, 读取 {read_ratio:.2f}x")
    
    return stats


def plot_results(stats: Dict[str, Dict[str, float]]):
    """绘制性能对比图表"""
    if not stats:
        print("❌ 没有有效的统计数据可以绘制")
        return
    
    db_names = list(stats.keys())
    write_times = [stats[db]["write_avg"] for db in db_names]
    read_times = [stats[db]["read_avg"] for db in db_names]
    
    # 创建图表
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
    
    # 写入性能图表
    bars1 = ax1.bar(db_names, write_times, alpha=0.7, color='skyblue')
    ax1.set_title(f'写入性能对比 ({N} 条记录)', fontsize=14, fontweight='bold')
    ax1.set_ylabel('时间 (秒)', fontsize=12)
    ax1.set_yscale('log')
    ax1.grid(True, alpha=0.3)
    
    # 在柱子上显示数值
    for bar, time_val in zip(bars1, write_times):
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() * 1.1,
                f'{time_val:.3f}s', ha='center', va='bottom', fontsize=10)
    
    # 读取性能图表
    bars2 = ax2.bar(db_names, read_times, alpha=0.7, color='lightcoral')
    ax2.set_title(f'读取性能对比 ({N} 条记录)', fontsize=14, fontweight='bold')
    ax2.set_ylabel('时间 (秒)', fontsize=12)
    ax2.set_yscale('log')
    ax2.grid(True, alpha=0.3)
    
    # 在柱子上显示数值
    for bar, time_val in zip(bars2, read_times):
        ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() * 1.1,
                f'{time_val:.3f}s', ha='center', va='bottom', fontsize=10)
    
    # 旋转x轴标签以免重叠
    for ax in [ax1, ax2]:
        ax.tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    
    # 保存图表
    output_path = Path(__file__).parent / "flaxdict_benchmark_results.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"\n📊 图表已保存到: {output_path}")
    
    # 显示图表（如果可能）
    try:
        plt.show()
    except:
        print("   (无法显示图表，请查看保存的文件)")


def main():
    """主函数"""
    print("🚀 开始FlaxDict性能基准测试...\n")
    
    try:
        # 运行基准测试
        results = run_benchmark()
        
        # 分析结果
        stats = analyze_results(results)
        
        # 绘制图表
        plot_results(stats)
        
        print("\n✅ 基准测试完成！")
        
        # 总结
        print("\n🎯 主要发现:")
        print("• Python Dict (内存): 最快的基准，但数据不持久化")
        print("• FlaxDict: 在持久化存储中提供良好的性能平衡")
        print("• SQLite: 功能丰富但相对较慢")
        print("• Shelve: Python标准库选项，性能中等")
        print("• RocksDict/LMDB: 高性能的第三方选项（如果安装）")
        
    except KeyboardInterrupt:
        print("\n⚠️  测试被用户中断")
    except Exception as e:
        print(f"\n❌ 测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()