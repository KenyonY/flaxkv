# FlaxKV2 性能测试工具

这个目录包含用于对FlaxKV2及其他数据库系统进行基准测试的工具。

## 功能特点

- 支持多种数据库后端测试比较
  - Python原生字典
  - FlaxKV2 LevelDB
  - FlaxKV2 Remote (网络服务)
  - SQLite
  - RocksDict
  - ShelveDict
- 测试读写性能
- 结果自动保存并可视化
- 可调整测试参数

## 安装依赖

```bash
pip install rocksdict sparrow-python pandas matplotlib pytest requests aiohttp
```

## 使用方法

运行基准测试脚本:

```bash
python -m benchmark.benchmark [参数]
```

### 可用参数

- `--n-items`: 测试项目数量 (默认: 1500)
- `--vector-dim`: 随机向量维度 (默认: 1000)
- `--repeat`: 每个测试重复次数 (默认: 3)
- `--db-types`: 要测试的数据库类型列表
  - 可选: dict, SQLite, RocksDict, Shelve, FlaxKV2-LevelDB, FlaxKV2-Remote
- `--output-dir`: 结果保存目录 (默认: benchmark_results)
- `--log-level`: 日志级别 (debug, info, warning, error, critical)
- `--no-cleanup`: 测试后不清理临时文件
- `--save-plot`: 保存图表到文件而非显示

### 示例

```bash
# 运行所有数据库后端的测试 (500项)
python -m benchmark.benchmark --n-items 500 --db-types dict SQLite RocksDict FlaxKV2-LevelDB FlaxKV2-Remote

# 只测试FlaxKV2的两种模式
python -m benchmark.benchmark --n-items 1000 --db-types FlaxKV2-LevelDB FlaxKV2-Remote --save-plot
```

## 结果分析

测试结果将保存在指定的输出目录中（默认为`benchmark_results`）:

- CSV格式的原始数据
- JSON格式的测试参数记录
- 可选的图表PNG文件

测试完成后会在控制台显示性能比较表格和可视化图表。

## 目录结构

- `benchmark.py`: 主入口点和命令行界面
- `run.py`: 测试执行和基准测试核心逻辑
- `dbclass.py`: 各种数据库包装类
- `helpers.py`: 辅助函数和工具

## 自定义测试

如需添加新的数据库后端进行测试，请修改`dbclass.py`文件添加相应的包装类，并确保它提供了一致的字典接口。 