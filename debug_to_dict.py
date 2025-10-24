"""调试 to_dict() 问题"""
import tempfile
import shutil
from flaxkv2 import LevelDBDict

tmpdir = tempfile.mkdtemp()
print(f"临时目录: {tmpdir}\n")

try:
    db = LevelDBDict('test', path=tmpdir, rebuild=True)

    # 写入多层嵌套
    db['config'] = {
        'database': {
            'host': 'localhost',
            'port': 5432
        },
        'cache': {
            'redis': 'url'
        }
    }

    config = db['config']
    print(f"config type: {type(config)}")
    print(f"config._prefix: {config._prefix}")

    # 查看 keys
    print(f"\nconfig.keys():")
    for key in config.keys():
        print(f"  {key}")

    # 查看 items
    print(f"\nconfig.items():")
    for key, value in config.items():
        print(f"  {key}: {value} (type: {type(value).__name__})")

    # 尝试 to_dict
    print(f"\nconfig.to_dict():")
    config_dict = config.to_dict()
    print(f"  {config_dict}")

    db.close()

finally:
    shutil.rmtree(tmpdir)
    print(f"\n已清理")
