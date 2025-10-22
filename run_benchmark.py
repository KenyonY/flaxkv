#!/usr/bin/env python
"""
快速运行基准测试脚本
"""

import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from benchmarks.benchmark_buffering import main

if __name__ == "__main__":
    main()
