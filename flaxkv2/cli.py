"""
FlaxKV2 命令行接口 (使用Fire实现)
"""

import os
import uvicorn
import fire
from rich import print

from flaxkv2 import __version__
from flaxkv2.utils.log import set_log_level


class FlaxKV2CLI:
    """FlaxKV2命令行工具"""

    def __init__(self):
        """初始化CLI"""
        pass

    def version(self):
        """显示版本信息"""
        return f"FlaxKV2 {__version__}"

    def run(self, host='0.0.0.0', port=8000, data_dir=None, log_level='INFO', debug=False):
        """
        运行FlaxKV2服务器
        
        Args:
            host: 监听主机名
            port: 监听端口
            data_dir: 数据目录
            log_level: 日志级别 (DEBUG, INFO, WARNING, ERROR)
            debug: 调试模式
        """
        # 设置环境变量
        if data_dir:
            os.environ["FLAXKV_DATA_DIR"] = data_dir
            
        if debug:
            os.environ["FLAXKV_DEBUG"] = "1"
            
        # 设置日志级别
        set_log_level(log_level.upper())
        
        # 显示API文档地址信息
        server_url = f"http://{host if host != '0.0.0.0' else 'localhost'}:{port}"
        print(f"\n[bold green]FlaxKV2服务器启动中...[/bold green]\n")
        print(f"API文档地址:")
        print(f"  - Swagger UI: [bold blue]{server_url}/schema/swagger[/bold blue]")
        print(f"  - Redoc:      [bold blue]{server_url}/schema/redoc[/bold blue]")
        print(f"  - Scalar:     [bold blue]{server_url}/schema/scalar[/bold blue]")
        print(f"  - Rapidoc:    [bold blue]{server_url}/schema/rapidoc[/bold blue]")
        print(f"  - Stoplight:  [bold blue]{server_url}/schema/stoplight[/bold blue]")
        print(f"  - Yaml:       [bold blue]{server_url}/schema/yaml[/bold blue]\n")
        
        # 启动服务器
        uvicorn.run(
            "flaxkv2.server.app:create_app",
            host=host,
            port=port,
            factory=True,
            log_level=log_level.lower()
        )


def main():
    """主函数"""
    fire.Fire(FlaxKV2CLI)


if __name__ == '__main__':
    main() 