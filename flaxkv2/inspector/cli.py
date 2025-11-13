"""
FlaxKV2 Inspector CLI - 命令行工具
"""

import json
import sys
from typing import Optional

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich import box

from flaxkv2.inspector import Inspector


console = Console()


class InspectCommands:
    """Inspector 命令集合"""

    def __init__(self):
        """初始化 Inspect 命令"""
        pass

    def keys(
        self,
        db_name: str,
        path: str = '.',
        pattern: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        backend: str = 'auto'
    ):
        """
        列出数据库中的所有键

        Args:
            db_name: 数据库名称
            path: 数据库路径（本地路径或远程地址，如 '127.0.0.1:5555'）
            pattern: 正则表达式模式（可选）
            limit: 返回的最大数量（默认: 100）
            offset: 跳过的键数量（默认: 0）
            backend: 后端类型 ('local', 'remote', 'auto')，默认 'auto'

        示例:
            flaxkv2 inspect keys mydb --path /data
            flaxkv2 inspect keys mydb --path 127.0.0.1:5555 --backend remote
            flaxkv2 inspect keys mydb --pattern "user.*" --limit 50
        """
        try:
            with Inspector(db_name, path, backend=backend) as inspector:
                keys = inspector.list_keys(pattern=pattern, limit=limit, offset=offset)
                total = inspector.count_keys()

                if not keys:
                    console.print("[yellow]未找到任何键[/yellow]")
                    return

                # 创建表格
                table = Table(
                    title=f"数据库键列表: {db_name}",
                    show_header=True,
                    header_style="bold magenta",
                    box=box.ROUNDED
                )
                table.add_column("#", style="dim", width=6)
                table.add_column("键名", style="cyan")

                for idx, key in enumerate(keys, start=offset + 1):
                    table.add_row(str(idx), key)

                console.print(table)
                console.print(f"\n显示 {offset + 1}-{offset + len(keys)} / 共 {total} 个键")

                if pattern:
                    console.print(f"过滤条件: [blue]{pattern}[/blue]")

        except Exception as e:
            console.print(f"[bold red]错误:[/bold red] {str(e)}")
            sys.exit(1)

    def get(
        self,
        db_name: str,
        key: str,
        path: str = '.',
        backend: str = 'auto',
        show_value: bool = True
    ):
        """
        查看指定键的详细信息

        Args:
            db_name: 数据库名称
            key: 键名
            path: 数据库路径（本地路径或远程地址）
            backend: 后端类型 ('local', 'remote', 'auto')
            show_value: 是否显示值（默认: True）

        示例:
            flaxkv2 inspect get mydb user123
            flaxkv2 inspect get mydb config --path 127.0.0.1:5555 --backend remote
        """
        try:
            with Inspector(db_name, path, backend=backend) as inspector:
                info = inspector.get_value_info(key)

                if not info:
                    console.print(f"[bold red]键不存在:[/bold red] {key}")
                    return

                if 'error' in info:
                    console.print(f"[bold red]读取错误:[/bold red] {info['error']}")
                    return

                # 创建信息面板
                text = Text()
                text.append(f"键名: ", style="bold")
                text.append(f"{info['key']}\n", style="cyan")

                text.append(f"类型: ", style="bold")
                text.append(f"{info['type']}\n", style="green")

                text.append(f"大小: ", style="bold")
                text.append(f"{self._format_size(info['size'])}\n", style="yellow")

                if info.get('ttl') is not None:
                    text.append(f"TTL: ", style="bold")
                    ttl_style = "red" if info.get('expired') else "blue"
                    text.append(f"{info['ttl']:.2f}秒\n", style=ttl_style)

                    text.append(f"过期时间: ", style="bold")
                    text.append(f"{info['expires_at']}\n", style=ttl_style)

                    if info.get('expired'):
                        text.append("状态: ", style="bold")
                        text.append("已过期\n", style="red bold")

                panel = Panel(text, title="键信息", border_style="blue")
                console.print(panel)

                # 显示值
                if show_value and 'value' in info:
                    console.print("\n[bold]值:[/bold]")
                    if isinstance(info['value'], (dict, list)):
                        console.print_json(data=info['value'])
                    else:
                        console.print(info['value'])

        except Exception as e:
            console.print(f"[bold red]错误:[/bold red] {str(e)}")
            sys.exit(1)

    def stats(
        self,
        db_name: str,
        path: str = '.',
        backend: str = 'auto'
    ):
        """
        显示数据库统计信息

        Args:
            db_name: 数据库名称
            path: 数据库路径（本地路径或远程地址）
            backend: 后端类型 ('local', 'remote', 'auto')

        示例:
            flaxkv2 inspect stats mydb
            flaxkv2 inspect stats mydb --path /data/db
            flaxkv2 inspect stats mydb --path 127.0.0.1:5555 --backend remote
        """
        try:
            with Inspector(db_name, path, backend=backend) as inspector:
                console.print("[blue]正在收集统计信息...[/blue]")
                stats = inspector.get_stats()

                if 'error' in stats:
                    console.print(f"[bold red]错误:[/bold red] {stats['error']}")
                    return

                # 总览
                table1 = Table(title="总览", box=box.ROUNDED)
                table1.add_column("指标", style="cyan")
                table1.add_column("值", style="green")

                table1.add_row("总键数", str(stats['total_keys']))
                table1.add_row("总大小", self._format_size(stats['total_size']))

                console.print(table1)

                # 类型分布
                if stats['type_distribution']:
                    console.print()
                    table2 = Table(title="类型分布", box=box.ROUNDED)
                    table2.add_column("类型", style="cyan")
                    table2.add_column("数量", style="green")
                    table2.add_column("占比", style="yellow")

                    for type_name, count in sorted(
                        stats['type_distribution'].items(),
                        key=lambda x: x[1],
                        reverse=True
                    ):
                        percentage = (count / stats['total_keys'] * 100) if stats['total_keys'] > 0 else 0
                        table2.add_row(type_name, str(count), f"{percentage:.1f}%")

                    console.print(table2)

                # 大小分布
                console.print()
                table3 = Table(title="大小分布", box=box.ROUNDED)
                table3.add_column("分类", style="cyan")
                table3.add_column("数量", style="green")
                table3.add_column("占比", style="yellow")

                size_labels = {
                    'tiny': '极小 (< 1KB)',
                    'small': '小 (1KB - 10KB)',
                    'medium': '中 (10KB - 100KB)',
                    'large': '大 (100KB - 1MB)',
                    'huge': '巨大 (> 1MB)',
                }

                for size_cat, label in size_labels.items():
                    count = stats['size_distribution'][size_cat]
                    percentage = (count / stats['total_keys'] * 100) if stats['total_keys'] > 0 else 0
                    table3.add_row(label, str(count), f"{percentage:.1f}%")

                console.print(table3)

                # TTL 状态
                console.print()
                table4 = Table(title="TTL 状态", box=box.ROUNDED)
                table4.add_column("状态", style="cyan")
                table4.add_column("数量", style="green")
                table4.add_column("占比", style="yellow")

                ttl_stats = stats['ttl_status']
                total = stats['total_keys']

                table4.add_row(
                    "有 TTL",
                    str(ttl_stats['with_ttl']),
                    f"{(ttl_stats['with_ttl'] / total * 100) if total > 0 else 0:.1f}%"
                )
                table4.add_row(
                    "无 TTL",
                    str(ttl_stats['without_ttl']),
                    f"{(ttl_stats['without_ttl'] / total * 100) if total > 0 else 0:.1f}%"
                )
                table4.add_row(
                    "已过期",
                    str(ttl_stats['expired']),
                    f"{(ttl_stats['expired'] / total * 100) if total > 0 else 0:.1f}%",
                    style="red" if ttl_stats['expired'] > 0 else None
                )

                console.print(table4)

        except Exception as e:
            console.print(f"[bold red]错误:[/bold red] {str(e)}")
            sys.exit(1)

    def search(
        self,
        db_name: str,
        pattern: str,
        path: str = '.',
        limit: int = 100,
        backend: str = 'auto'
    ):
        """
        搜索匹配模式的键

        Args:
            db_name: 数据库名称
            pattern: 正则表达式模式
            path: 数据库路径（本地路径或远程地址）
            limit: 最大返回数量（默认: 100）
            backend: 后端类型 ('local', 'remote', 'auto')

        示例:
            flaxkv2 inspect search mydb "user.*"
            flaxkv2 inspect search mydb "config_.*" --limit 50
        """
        try:
            with Inspector(db_name, path, backend=backend) as inspector:
                results = inspector.search_keys(pattern, limit=limit)

                if not results:
                    console.print(f"[yellow]未找到匹配 '{pattern}' 的键[/yellow]")
                    return

                # 创建表格
                table = Table(
                    title=f"搜索结果: {pattern}",
                    show_header=True,
                    header_style="bold magenta",
                    box=box.ROUNDED
                )
                table.add_column("#", style="dim", width=6)
                table.add_column("键名", style="cyan")
                table.add_column("类型", style="green")
                table.add_column("大小", style="yellow")

                for idx, (key, info) in enumerate(results, start=1):
                    table.add_row(
                        str(idx),
                        key,
                        info['type'],
                        self._format_size(info['size'])
                    )

                console.print(table)
                console.print(f"\n找到 {len(results)} 个匹配的键")

        except Exception as e:
            console.print(f"[bold red]错误:[/bold red] {str(e)}")
            sys.exit(1)

    def delete(
        self,
        db_name: str,
        key: str,
        path: str = '.',
        backend: str = 'auto',
        force: bool = False
    ):
        """
        删除指定的键

        Args:
            db_name: 数据库名称
            key: 键名
            path: 数据库路径（本地路径或远程地址）
            backend: 后端类型 ('local', 'remote', 'auto')
            force: 强制删除，不需要确认（默认: False）

        示例:
            flaxkv2 inspect delete mydb user123
            flaxkv2 inspect delete mydb temp_data --force
        """
        try:
            with Inspector(db_name, path, backend=backend) as inspector:
                # 检查键是否存在
                info = inspector.get_value_info(key)
                if not info:
                    console.print(f"[bold red]键不存在:[/bold red] {key}")
                    return

                # 确认删除
                if not force:
                    console.print(f"\n即将删除键: [bold red]{key}[/bold red]")
                    console.print(f"类型: {info['type']}")
                    console.print(f"大小: {self._format_size(info['size'])}")

                    confirm = console.input("\n确认删除? [y/N]: ")
                    if confirm.lower() != 'y':
                        console.print("[yellow]已取消[/yellow]")
                        return

                # 执行删除
                if inspector.delete_key(key):
                    console.print(f"[bold green]✓[/bold green] 已删除键: {key}")
                else:
                    console.print(f"[bold red]✗[/bold red] 删除失败")

        except Exception as e:
            console.print(f"[bold red]错误:[/bold red] {str(e)}")
            sys.exit(1)

    def set(
        self,
        db_name: str,
        key: str,
        value: str,
        path: str = '.',
        backend: str = 'auto',
        ttl: Optional[int] = None,
        value_type: str = 'string'
    ):
        """
        设置键值

        Args:
            db_name: 数据库名称
            key: 键名
            value: 值（字符串形式）
            path: 数据库路径（本地路径或远程地址）
            backend: 后端类型 ('local', 'remote', 'auto')
            ttl: 过期时间（秒）
            value_type: 值类型 ('string', 'int', 'float', 'json')

        示例:
            flaxkv2 inspect set mydb name "John" --value_type string
            flaxkv2 inspect set mydb age "30" --value_type int
            flaxkv2 inspect set mydb config '{"key":"value"}' --value_type json
            flaxkv2 inspect set mydb temp "data" --ttl 3600
        """
        try:
            # 解析值
            parsed_value = self._parse_value(value, value_type)

            with Inspector(db_name, path, backend=backend) as inspector:
                if inspector.set_value(key, parsed_value, ttl=ttl):
                    console.print(f"[bold green]✓[/bold green] 已设置键: {key}")
                    if ttl:
                        console.print(f"TTL: {ttl} 秒")
                else:
                    console.print(f"[bold red]✗[/bold red] 设置失败")

        except Exception as e:
            console.print(f"[bold red]错误:[/bold red] {str(e)}")
            sys.exit(1)

    def _parse_value(self, value: str, value_type: str):
        """解析值"""
        if value_type == 'string':
            return value
        elif value_type == 'int':
            return int(value)
        elif value_type == 'float':
            return float(value)
        elif value_type == 'json':
            return json.loads(value)
        else:
            raise ValueError(f"不支持的值类型: {value_type}")

    def _format_size(self, size: int) -> str:
        """格式化大小"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024.0:
                return f"{size:.2f} {unit}"
            size /= 1024.0
        return f"{size:.2f} TB"
