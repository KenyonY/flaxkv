#!/usr/bin/env python3
"""
FlaxFile 目录同步功能
"""

import os
from pathlib import Path
from typing import List, Tuple, Optional
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TransferSpeedColumn, TimeRemainingColumn, DownloadColumn

console = Console()


def parse_gitignore(gitignore_path: Path) -> List[str]:
    """
    解析 .gitignore 文件

    Args:
        gitignore_path: .gitignore 文件路径

    Returns:
        忽略规则列表
    """
    if not gitignore_path.exists():
        return []

    patterns = []
    with open(gitignore_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            # 跳过空行和注释
            if not line or line.startswith('#'):
                continue
            patterns.append(line)

    return patterns


def should_ignore(path: str, patterns: List[str], is_dir: bool = False) -> bool:
    """
    检查路径是否应该被忽略（简化版 gitignore 匹配）

    Args:
        path: 相对路径
        patterns: gitignore 规则列表
        is_dir: 是否为目录

    Returns:
        是否应该忽略
    """
    import fnmatch

    for pattern in patterns:
        # 处理否定规则（!）
        negate = pattern.startswith('!')
        if negate:
            pattern = pattern[1:]

        # 处理目录规则（以 / 结尾）
        pattern_is_dir = pattern.endswith('/')
        if pattern_is_dir:
            pattern = pattern[:-1]
            # 目录规则只匹配目录
            if not is_dir:
                continue

        # 处理根目录规则（以 / 开头）
        if pattern.startswith('/'):
            pattern = pattern[1:]
            # 只匹配根目录
            if fnmatch.fnmatch(path, pattern):
                return not negate
        else:
            # 匹配任意位置
            # 支持通配符
            if fnmatch.fnmatch(path, pattern) or fnmatch.fnmatch(path, f"*/{pattern}"):
                return not negate
            # 检查是否匹配路径的任意部分
            parts = path.split('/')
            for part in parts:
                if fnmatch.fnmatch(part, pattern):
                    return not negate

    return False


def scan_directory(directory: str, respect_gitignore: bool = True) -> List[Tuple[str, str]]:
    """
    递归扫描目录，返回所有文件的相对路径

    Args:
        directory: 要扫描的目录路径
        respect_gitignore: 是否遵循 .gitignore 规则（默认 True）

    Returns:
        [(绝对路径, 相对路径), ...] 列表
    """
    directory = Path(directory).resolve()

    if not directory.exists():
        raise FileNotFoundError(f"目录不存在: {directory}")

    if not directory.is_dir():
        raise NotADirectoryError(f"不是目录: {directory}")

    # 解析 .gitignore
    gitignore_patterns = []
    if respect_gitignore:
        gitignore_path = directory / '.gitignore'
        gitignore_patterns = parse_gitignore(gitignore_path)

    files = []

    for root, dirs, filenames in os.walk(directory):
        root_path = Path(root)

        # 过滤目录（使用 gitignore）
        if respect_gitignore and gitignore_patterns:
            filtered_dirs = []
            for d in dirs:
                dir_path = root_path / d
                rel_path = str(dir_path.relative_to(directory))
                if not should_ignore(rel_path, gitignore_patterns, is_dir=True):
                    filtered_dirs.append(d)
            dirs[:] = filtered_dirs

        for filename in filenames:
            abs_path = Path(root) / filename
            rel_path = abs_path.relative_to(directory)

            # 检查是否应该忽略
            if respect_gitignore and gitignore_patterns:
                if should_ignore(str(rel_path), gitignore_patterns, is_dir=False):
                    continue

            files.append((str(abs_path), str(rel_path)))

    return files


def push_directory(
    client,
    local_dir: str,
    remote_dir: str,
    show_progress: bool = True,
    password: Optional[str] = None
) -> dict:
    """
    上传本地目录到服务器

    Args:
        client: FlaxFileClient 实例
        local_dir: 本地目录路径
        remote_dir: 远程目录名称
        show_progress: 是否显示进度
        password: 密码（可选）

    Returns:
        同步结果统计
    """
    # 1. 扫描本地目录
    console.print(f"[cyan]📁 扫描本地目录: {local_dir}")
    files = scan_directory(local_dir)

    if not files:
        console.print("[yellow]⚠️  目录为空，没有文件需要上传")
        return {
            'total_files': 0,
            'uploaded': 0,
            'failed': 0,
            'total_bytes': 0
        }

    console.print(f"[green]✓ 发现 {len(files)} 个文件")

    # 2. 计算总大小
    total_bytes = sum(os.path.getsize(abs_path) for abs_path, _ in files)
    console.print(f"[cyan]📊 总大小: {total_bytes / (1024*1024):.2f} MB")
    console.print()

    # 3. 先连接到服务器（在显示进度条之前完成密码输入）
    console.print("[cyan]🔗 连接到服务器...")
    client.connect()
    console.print()

    # 4. 上传所有文件
    uploaded = 0
    failed = 0
    failed_files = []

    if show_progress:
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            main_task = progress.add_task(
                f"[cyan]上传到 {remote_dir}/",
                total=total_bytes
            )

            bytes_uploaded = 0

            for abs_path, rel_path in files:
                # 构造远程 key
                remote_key = f"{remote_dir}/{rel_path}"

                try:
                    file_size = os.path.getsize(abs_path)

                    # 更新当前文件描述
                    progress.update(
                        main_task,
                        description=f"[cyan]上传: {rel_path}"
                    )

                    # 上传文件（不显示单文件进度，避免刷屏）
                    client.upload_file(abs_path, remote_key, show_progress=False)
                    uploaded += 1

                    # 更新字节进度
                    bytes_uploaded += file_size
                    progress.update(main_task, completed=bytes_uploaded)

                except Exception as e:
                    failed += 1
                    failed_files.append((rel_path, str(e)))
                    console.print(f"[red]✗ 上传失败: {rel_path} - {e}")

    else:
        # 无进度条模式
        for abs_path, rel_path in files:
            remote_key = f"{remote_dir}/{rel_path}"

            try:
                console.print(f"[cyan]上传: {rel_path}")
                client.upload_file(abs_path, remote_key, show_progress=False)
                uploaded += 1
            except Exception as e:
                failed += 1
                failed_files.append((rel_path, str(e)))
                console.print(f"[red]✗ 上传失败: {rel_path} - {e}")

    # 4. 显示结果
    console.print()
    if failed == 0:
        console.print(f"[bold green]✓ 同步完成! 成功上传 {uploaded} 个文件")
    else:
        console.print(f"[yellow]⚠️  同步完成，但有 {failed} 个文件失败:")
        for rel_path, error in failed_files:
            console.print(f"  [red]✗ {rel_path}: {error}")

    return {
        'total_files': len(files),
        'uploaded': uploaded,
        'failed': failed,
        'failed_files': failed_files,
        'total_bytes': total_bytes
    }


def pull_directory(
    client,
    remote_dir: str,
    local_dir: str,
    show_progress: bool = True,
    password: Optional[str] = None
) -> dict:
    """
    从服务器下载目录到本地

    Args:
        client: FlaxFileClient 实例
        remote_dir: 远程目录名称
        local_dir: 本地目录路径
        show_progress: 是否显示进度
        password: 密码（可选）

    Returns:
        同步结果统计
    """
    # 1. 先连接到服务器（在显示进度条之前完成密码输入）
    console.print("[cyan]🔗 连接到服务器...")
    client.connect()
    console.print()

    # 2. 列出远程文件
    console.print(f"[cyan]📋 获取远程文件列表: {remote_dir}/")

    try:
        files = client.list_files(prefix=remote_dir)
    except Exception as e:
        console.print(f"[red]✗ 获取文件列表失败: {e}")
        return {
            'total_files': 0,
            'downloaded': 0,
            'failed': 0,
            'total_bytes': 0
        }

    if not files:
        console.print("[yellow]⚠️  远程目录为空或不存在")
        return {
            'total_files': 0,
            'downloaded': 0,
            'failed': 0,
            'total_bytes': 0
        }

    console.print(f"[green]✓ 发现 {len(files)} 个文件")

    # 3. 计算总大小
    total_bytes = sum(f['size'] for f in files)
    console.print(f"[cyan]📊 总大小: {total_bytes / (1024*1024):.2f} MB")
    console.print()

    # 4. 创建本地目录
    local_dir_path = Path(local_dir)
    local_dir_path.mkdir(parents=True, exist_ok=True)

    # 5. 下载所有文件
    downloaded = 0
    failed = 0
    failed_files = []

    if show_progress:
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            main_task = progress.add_task(
                f"[cyan]下载 {remote_dir}/",
                total=total_bytes
            )

            bytes_downloaded = 0

            for file_info in files:
                remote_key = file_info['key']
                file_size = file_info['size']

                # 计算本地路径（移除远程目录前缀）
                if remote_key.startswith(remote_dir + '/'):
                    rel_path = remote_key[len(remote_dir) + 1:]
                elif remote_key.startswith(remote_dir):
                    rel_path = remote_key[len(remote_dir):]
                else:
                    rel_path = remote_key

                local_path = local_dir_path / rel_path

                try:
                    # 更新当前文件描述
                    progress.update(
                        main_task,
                        description=f"[cyan]下载: {rel_path}"
                    )

                    # 创建父目录
                    local_path.parent.mkdir(parents=True, exist_ok=True)

                    # 下载文件（不显示单文件进度，避免刷屏）
                    client.download_file(remote_key, str(local_path), show_progress=False)
                    downloaded += 1

                    # 更新字节进度
                    bytes_downloaded += file_size
                    progress.update(main_task, completed=bytes_downloaded)

                except Exception as e:
                    failed += 1
                    failed_files.append((rel_path, str(e)))
                    console.print(f"[red]✗ 下载失败: {rel_path} - {e}")

    else:
        # 无进度条模式
        for file_info in files:
            remote_key = file_info['key']

            # 计算本地路径
            if remote_key.startswith(remote_dir + '/'):
                rel_path = remote_key[len(remote_dir) + 1:]
            elif remote_key.startswith(remote_dir):
                rel_path = remote_key[len(remote_dir):]
            else:
                rel_path = remote_key

            local_path = local_dir_path / rel_path

            try:
                console.print(f"[cyan]下载: {rel_path}")

                # 创建父目录
                local_path.parent.mkdir(parents=True, exist_ok=True)

                # 下载文件
                client.download_file(remote_key, str(local_path), show_progress=False)
                downloaded += 1

            except Exception as e:
                failed += 1
                failed_files.append((rel_path, str(e)))
                console.print(f"[red]✗ 下载失败: {rel_path} - {e}")

    # 6. 显示结果
    console.print()
    if failed == 0:
        console.print(f"[bold green]✓ 同步完成! 成功下载 {downloaded} 个文件到 {local_dir}")
    else:
        console.print(f"[yellow]⚠️  同步完成，但有 {failed} 个文件失败:")
        for rel_path, error in failed_files:
            console.print(f"  [red]✗ {rel_path}: {error}")

    return {
        'total_files': len(files),
        'downloaded': downloaded,
        'failed': failed,
        'failed_files': failed_files,
        'total_bytes': total_bytes
    }
