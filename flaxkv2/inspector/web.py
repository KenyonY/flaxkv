"""
FlaxKV2 Inspector Web UI - Flask 后端服务器
"""

import os
import json
from typing import Optional
from pathlib import Path

try:
    from flask import Flask, jsonify, request, send_from_directory
    from flask_cors import CORS
except ImportError:
    raise ImportError(
        "Flask 未安装。请运行: pip install flask flask-cors"
    )

from flaxkv2.inspector import Inspector


class InspectorWebServer:
    """Inspector Web UI 服务器"""

    def __init__(self, db_name: str, path: str, backend: str = 'auto', **kwargs):
        """
        初始化 Web 服务器

        Args:
            db_name: 数据库名称
            path: 数据库路径（本地路径或远程地址）
            backend: 后端类型 ('local', 'remote', 'auto')
            **kwargs: 传递给 FlaxKV 的其他参数
        """
        self.db_name = db_name
        self.path = path
        self.backend = backend
        self.db_kwargs = kwargs

        # 创建 Flask 应用
        self.app = Flask(__name__, static_folder=None)
        CORS(self.app)  # 允许跨域

        # 注册路由
        self._register_routes()

    def _get_inspector(self) -> Inspector:
        """获取 Inspector 实例"""
        return Inspector(self.db_name, self.path, backend=self.backend, **self.db_kwargs)

    def _register_routes(self):
        """注册所有路由"""

        @self.app.route('/')
        def index():
            """主页"""
            static_dir = Path(__file__).parent.parent / 'static'
            return send_from_directory(str(static_dir), 'index.html')

        @self.app.route('/static/<path:filename>')
        def static_files(filename):
            """静态文件"""
            static_dir = Path(__file__).parent.parent / 'static'
            return send_from_directory(str(static_dir), filename)

        @self.app.route('/api/info', methods=['GET'])
        def get_info():
            """获取数据库基本信息"""
            return jsonify({
                'db_name': self.db_name,
                'path': self.path,
                'backend': self.backend,
            })

        @self.app.route('/api/keys', methods=['GET'])
        def list_keys():
            """列出所有键"""
            pattern = request.args.get('pattern')
            limit = int(request.args.get('limit', 100))
            offset = int(request.args.get('offset', 0))

            try:
                with self._get_inspector() as inspector:
                    keys = inspector.list_keys(pattern=pattern, limit=limit, offset=offset)
                    total = inspector.count_keys()

                    return jsonify({
                        'success': True,
                        'data': {
                            'keys': keys,
                            'total': total,
                            'limit': limit,
                            'offset': offset,
                        }
                    })
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500

        @self.app.route('/api/keys/<key>', methods=['GET'])
        def get_key(key: str):
            """获取键的详细信息"""
            try:
                with self._get_inspector() as inspector:
                    info = inspector.get_value_info(key)

                    if not info:
                        return jsonify({
                            'success': False,
                            'error': '键不存在'
                        }), 404

                    return jsonify({
                        'success': True,
                        'data': info
                    })
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500

        @self.app.route('/api/keys/<key>', methods=['DELETE'])
        def delete_key(key: str):
            """删除键"""
            try:
                with self._get_inspector() as inspector:
                    if inspector.delete_key(key):
                        return jsonify({
                            'success': True,
                            'message': f'已删除键: {key}'
                        })
                    else:
                        return jsonify({
                            'success': False,
                            'error': '删除失败'
                        }), 400
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500

        @self.app.route('/api/keys', methods=['POST'])
        def set_key():
            """设置键值"""
            try:
                data = request.get_json()
                key = data.get('key')
                value = data.get('value')
                ttl = data.get('ttl')

                if not key:
                    return jsonify({
                        'success': False,
                        'error': '缺少键名'
                    }), 400

                with self._get_inspector() as inspector:
                    if inspector.set_value(key, value, ttl=ttl):
                        return jsonify({
                            'success': True,
                            'message': f'已设置键: {key}'
                        })
                    else:
                        return jsonify({
                            'success': False,
                            'error': '设置失败'
                        }), 400
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500

        @self.app.route('/api/stats', methods=['GET'])
        def get_stats():
            """获取统计信息"""
            try:
                with self._get_inspector() as inspector:
                    stats = inspector.get_stats()

                    return jsonify({
                        'success': True,
                        'data': stats
                    })
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500

        @self.app.route('/api/search', methods=['GET'])
        def search_keys():
            """搜索键"""
            pattern = request.args.get('pattern')
            limit = int(request.args.get('limit', 100))

            if not pattern:
                return jsonify({
                    'success': False,
                    'error': '缺少搜索模式'
                }), 400

            try:
                with self._get_inspector() as inspector:
                    results = inspector.search_keys(pattern, limit=limit)

                    return jsonify({
                        'success': True,
                        'data': {
                            'results': [
                                {'key': key, 'info': info}
                                for key, info in results
                            ],
                            'total': len(results),
                        }
                    })
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': str(e)
                }), 500

    def run(self, host: str = '127.0.0.1', port: int = 8080, debug: bool = False):
        """
        启动 Web 服务器

        Args:
            host: 监听主机名
            port: 监听端口
            debug: 调试模式
        """
        print(f"\n🚀 FlaxKV2 Inspector Web UI 启动中...")
        print(f"📊 数据库: {self.db_name}")
        print(f"📁 路径: {self.path}")
        print(f"🌐 访问地址: http://{host}:{port}")
        print(f"\n按 Ctrl+C 停止服务器\n")

        self.app.run(host=host, port=port, debug=debug)


def start_web_server(
    db_name: str,
    path: str = '.',
    backend: str = 'auto',
    host: str = '127.0.0.1',
    port: int = 8080,
    debug: bool = False,
    **kwargs
):
    """
    启动 Inspector Web UI 服务器

    Args:
        db_name: 数据库名称
        path: 数据库路径（本地路径或远程地址）
        backend: 后端类型 ('local', 'remote', 'auto')
        host: 监听主机名（默认: 127.0.0.1）
        port: 监听端口（默认: 8080）
        debug: 调试模式（默认: False）
        **kwargs: 传递给 FlaxKV 的其他参数
    """
    server = InspectorWebServer(db_name, path, backend=backend, **kwargs)
    server.run(host=host, port=port, debug=debug)
