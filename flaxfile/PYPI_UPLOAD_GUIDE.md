# FlaxFile PyPI 上传指南

## ✅ 准备工作已完成

1. ✅ 已创建 `pyproject.toml`（使用 hatchling 构建后端）
2. ✅ 已重构目录结构为标准的 src-layout
3. ✅ 已成功构建测试（v1.0.0）
4. ✅ 已验证本地安装和功能

## 📁 当前目录结构

```
flaxfile/  (项目根目录)
├── pyproject.toml          # 项目配置文件
├── README.md               # 项目说明（会显示在 PyPI 页面）
├── QUICKSTART.md           # 快速开始指南
├── dist/                   # 构建产物
│   ├── flaxfile-1.0.0-py3-none-any.whl
│   └── flaxfile-1.0.0.tar.gz
└── src/
    └── flaxfile/           # Python 包
        ├── __init__.py     # 包含版本号 __version__ = "1.0.0"
        ├── cli.py
        ├── client.py
        ├── config.py
        └── server.py
```

## 🚀 上传到 PyPI

### 1. 安装上传工具

```bash
pip install twine
```

### 2. 创建 PyPI 账号

- 访问 https://pypi.org/account/register/
- 注册账号并验证邮箱

### 3. 配置 PyPI Token（推荐）

1. 登录 PyPI，进入 Account Settings → API Tokens
2. 创建新 token，scope 选择 "Entire account" 或针对 flaxfile 项目
3. 保存 token（只会显示一次！）
4. 在本地配置：

```bash
# 创建 ~/.pypirc 文件
cat > ~/.pypirc << 'EOF'
[pypi]
username = __token__
password = pypi-AgEIcHlwaS5vcmc...你的token...
EOF

chmod 600 ~/.pypirc
```

### 4. 测试上传（可选，推荐首次使用）

先上传到 TestPyPI 测试：

```bash
# 注册 TestPyPI 账号：https://test.pypi.org/account/register/

# 上传到 TestPyPI
twine upload --repository testpypi dist/*

# 测试安装
pip install --index-url https://test.pypi.org/simple/ flaxfile
```

### 5. 正式上传到 PyPI

```bash
cd /Users/kunyuan/github/flaxkv/flaxfile

# 确保构建是最新的
rm -rf dist/
python -m build

# 检查构建产物
twine check dist/*

# 上传到 PyPI
twine upload dist/*
```

### 6. 验证上传

- 访问 https://pypi.org/project/flaxfile/
- 测试安装：

```bash
pip install flaxfile
flaxfile version
```

## 🔄 发布新版本

1. 修改 `src/flaxfile/__init__.py` 中的版本号：
   ```python
   __version__ = "1.0.1"
   ```

2. 重新构建和上传：
   ```bash
   rm -rf dist/
   python -m build
   twine check dist/*
   twine upload dist/*
   ```

## 📝 注意事项

1. **版本号不可重复**：每次上传必须使用新的版本号
2. **README.md**：会自动显示在 PyPI 项目页面
3. **许可证**：使用父目录的 LICENSE 文件（MIT License）
4. **依赖项**：
   - pyzmq>=25.0.0
   - fire>=0.5.0
5. **Python 版本**：支持 Python 3.8+

## ⚠️ 上传前检查清单

- [ ] README.md 内容完整准确
- [ ] 版本号已更新（如果不是首次发布）
- [ ] 所有功能已测试
- [ ] 构建成功：`python -m build`
- [ ] 包检查通过：`twine check dist/*`
- [ ] 本地安装测试：`pip install dist/*.whl`

## 🎉 当前状态

✅ **已准备就绪，可以直接上传到 PyPI！**

当前构建的包：
- flaxfile-1.0.0-py3-none-any.whl (16 KB)
- flaxfile-1.0.0.tar.gz (14 KB)

执行以下命令即可上传：
```bash
pip install twine
twine upload dist/*
```
