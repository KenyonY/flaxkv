from flaxkv2 import FlaxKV

# 连接到加密的远程服务器
# 方式 1: 直接传递密码（推荐）
db = FlaxKV(
    "abc",
    "tcp://127.0.0.1:25555",
    auto_nested=True,
    enable_encryption=True,      # 启用加密
    password="yao",               # 密码（与服务器端相同）
    derive_from_password=True     # 从密码派生密钥（默认值）
)

db['key1'] = 'value1'
print(db['key1'])

# 方式 2: 如果不启用加密，只需要去掉加密参数
# db = FlaxKV("abc", "tcp://127.0.0.1:25555", auto_nested=True)


"""
当我用flaxkv2 cli去set 或 get
一个比较大的文件时（如1G大小的文件），现在的方案是否足以应付这种情况？还是会有更好的方式？
"""