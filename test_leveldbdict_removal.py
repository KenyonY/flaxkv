#!/usr/bin/env python3
"""
验证 LevelDBDict 从公共 API 移除的测试
"""
import tempfile
import shutil
import warnings

def test_1_cannot_import_from_main_module():
    """测试 1: 从主模块导入 LevelDBDict 应该失败"""
    print("测试 1: 验证无法从 flaxkv2 主模块导入 LevelDBDict...")
    try:
        from flaxkv2 import LevelDBDict
        print("  ❌ 失败: 仍然可以从 flaxkv2 导入 LevelDBDict")
        return False
    except (ImportError, AttributeError) as e:
        print(f"  ✅ 成功: 无法从主模块导入 ({type(e).__name__})")
        return True

def test_2_can_import_directly():
    """测试 2: 直接导入应该仍然可用（向后兼容）"""
    print("\n测试 2: 验证可以直接导入 LevelDBDict...")
    try:
        from flaxkv2.core.leveldb_dict import LevelDBDict
        print("  ✅ 成功: 可以直接导入")
        return True
    except ImportError as e:
        print(f"  ❌ 失败: 无法直接导入 ({e})")
        return False

def test_3_deprecation_warning():
    """测试 3: 创建实例时应触发 DeprecationWarning"""
    print("\n测试 3: 验证创建实例触发 DeprecationWarning...")
    temp_dir = tempfile.mkdtemp()
    try:
        from flaxkv2.core.leveldb_dict import LevelDBDict

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            db = LevelDBDict("test_db", temp_dir, rebuild=True, raw=True)
            db.close()

            if len(w) > 0:
                warning = w[0]
                is_deprecation = issubclass(warning.category, DeprecationWarning)
                has_message = "deprecated" in str(warning.message).lower()

                if is_deprecation and has_message:
                    print(f"  ✅ 成功: 触发了 DeprecationWarning")
                    print(f"     消息: {warning.message}")
                    return True
                else:
                    print(f"  ❌ 失败: 警告类型或消息不正确")
                    return False
            else:
                print("  ❌ 失败: 没有触发警告")
                return False
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

def test_4_flaxkv_uses_rawleveldbdict():
    """测试 4: FlaxKV 工厂创建 RawLevelDBDict 实例"""
    print("\n测试 4: 验证 FlaxKV 使用 RawLevelDBDict...")
    temp_dir = tempfile.mkdtemp()
    try:
        from flaxkv2 import FlaxKV, RawLevelDBDict

        db = FlaxKV("test_db", temp_dir, rebuild=True)

        if isinstance(db, RawLevelDBDict):
            print("  ✅ 成功: FlaxKV 创建了 RawLevelDBDict 实例")
            db.close()
            return True
        else:
            print(f"  ❌ 失败: FlaxKV 创建了 {type(db).__name__} 实例")
            db.close()
            return False
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

def test_5_not_in_all():
    """测试 5: LevelDBDict 不在 __all__ 中"""
    print("\n测试 5: 验证 LevelDBDict 不在 __all__ 导出列表中...")
    import flaxkv2

    if hasattr(flaxkv2, '__all__'):
        if 'LevelDBDict' not in flaxkv2.__all__:
            print(f"  ✅ 成功: LevelDBDict 不在 __all__ 中")
            print(f"     当前 __all__: {flaxkv2.__all__}")
            return True
        else:
            print(f"  ❌ 失败: LevelDBDict 仍在 __all__ 中")
            return False
    else:
        print("  ⚠️  警告: 模块没有 __all__ 属性")
        return True

def main():
    print("=" * 70)
    print("LevelDBDict 移除验证测试")
    print("=" * 70)

    results = []
    results.append(test_1_cannot_import_from_main_module())
    results.append(test_2_can_import_directly())
    results.append(test_3_deprecation_warning())
    results.append(test_4_flaxkv_uses_rawleveldbdict())
    results.append(test_5_not_in_all())

    print("\n" + "=" * 70)
    print(f"测试结果: {sum(results)}/{len(results)} 通过")
    print("=" * 70)

    if all(results):
        print("\n✅ 所有测试通过！LevelDBDict 已成功从公共 API 移除")
        return 0
    else:
        print("\n❌ 部分测试失败，请检查上述输出")
        return 1

if __name__ == "__main__":
    exit(main())
