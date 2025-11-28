#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
一键打包 PyQt6 Parquet Viewer:
- 自动安装依赖
- PNG -> ICO（多尺寸，可选）
- 生成版本信息
- 调用 PyInstaller API 打包（嵌入并打包 ICO 资源）
- 收集 pyarrow/pandas/PyQt6 资源，自动带上 Qt plugins 和 translations
- 生成系统级 & 用户级 .reg 文件用于关联 .parquet
"""

import os
import sys
import subprocess
from pathlib import Path
from textwrap import dedent

# ===================== 可配置参数 =====================
APP_NAME     = "ParquetViewer"
ENTRY_SCRIPT = "parquet_viewer.py"      # 入口脚本
ICON_PNG     = "icon_512.png"           # 可选：若存在则自动生成 ICO
ICON_ICO     = "app.ico"                # 最终使用的 ICO 名称
VERSION_STR  = "1.0.0"
COMPANY      = "ParquetViewer"
DESC         = "Parquet File Viewer"

# 打包配置
ONEFILE        = True    # True: --onefile；False: --onedir
WINDOWED       = True    # True: 无控制台；False: 有控制台
ADD_QT_PLUGINS = True    # 自动打包 PyQt6 插件目录和翻译目录
EXTRA_DATAS    = []      # 形如 [("assets", "assets")]
EXTRA_HOOKS    = []      # 额外 hooks 目录（通常为空）

# 体积优化（可选）
USE_UPX        = False                 # 使用 UPX 压缩（需要本机安装 upx）
UPX_DIR        = r"C:\tools\upx"       # upx 所在路径（或把 upx 加到 PATH）

# onefile 运行时临时目录（可选，缺省 None 使用系统临时目录）
RUNTIME_TMPDIR = None  # 例如 ".\\._tmp"
# =====================================================


def pip_install(pkg: str):
    print(f"[*] ensuring package: {pkg}")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", pkg])


def ensure_deps():
    # 基础
    try:
        import PyInstaller  # noqa
    except Exception:
        pip_install("pyinstaller")
    # 运行依赖
    for pkg in ["pillow", "pandas", "pyarrow", "PyQt6"]:
        mod = pkg.split("==")[0].split(">=")[0]
        try:
            __import__(mod)
        except Exception:
            pip_install(pkg)


def make_ico():
    """将 ICON_PNG 生成多分辨率 ICO。如果 PNG 不存在则跳过。"""
    src = Path(ICON_PNG)
    if not src.exists():
        print(f"[!] {ICON_PNG} not found, skip ICO build (place a 512x512 png and rerun if needed).")
        return False
    from PIL import Image
    im = Image.open(src).convert("RGBA")
    sizes = [512, 256, 128, 64, 48, 32]
    im.save(ICON_ICO, format="ICO", sizes=[(s, s) for s in sizes])
    print(f"[+] ICO created: {ICON_ICO}")
    return True


def write_version_file():
    vf = Path("file_version.txt")
    vf.write_text(dedent(f"""
    # UTF-8
    VSVersionInfo(
      ffi=FixedFileInfo(
        filevers=({VERSION_STR.replace('.', ',')}, 0),
        prodvers=({VERSION_STR.replace('.', ',')}, 0),
        mask=0x3f,
        flags=0x0,
        OS=0x40004,
        fileType=0x1,
        subtype=0x0,
        date=(0, 0)
      ),
      kids=[
        StringFileInfo([
          StringTable('040904B0', [
            StringStruct('CompanyName', '{COMPANY}'),
            StringStruct('FileDescription', '{DESC}'),
            StringStruct('FileVersion', '{VERSION_STR}'),
            StringStruct('ProductName', '{APP_NAME}'),
            StringStruct('ProductVersion', '{VERSION_STR}')
          ])
        ]),
        VarFileInfo([VarStruct('Translation', [1033, 1200])])
      ]
    )
    """).strip(), encoding="utf-8")
    print("[+] version file written: file_version.txt")


def guess_qt_plugin_adddatas(args_list):
    """自动把 PyQt6 的 plugins 和 translations 目录加入 --add-data。"""
    try:
        import PyQt6, inspect  # noqa
        base = Path(inspect.getfile(PyQt6)).parent  # .../site-packages/PyQt6
        qt6 = base / "Qt6"
        plugins = qt6 / "plugins"
        translations = qt6 / "translations"
        if plugins.exists():
            args_list += ["--add-data", f"{str(plugins)};PyQt6/Qt6/plugins"]
            print(f"[+] Added PyQt6 plugins: {plugins}")
        if translations.exists():
            args_list += ["--add-data", f"{str(translations)};PyQt6/Qt6/translations"]
            print(f"[+] Added PyQt6 translations: {translations}")
    except Exception as e:
        print(f"[!] Failed to add PyQt6 plugins/translations: {e}")


def run_pyinstaller():
    from PyInstaller.__main__ import run as pyinstaller_run

    if not Path(ENTRY_SCRIPT).exists():
        raise SystemExit(f"[!] ENTRY_SCRIPT not found: {ENTRY_SCRIPT}")

    args = [
        ENTRY_SCRIPT,
        "--name", APP_NAME,
        "--noconfirm",
        "--clean",                     # 清理历史缓存
        "--collect-all", "pyarrow",    # 收集 pyarrow 全部资源（dll/pyd/数据）
        "--collect-data", "pandas",    # 收集 pandas 数据文件（如 tz/locale）
        "--collect-submodules", "PyQt6",
    ]

    if WINDOWED:
        args.append("--windowed")
    if ONEFILE:
        args.append("--onefile")
        if RUNTIME_TMPDIR:
            args += ["--runtime-tmpdir", RUNTIME_TMPDIR]

    if USE_UPX:
        # 需要先安装 UPX 并把路径配置正确，或在 PATH 中
        if UPX_DIR and Path(UPX_DIR).exists():
            args += ["--upx-dir", UPX_DIR]
        else:
            print("[!] USE_UPX=True but UPX_DIR not found; skip UPX.")

    # icon（优先 ICO；没有再退 PNG）
    if Path(ICON_ICO).exists():
        args += ["--icon", ICON_ICO]
    elif Path(ICON_PNG).exists():
        args += ["--icon", ICON_PNG]

    # 版本信息
    if Path("file_version.txt").exists():
        args += ["--version-file", "file_version.txt"]

    # 额外资源（把 ICO 也作为运行时资源打进包，QIcon 可读取）
    datas = list(EXTRA_DATAS)
    if Path(ICON_ICO).exists():
        datas.append((ICON_ICO, "."))  # 放到可执行同目录（onefile 会解到临时目录）
    for src, dest in datas:
        args += ["--add-data", f"{src};{dest}"]

    # Qt 插件/翻译
    if ADD_QT_PLUGINS:
        guess_qt_plugin_adddatas(args)

    # 额外 hook（一般用不上）
    for h in EXTRA_HOOKS:
        args += ["--additional-hooks-dir", h]

    print("[*] PyInstaller args:")
    print("    " + " ".join(args))
    pyinstaller_run(args)


def write_file_association_regs(exe_path: Path):
    """生成系统级 & 用户级 .reg 文件使 .parquet 关联到本程序。"""
    exe_abs = str(exe_path.resolve())
    exe_abs_escaped = exe_abs.replace("\\", "\\\\")  # .reg 需要转义反斜杠

    # 系统级（需要管理员）
    reg = Path("associate_parquet.reg")
    reg.write_text(dedent(f"""
    Windows Registry Editor Version 5.00

    [HKEY_CLASSES_ROOT\\.parquet]
    @="{APP_NAME}File"

    [HKEY_CLASSES_ROOT\\{APP_NAME}File]
    @="Parquet File"

    [HKEY_CLASSES_ROOT\\{APP_NAME}File\\DefaultIcon]
    @="\\"{exe_abs_escaped}\\",0"

    [HKEY_CLASSES_ROOT\\{APP_NAME}File\\shell\\open\\command]
    @="\\"{exe_abs_escaped}\\" \\"%1\\""
    """).strip(), encoding="utf-8")
    print("   Extra: associate_parquet.reg generated (system-level).")

    # 用户级（无管理员）
    reg_user = Path("associate_parquet_user.reg")
    reg_user.write_text(dedent(f"""
    Windows Registry Editor Version 5.00

    [HKEY_CURRENT_USER\\Software\\Classes\\.parquet]
    @="{APP_NAME}File"

    [HKEY_CURRENT_USER\\Software\\Classes\\{APP_NAME}File]
    @="Parquet File"

    [HKEY_CURRENT_USER\\Software\\Classes\\{APP_NAME}File\\DefaultIcon]
    @="\\"{exe_abs_escaped}\\",0"

    [HKEY_CURRENT_USER\\Software\\Classes\\{APP_NAME}File\\shell\\open\\command]
    @="\\"{exe_abs_escaped}\\" \\"%1\\""
    """).strip(), encoding="utf-8")
    print("   Extra: associate_parquet_user.reg generated (user-level, no admin).")


def clean_previous_builds_if_requested():
    """环境变量 CLEAN_BUILD=1 时，清理历史构建缓存."""
    if os.environ.get("CLEAN_BUILD") != "1":
        return
    print("[*] CLEAN_BUILD=1 -> cleaning previous build artifacts...")
    # 删除目录
    for d in ["build", "dist", "__pycache__"]:
        p = Path(d)
        if p.exists():
            try:
                subprocess.call(["cmd", "/c", f'rmdir /s /q "{p}"'])
            except Exception:
                pass
    # 删除 spec
    spec = Path(f"{APP_NAME}.spec")
    if spec.exists():
        try:
            spec.unlink()
        except Exception:
            pass


def main():
    clean_previous_builds_if_requested()
    ensure_deps()
    if not Path(ICON_ICO).exists():
        _ = make_ico()  # 若没有 PNG，会跳过；已有 ICO 也 OK
    write_version_file()
    run_pyinstaller()

    out = Path("dist") / (APP_NAME + (".exe" if ONEFILE else ""))
    print("\n✅ Done.")
    if out.exists():
        print(f"   Output: {out}")
    else:
        print("   Output dir: dist/")

    # 生成 .reg（使用 dist 下 EXE 的绝对路径）
    if out.exists():
        write_file_association_regs(out)
    else:
        print("[!] EXE not found; skip generating .reg with absolute path.")

    print("\nTips:")
    print("  - 若图标/资源未刷新，先清缓存再重打：")
    print("      set CLEAN_BUILD=1 && python build_parquet_viewer.py")
    print("  - 如需将 .parquet 关联到该程序：双击导入 associate_parquet_user.reg（用户级，无需管理员）")
    print("  - 若要安装到 Program Files 并系统级关联，建议用安装器（Inno Setup/WiX）或导入 associate_parquet.reg（管理员）")


if __name__ == "__main__":
    main()
