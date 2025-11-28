#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Slim 打包：DuckDB + PyQt6 + onedir + UPX（无 pandas/pyarrow）
- 仅依赖：PyQt6、duckdb、pyinstaller、pillow(仅用于生成 ico)
- 仅打包 Qt 必要插件：platforms + imageformats
- onedir 布局（启动更快、exe 更小，整体目录便于进一步裁剪）
- 可选：UPX 压缩（需要先安装 upx，并配置路径或加入 PATH）
"""

import os
import sys
import shutil
import subprocess
from pathlib import Path
from textwrap import dedent

# ====== 配置区 ======
APP_NAME       = "ParquetViewer"
ENTRY_SCRIPT   = "parquet_viewer_duckdb.py"   # 主程序文件
ICON_PNG       = "icon_512.png"               # 可选：存在时会生成 ICO
ICON_ICO       = "app.ico"                    # 程序图标
VERSION_STR    = "1.0.0"
COMPANY        = "ParquetViewer"
DESC           = "Parquet File Viewer (DuckDB)"

WINDOWED       = True                          # 隐藏控制台
USE_UPX        = True                          # 需要本机安装 upx
UPX_DIR        = r"C:\tools\upx"               # 若已在 PATH，可留空
CLEAN_FIRST    = True                          # 构建前清理 build/dist
# ====================


def pip_install(p: str):
    print(f"[*] ensuring {p}")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", p])


def ensure_deps():
    for p in ["pyinstaller", "PyQt6", "duckdb", "pillow"]:
        try:
            __import__(p.split("==")[0].split(">=")[0])
        except Exception:
            pip_install(p)


def make_ico() -> bool:
    """将 ICON_PNG 生成多分辨率 ICO。如果 PNG 不存在则跳过。"""
    src = Path(ICON_PNG)
    if not src.exists():
        print(f"[!] {ICON_PNG} not found, skip ICO generating.")
        return False
    from PIL import Image
    img = Image.open(src).convert("RGBA")
    img.save(ICON_ICO, format="ICO",
             sizes=[(512, 512), (256, 256), (128, 128), (64, 64), (48, 48), (32, 32)])
    print(f"[+] ICO created: {ICON_ICO}")
    return True


def write_version_file():
    Path("file_version.txt").write_text(dedent(f"""
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
    print("[+] version file written.")


def run_pyinstaller():
    from PyInstaller.__main__ import run as pyrun

    if not Path(ENTRY_SCRIPT).exists():
        raise SystemExit(f"[!] ENTRY_SCRIPT not found: {ENTRY_SCRIPT}")

    args = [
        ENTRY_SCRIPT,
        "--name", APP_NAME,
        "--noconfirm",
        "--clean",
        # onedir：不要 --onefile
    ]
    if WINDOWED:
        args.append("--windowed")

    # 图标
    if Path(ICON_ICO).exists():
        args += ["--icon", ICON_ICO]
    elif Path(ICON_PNG).exists():
        args += ["--icon", ICON_PNG]

    # 版本信息
    if Path("file_version.txt").exists():
        args += ["--version-file", "file_version.txt"]

    # 运行时附带 ICO（可选，用于 setWindowIcon / 关联）
    if Path(ICON_ICO).exists():
        args += ["--add-data", f"{ICON_ICO};."]

    # 显式排除不需要的模块（瘦身关键）
    for mod in [
        "pandas", "pyarrow", "numpy",
        "PyQt6.QtWebEngineCore", "PyQt6.QtWebEngineWidgets", "PyQt6.QtWebEngineQuick",
        "PyQt6.QtNetworkAuth", "PyQt6.QtBluetooth", "PyQt6.QtPositioning",
        "PyQt6.QtLocation", "PyQt6.QtQml", "PyQt6.QtQuick"
    ]:
        args += ["--exclude-module", mod]

    # 某些环境下 duckdb 需声明 hidden-import
    args += ["--hidden-import", "duckdb"]

    # 精简 Qt 插件：只带 platforms 和 imageformats
    try:
        import inspect, PyQt6
        base = Path(inspect.getfile(PyQt6)).parent
        qt6 = base / "Qt6"
        platforms = qt6 / "plugins" / "platforms"
        imageformats = qt6 / "plugins" / "imageformats"
        if platforms.exists():
            args += ["--add-data", f"{str(platforms)};PyQt6/Qt6/plugins/platforms"]
            print(f"[+] Added platforms: {platforms}")
        if imageformats.exists():
            args += ["--add-data", f"{str(imageformats)};PyQt6/Qt6/plugins/imageformats"]
            print(f"[+] Added imageformats: {imageformats}")
    except Exception as e:
        print(f"[!] add Qt plugins failed: {e}")

    # UPX
    if USE_UPX:
        if UPX_DIR and Path(UPX_DIR).exists():
            args += ["--upx-dir", UPX_DIR]
        elif shutil.which("upx"):
            pass
        else:
            print("[!] USE_UPX=True 但未找到 upx，可到 https://upx.github.io/ 下载或将其加入 PATH，或改为 USE_UPX=False。")

    print("[*] PyInstaller args:")
    print("    " + " ".join(args))
    pyrun(args)


def write_assoc_user_reg(exe_path: Path):
    """生成用户级（无需管理员）.parquet 关联注册表文件。"""
    exe_abs = str(exe_path.resolve()).replace("\\", "\\\\")
    Path("associate_parquet_user.reg").write_text(dedent(f"""
    Windows Registry Editor Version 5.00

    [HKEY_CURRENT_USER\\Software\\Classes\\.parquet]
    @="{APP_NAME}File"

    [HKEY_CURRENT_USER\\Software\\Classes\\{APP_NAME}File]
    @="Parquet File"

    [HKEY_CURRENT_USER\\Software\\Classes\\{APP_NAME}File\\DefaultIcon]
    @="\\"{exe_abs}\\",0"

    [HKEY_CURRENT_USER\\Software\\Classes\\{APP_NAME}File\\shell\\open\\command]
    @="\\"{exe_abs}\\" \\"%1\\""
    """).strip(), encoding="utf-8")
    print("[+] associate_parquet_user.reg generated (user-level).")


def main():
    # 可选：一键清理历史产物
    if CLEAN_FIRST:
        for d in ["build", "dist", "__pycache__"]:
            if Path(d).exists():
                try:
                    shutil.rmtree(d)
                except Exception:
                    subprocess.call(["cmd", "/c", f'rmdir /s /q "{d}"'])
        spec = Path(f"{APP_NAME}.spec")
        if spec.exists():
            try:
                spec.unlink()
            except Exception:
                pass

    ensure_deps()
    if not Path(ICON_ICO).exists():
        _ = make_ico()
    write_version_file()
    run_pyinstaller()

    exe = Path("dist") / APP_NAME / f"{APP_NAME}.exe"  # onedir
    if exe.exists():
        print(f"\n✅ Done. Output: {exe}")
        write_assoc_user_reg(exe)
    else:
        print("\n[!] Build finished but exe not found. Check dist/ structure.")

    print("\nTips:")
    print("  - 体积仍嫌大时：保持 onedir + USE_UPX=True；或尝试 Nuitka。")
    print("  - 双击 associate_parquet_user.reg 可把 .parquet 关联到该 EXE（用户级，无需管理员）。")


if __name__ == "__main__":
    main()
