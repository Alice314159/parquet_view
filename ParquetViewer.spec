# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['parquet_viewer_duckdb.py'],
    pathex=[],
    binaries=[],
    datas=[('app.ico', '.'), ('F:\\VesperSet\\duckDB_client\\.venv\\Lib\\site-packages\\PyQt6\\Qt6\\plugins\\platforms', 'PyQt6/Qt6/plugins/platforms'), ('F:\\VesperSet\\duckDB_client\\.venv\\Lib\\site-packages\\PyQt6\\Qt6\\plugins\\imageformats', 'PyQt6/Qt6/plugins/imageformats')],
    hiddenimports=['duckdb'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['pandas', 'pyarrow', 'numpy', 'PyQt6.QtWebEngineCore', 'PyQt6.QtWebEngineWidgets', 'PyQt6.QtWebEngineQuick', 'PyQt6.QtNetworkAuth', 'PyQt6.QtBluetooth', 'PyQt6.QtPositioning', 'PyQt6.QtLocation', 'PyQt6.QtQml', 'PyQt6.QtQuick'],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='ParquetViewer',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    version='file_version.txt',
    icon=['app.ico'],
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='ParquetViewer',
)
