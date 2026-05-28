# PyInstaller spec for the Windows end-user launcher.
#
# Build on Windows with:
#   uv run pyinstaller --clean --noconfirm faers-db-windows.spec
#
# The generated dist/FAERS-DB folder intentionally does not include the
# multi-GB warehouse. Place the released warehouse folder beside FAERS-DB.exe
# or under %LOCALAPPDATA%\FAERS-DB\warehouse.

block_cipher = None

hiddenimports = [
    "uvicorn.lifespan.on",
    "uvicorn.loops.auto",
    "uvicorn.protocols.http.auto",
    "uvicorn.protocols.http.h11_impl",
]

a = Analysis(
    ["faersdb/launcher.py"],
    pathex=[],
    binaries=[],
    datas=[("faersdb/static", "faersdb/static")],
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=["tests"],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="FAERS-DB",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="FAERS-DB",
)
