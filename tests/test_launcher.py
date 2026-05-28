from faersdb import launcher


def test_find_warehouse_prefers_env_path(monkeypatch, tmp_path):
    env_warehouse = tmp_path / "custom" / "warehouse"
    env_warehouse.mkdir(parents=True)
    monkeypatch.setenv("WAREHOUSE_DIR", str(env_warehouse))
    monkeypatch.setattr(launcher, "_app_base_dir", lambda: tmp_path / "app")

    assert launcher.find_warehouse_dir() == env_warehouse.resolve()


def test_find_warehouse_falls_back_to_app_folder(monkeypatch, tmp_path):
    monkeypatch.delenv("WAREHOUSE_DIR", raising=False)
    monkeypatch.delenv("LOCALAPPDATA", raising=False)
    monkeypatch.setattr(launcher, "_app_base_dir", lambda: tmp_path / "FAERS-DB")
    monkeypatch.chdir(tmp_path)

    assert launcher.find_warehouse_dir() == (tmp_path / "FAERS-DB" / "warehouse").resolve()


def test_find_available_port_returns_open_port():
    port = launcher.find_available_port(preferred=53121)

    assert isinstance(port, int)
    assert port >= 53121
