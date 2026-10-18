from importlib import metadata

import pytest

from scripts import check_dependency_floors as floors

REQUIRES = [
    "aiobotocore>=2.5.0",
    'redis>=4.3.0; extra == "redis"',
    'msgpack>=1.2.3; extra == "msgpack"',
    'pytest>=7.0; extra == "testing"',
]


def test_declared_floors_selects_core_and_requested_extras():
    assert floors.declared_floors(REQUIRES, ["redis"]) == {"aiobotocore": "2.5.0", "redis": "4.3.0"}


def test_declared_floors_core_only():
    assert floors.declared_floors(REQUIRES, []) == {"aiobotocore": "2.5.0"}


def test_declared_floors_keeps_core_requirement_with_environment_marker():
    line = 'aiobotocore>=2.5.0; python_version >= "3.0"'
    assert floors.declared_floors([line], []) == {"aiobotocore": "2.5.0"}
    assert floors.declared_floors([line], ["redis"]) == {"aiobotocore": "2.5.0"}


def test_declared_floors_accepts_exact_pin():
    assert floors.declared_floors(["msgpack==1.2.3"], []) == {"msgpack": "1.2.3"}


@pytest.mark.parametrize("line", ["aiobotocore", "aiobotocore<3", "aiobotocore>=2.5.0,==2.6.0"])
def test_declared_floors_rejects_requirement_without_single_floor(line):
    with pytest.raises(ValueError):
        floors.declared_floors([line], [])


def test_find_mismatches_passes_when_floors_installed():
    installed = {"aiobotocore": "2.5.0", "redis": "4.3.0"}
    assert floors.find_mismatches({"aiobotocore": "2.5.0", "redis": "4.3"}, installed.__getitem__) == []


@pytest.mark.parametrize("version", ["2.4.2", "2.5.1"])
def test_find_mismatches_reports_version_off_floor(version):
    installed = {"aiobotocore": version, "redis": "4.3.0"}
    result = floors.find_mismatches({"aiobotocore": "2.5.0", "redis": "4.3.0"}, installed.__getitem__)
    assert result == [("aiobotocore", "2.5.0", version)]


def test_find_mismatches_reports_missing_package():
    def installed(name):
        raise metadata.PackageNotFoundError(name)

    assert floors.find_mismatches({"redis": "4.3.0"}, installed) == [("redis", "4.3.0", None)]


def test_main_rejects_unknown_extra(capsys):
    assert floors.main(["--extras", "redis,not-an-extra"]) == 2
    assert "not-an-extra" in capsys.readouterr().err


class FakeDistribution:
    requires = REQUIRES

    class metadata:
        @staticmethod
        def get_all(key):
            return ["redis", "msgpack", "testing"] if key == "Provides-Extra" else None


@pytest.mark.parametrize(
    "redis_version, expected",
    [("4.3.0", 0), ("4.4.0", 1)],
)
def test_main_exit_status_follows_installed_versions(monkeypatch, capsys, redis_version, expected):
    installed = {"aiobotocore": "2.5.0", "redis": redis_version}
    monkeypatch.setattr(floors.metadata, "distribution", lambda name: FakeDistribution)
    monkeypatch.setattr(floors.metadata, "version", installed.__getitem__)

    assert floors.main(["--extras", "redis"]) == expected
    if expected:
        assert "FAIL redis: declared floor 4.3.0, installed 4.4.0" in capsys.readouterr().err
