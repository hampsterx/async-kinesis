import subprocess
import sys

import pytest

from scripts import release


@pytest.mark.parametrize("version", ["3.0.1", "10.0.0", "0.0.1"])
def test_parse_version_accepts_bare_versions(version):
    assert release.parse_version(version) == tuple(int(part) for part in version.split("."))


@pytest.mark.parametrize(
    "version",
    [
        "v3.0.1",
        "3.0.1-dev",
        "3.0",
        "3.0.1\n",
        "٣.٠.١",  # Arabic-Indic digits: git accepts the tag, packaging does not
    ],
)
def test_parse_version_rejects_non_bare_versions(version):
    with pytest.raises(ValueError):
        release.parse_version(version)


def test_parse_version_orders_numerically():
    assert release.parse_version("10.0.0") > release.parse_version("9.9.9")


def _git(repo, *args):
    subprocess.run(
        ["git", "-c", "user.name=test", "-c", "user.email=test@example.com", *args],
        cwd=repo,
        check=True,
        capture_output=True,
    )


@pytest.fixture
def tagged_repo(tmp_path, monkeypatch):
    _git(tmp_path, "init", "-q")
    (tmp_path / "setup.py").write_text('setup(version="1.1.5")\n')
    _git(tmp_path, "add", "setup.py")
    _git(tmp_path, "commit", "-q", "-m", "init")
    for tag in ["2.5.6", "3.0.0", "v9.9.9", "10.0.0-rc1"]:
        _git(tmp_path, "tag", tag)
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_current_version_is_highest_bare_tag_not_setup_py(tagged_repo):
    assert release.get_current_version() == "3.0.0"


def test_next_patch_tags_clean_checkout_without_touching_setup_py(tagged_repo, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["release.py", "--next-patch", "--skip-tests", "--skip-lint"])
    monkeypatch.setattr("builtins.input", lambda prompt: "n")

    release.main()

    tags = subprocess.run(["git", "tag", "--points-at", "HEAD"], capture_output=True, text=True, check=True)
    assert "3.0.1" in tags.stdout.split()
    assert (tagged_repo / "setup.py").read_text() == 'setup(version="1.1.5")\n'
    status = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True, check=True)
    assert status.stdout == ""


@pytest.mark.parametrize("version", ["3.0.0", "2.9.9"])
def test_refuses_version_not_newer_than_latest_tag(tagged_repo, monkeypatch, version):
    monkeypatch.setattr(sys, "argv", ["release.py", "--version", version, "--skip-tests", "--skip-lint"])

    with pytest.raises(SystemExit) as exc:
        release.main()

    assert exc.value.code == 1
    tags = subprocess.run(["git", "tag", "--list"], capture_output=True, text=True, check=True)
    assert sorted(tags.stdout.split()) == sorted(["2.5.6", "3.0.0", "v9.9.9", "10.0.0-rc1"])
