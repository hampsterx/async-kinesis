import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

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


@pytest.mark.parametrize("bump,expected", [("patch", "3.2.2"), ("minor", "3.3.0"), ("major", "4.0.0")])
def test_increment_version(bump, expected):
    assert release.increment_version("3.2.1", bump) == expected


def _git(repo, *args):
    subprocess.run(
        ["git", "-c", "user.name=test", "-c", "user.email=test@example.com", *args],
        cwd=repo,
        check=True,
        capture_output=True,
    )


# A tag holding a Unicode space must not split into a bare "10.0.0"
TAGS = ["2.5.6", "3.0.0", "v9.9.9", "10.0.0-rc1", "junk\u200310.0.0"]


@pytest.fixture
def tagged_repo(tmp_path, monkeypatch):
    # Signing or other user config would make the fixture commit and the helper's
    # bare `git tag` fail for reasons unrelated to the code under test
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", os.devnull)
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    _git(tmp_path, "init", "-q")
    (tmp_path / "setup.py").write_text('setup(version="1.1.5")\n')
    _git(tmp_path, "add", "setup.py")
    _git(tmp_path, "commit", "-q", "-m", "init")
    for tag in TAGS:
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
def test_refuses_version_not_newer_than_latest_tag(tagged_repo, monkeypatch, capsys, version):
    monkeypatch.setattr(sys, "argv", ["release.py", "--version", version, "--skip-tests", "--skip-lint"])
    monkeypatch.setattr(release, "create_tag", lambda v: pytest.fail("create_tag must not be reached"))

    with pytest.raises(SystemExit) as exc:
        release.main()

    assert exc.value.code == 1
    assert f"{version} is not newer than the latest release 3.0.0" in capsys.readouterr().out


REPO_ROOT = Path(__file__).resolve().parent.parent
CLEAN_MODULE = "VALUE = 1\n"
WORKFLOWS = REPO_ROOT / ".github" / "workflows"


@pytest.fixture
def lint_tree(tmp_path, monkeypatch):
    # run_linting() runs the tools by bare name, so resolve them from this interpreter's environment
    monkeypatch.setenv("PATH", f"{os.path.dirname(sys.executable)}{os.pathsep}{os.environ['PATH']}")
    (tmp_path / "kinesis").mkdir()
    (tmp_path / "tests").mkdir()
    (tmp_path / "pyproject.toml").write_text(
        '[tool.black]\nline-length = 120\n\n[tool.isort]\nprofile = "black"\nline_length = 120\n'
    )
    # E711 is in no list the helper or CI has ever passed, so only this file can excuse it
    (tmp_path / ".flake8").write_text("[flake8]\nmax-line-length = 120\nextend-ignore = E711\n")
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_run_linting_passes_clean_tree(lint_tree):
    (lint_tree / "kinesis" / "module.py").write_text(CLEAN_MODULE)

    assert release.run_linting() is True


def test_run_linting_takes_ignores_from_flake8_config(lint_tree):
    (lint_tree / "kinesis" / "module.py").write_text("VALUE = 1\nprint(VALUE == None)\n")

    assert release.run_linting() is True


@pytest.mark.parametrize("directory", ["kinesis", "tests"])
@pytest.mark.parametrize(
    "source,message",
    [
        ("VALUE=1\n", "Black formatting check failed!"),
        ("import sys\nimport os\n\nprint(os, sys)\n", "isort check failed!"),
        ("VALUE = 1\nprint(VALUE == True)\n", "flake8 check failed!"),
    ],
    ids=["black", "isort", "flake8"],
)
def test_run_linting_fails_on_each_tool(lint_tree, capsys, directory, source, message):
    (lint_tree / directory / "module.py").write_text(source)

    assert release.run_linting() is False
    assert message in capsys.readouterr().out


@pytest.mark.parametrize(
    "source",
    [
        "#" + " word" * 30 + "\n",  # pycodestyle exempts a long comment with no spaces
        "try:\n    VALUE = 1\nexcept:\n    VALUE = 2\n",
    ],
    ids=["E501", "E722"],
)
def test_run_linting_with_repo_config_rejects_what_ci_rejects(lint_tree, capsys, source):
    shutil.copy(REPO_ROOT / ".flake8", lint_tree / ".flake8")
    (lint_tree / "kinesis" / "module.py").write_text(source)

    assert release.run_linting() is False
    assert "flake8 check failed!" in capsys.readouterr().out


def test_ci_flake8_step_passes_no_options():
    steps = re.findall(r"^\s*run: (flake8\b.*)$", (WORKFLOWS / "test.yml").read_text(), re.M)

    assert steps == ["flake8 kinesis tests"]


def test_flake8_hook_passes_no_args():
    hook = (REPO_ROOT / ".pre-commit-config.yaml").read_text().split("id: flake8", 1)[1].split("- repo:", 1)[0]

    assert "args" not in hook


def test_publish_waits_on_the_tests_workflow():
    publish = (WORKFLOWS / "publish.yml").read_text()

    assert re.search(r"^  tests:\n    uses: \./\.github/workflows/test\.yml$", publish, re.M)
    assert re.search(r"^  publish:\n    needs: tests$", publish, re.M)
    assert re.search(r"^  workflow_call:$", (WORKFLOWS / "test.yml").read_text(), re.M)


def test_release_tests_get_a_read_only_token():
    publish = (WORKFLOWS / "publish.yml").read_text()

    assert re.search(r"^  tests:\n    uses: [^\n]+\n    permissions:\n      contents: read$", publish, re.M)
    assert re.search(r"^permissions:\n  contents: read$", (WORKFLOWS / "test.yml").read_text(), re.M)
