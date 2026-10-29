import os
from pathlib import Path

import pytest

from serverless_chaos import env


@pytest.fixture
def project(tmp_path, monkeypatch):
    """A fake project: <tmp>/proj/{pyproject.toml,.env} with a nested working directory. Package-root lookup disabled."""
    proj = tmp_path / "proj"
    (proj / "a" / "b").mkdir(parents=True)
    (proj / "pyproject.toml").write_text("[project]\nname='x'\n")
    monkeypatch.setattr(env, "_package_root", lambda: None)
    monkeypatch.delenv("SC_ENV_FILE", raising=False)
    monkeypatch.delenv("SC_REAL_AWS", raising=False)
    for k in ("SC_ENDPOINT_URL", "SC_LLM_MODEL", "SC_LLM_API_KEY", "LOCALSTACK_AUTH_TOKEN", "AWS_ACCESS_KEY_ID",
              "AWS_SECRET_ACCESS_KEY", "AWS_DEFAULT_REGION"):
        monkeypatch.delenv(k, raising=False)
    return proj


def test_finds_project_env_from_any_subdirectory(project, monkeypatch):
    (project / ".env").write_text("SC_LLM_MODEL=llama3.1\n")
    monkeypatch.chdir(project / "a" / "b")
    assert env.load_env() == project / ".env" and os.environ["SC_LLM_MODEL"] == "llama3.1"


def test_explicit_environment_wins_and_empty_values_are_ignored(project, monkeypatch):
    (project / ".env").write_text("# comment\nexport SC_LLM_MODEL = 'from-file'\nSC_ENDPOINT_URL=http://file:4566\nSC_LLM_API_KEY=\nLOCALSTACK_AUTH_TOKEN=abc=def\n")
    monkeypatch.setenv("SC_ENDPOINT_URL", "http://explicit:4566")
    monkeypatch.chdir(project)
    env.load_env()
    assert os.environ["SC_ENDPOINT_URL"] == "http://explicit:4566"        # explicit value preserved
    assert os.environ["SC_LLM_MODEL"] == "from-file"                        # export prefix + quotes handled
    assert os.environ["LOCALSTACK_AUTH_TOKEN"] == "abc=def"                 # '=' inside a value survives
    assert "SC_LLM_API_KEY" not in os.environ                               # empty value ignored


def test_localstack_defaults_applied_unless_overridden_or_real_aws(project, monkeypatch):
    monkeypatch.chdir(project)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-1")
    env.setup_environment()
    assert os.environ["AWS_ACCESS_KEY_ID"] == "test" and os.environ["SC_ENDPOINT_URL"] == "http://localhost:4566"
    assert os.environ["AWS_DEFAULT_REGION"] == "eu-west-1"                  # explicit region kept
    for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "SC_ENDPOINT_URL"):
        monkeypatch.delenv(k)
    monkeypatch.setenv("SC_REAL_AWS", "1")
    env.setup_environment()
    assert "SC_ENDPOINT_URL" not in os.environ                              # real AWS: no LocalStack endpoint forced


def test_does_not_pick_up_unrelated_env_files(project, tmp_path, monkeypatch):
    (tmp_path / ".env").write_text("SC_LLM_MODEL=stray\n")                  # parent dir without a project marker
    monkeypatch.chdir(project / "a")
    assert env.load_env() is None and "SC_LLM_MODEL" not in os.environ


def test_sc_env_file_is_authoritative_and_none_disables(project, tmp_path, monkeypatch):
    other = tmp_path / "custom.env"; other.write_text("SC_LLM_MODEL=custom\n")
    (project / ".env").write_text("SC_LLM_MODEL=project\n")
    monkeypatch.chdir(project)
    monkeypatch.setenv("SC_ENV_FILE", str(other))
    env.load_env()
    assert os.environ["SC_LLM_MODEL"] == "custom"
    monkeypatch.delenv("SC_LLM_MODEL"); monkeypatch.setenv("SC_ENV_FILE", "none")
    assert env.load_env() is None and "SC_LLM_MODEL" not in os.environ


def test_all_entry_points_use_the_shared_loader():
    root = Path(__file__).resolve().parents[2]
    for rel in ("src/serverless_chaos/cli.py", "examples/items_app/deploy.py", "scripts/smoke_localstack.py"):
        assert "setup_environment" in (root / rel).read_text(), f"{rel} must call setup_environment()"
