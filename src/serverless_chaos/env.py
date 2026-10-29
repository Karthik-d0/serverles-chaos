"""Environment setup shared by the CLI, deploy.py, scripts and integration tests.

* Finds the project-root `.env` no matter which directory you run from.
* Never overrides variables that are already set (explicit environment always wins; empty values in .env are ignored).
* Fills in LocalStack-friendly defaults (dummy AWS credentials, region, endpoint) so you never export them by hand.
  Set SC_REAL_AWS=1 to skip those defaults when targeting real AWS.
* SC_ENV_FILE=/path/to/file makes that file the only candidate (SC_ENV_FILE=none disables .env loading).
"""
import os
from pathlib import Path
from typing import Iterator, Optional

LOCALSTACK_DEFAULTS = {
    "AWS_ACCESS_KEY_ID": "test",
    "AWS_SECRET_ACCESS_KEY": "test",
    "AWS_DEFAULT_REGION": "us-east-1",
    "SC_ENDPOINT_URL": "http://localhost:4566",
}


def _package_root() -> Optional[Path]:
    """Repo root when running from a source checkout / editable install (…/src/serverless_chaos/env.py)."""
    root = Path(__file__).resolve().parents[2]
    return root if (root / "pyproject.toml").exists() else None


def _candidates(cwd: Optional[Path] = None) -> Iterator[Path]:
    explicit = os.getenv("SC_ENV_FILE")
    if explicit:
        yield Path(explicit)
        return
    root = _package_root()
    if root:
        yield root / ".env"
    start = (cwd or Path.cwd()).resolve()
    for d in [start, *start.parents]:
        if (d / "pyproject.toml").exists():       # project markers only, so we never pick up an unrelated ~/.env
            yield d / ".env"


def find_env_file(cwd: Optional[Path] = None) -> Optional[Path]:
    return next((p for p in _candidates(cwd) if p.is_file()), None)


def load_env(path: Optional[Path] = None) -> Optional[Path]:
    """Load KEY=VALUE lines into os.environ without overriding existing variables. Returns the file used."""
    path = path or find_env_file()
    if not path:
        return None
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export "):]
        key, value = line.split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        if value:
            os.environ.setdefault(key.strip(), value)
    return path


def setup_environment() -> Optional[Path]:
    path = load_env()
    if os.getenv("SC_REAL_AWS") != "1":
        for k, v in LOCALSTACK_DEFAULTS.items():
            os.environ.setdefault(k, v)
    return path
