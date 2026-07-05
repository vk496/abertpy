from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

_HARDCODED_KEY = "abertpy"
_HARDCODED_PMT = 8000

try:
    __version__ = _pkg_version("abertpy")
except PackageNotFoundError:  # running from a raw checkout, not installed
    __version__ = "0.0.0+unknown"
