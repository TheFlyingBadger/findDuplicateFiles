from pathlib import Path
__version__ = Path(__file__).parent.parent.joinpath(
    'VERSION').read_text().rstrip()


def getVersion():
    return __version__
