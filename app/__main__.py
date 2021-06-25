from functions import *
from version import getVersion

if __name__ == '__main__':
    CONFIG = getConfig(Path('__file__').parent.parent.joinpath('config.yaml'))
    ic(getVersion())
