# (C) Copyright 2010 Bewype <http://www.bewype.org>

# python import
import getopt, os, sys

# config obj
from ConfigObject import config_module

# debug flag for config loader
# -> set to False in development mode (unit test)
DEBUG = True

# current dir
CONF_DIR = os.path.join('.', 'config')


def _update_filenames(config_path, filenames, env=None):
    # prepare env path
    _path = config_path if env is None else os.path.join(config_path, env)
    # little check
    if os.path.exists(_path):
        pass
    else:
        return
    # only files in the passed path
    for _f in os.listdir(_path):
        # get file path
        _f_path = os.path.join(_path, _f)
        # is a dir
        if os.path.isdir(_f_path):
            continue
        # found config file
        elif os.path.splitext(_f)[-1] == '.ini':
            # new path for config
            filenames.append(_f_path)
        # not a config file
        else:
            continue


def _get_filenames(config_path):
    # init list
    _filenames = list()
    # update list
    _update_filenames(config_path, _filenames)
    # env
    _env = 'dev' if DEBUG is True else 'prod'
    # update list again
    _update_filenames(config_path, _filenames, env=_env)
    # return if
    return _filenames


# singleton flag
__initialized__ = False

# do init
if __initialized__ is False:
    # check config path
    if os.path.exists(CONF_DIR):
        # get filenames
        _f_names = _get_filenames(CONF_DIR)
        # init config obj
        config_module(__name__, __file__, *_f_names)
        # ...
        __initialized__ = True
    else:
        pass
else:
    pass
