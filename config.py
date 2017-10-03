# -*- coding: utf-8 -*-

import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

_data_dirname = 'data'
DATA_DIRNAME = os.path.join(BASE_DIR, _data_dirname)

_tmp_data_dirname = 'tmp'
TMP_DATA_DIRNAME = os.path.join(DATA_DIRNAME, _tmp_data_dirname)
