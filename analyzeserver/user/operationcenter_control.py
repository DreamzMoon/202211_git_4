# -*- coding: utf-8 -*-
# @Time : 2021/12/28  17:31
# @Author : shihong
# @File : .py
# 运营中心管理增删改查
# --------------------------------------
# 查
import os, sys, json
father_dir = os.path.dirname(os.path.dirname(__file__)).split("/")[-1]
sys.path.append(sys.path[0].split(father_dir)[0])
from flask import *
from analyzeserver.common import *
from config import *
import traceback
from util.help_fun import *
import pandas as pd
from analyzeserver.user.sysuser import check_token


userrelatebp = Blueprint('userrelate', __name__, url_prefix='/user/relate')