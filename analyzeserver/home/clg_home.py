# -*- coding: utf-8 -*-

# @Time : 2022/1/25 15:05

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : clg_home.py


import sys
sys.path.append("..")
sys.path.append("../../")
from flask import *
from config import *
import traceback
from util.help_fun import *
import time
import datetime
from datetime import timedelta
from analyzeserver.common import *
from analyzeserver.user.sysuser import check_token
from analyzeserver.common import *
import threading


clghomebp = Blueprint('clghome', __name__, url_prefix='/clghome')

