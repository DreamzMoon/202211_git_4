# -*- coding: utf-8 -*-
# @Time : 2021/11/1  10:31
# @Author : shihong
# @File : .py
# --------------------------------------
import sys
sys.path.append(".")
sys.path.append("../")
from flask import *
from config import *
import traceback
from util.help_fun import *
import time
import datetime

lhbp = Blueprint('lh', __name__, url_prefix='/lh')