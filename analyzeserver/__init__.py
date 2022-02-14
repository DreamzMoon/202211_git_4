# -*- coding: utf-8 -*-

# @Time : 2021/10/20 13:31

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : __init__.py.py

from flask import Flask
from flask_cors import *
import sys

sys.path.append("../")
sys.path.append(".")

from analyzeserver.other.user import userbp
from analyzeserver.lianghao.tranferorder import tobp
from analyzeserver.lianghao.operationcenter import opbp
from analyzeserver.lianghao.personal_market import pmbp
from analyzeserver.perfect_choose.center import ocbp
from analyzeserver.perfect_choose.lh_type import typebp
from analyzeserver.user.sysuser import sysuserbp
from analyzeserver.home.lh7_home_page import homebp
from analyzeserver.user.crmuserrelate import userrelatebp
from analyzeserver.lianghao.personal_property import ppbp
from analyzeserver.lianghao.daily_summary import dailybp
from analyzeserver.home.lh8_second_home_page import lhhomebp
from analyzeserver.lianghao.eight.second.platorder import platsecondbp
from analyzeserver.lianghao.eight.second.person_market import personsecondbp
from analyzeserver.lianghao.eight.pifa.platorder import platpfbp
from analyzeserver.lianghao.eight.pifa.person_market import personpfbp
from analyzeserver.home.lh7_home_page import homebp
from analyzeserver.home.lh8_second_home_page import lhhomebp
from analyzeserver.home.lh8_wholesale_home import lhpfhome8
from analyzeserver.user.operationcenter_control import operateconbp
from analyzeserver.log.system_log import syslogbp
from analyzeserver.public.uploadfile import uploadmodubp
from analyzeserver.public.chinaaddress import chaddrbp
from analyzeserver.user.crm_user_tag import usertagbp
from analyzeserver.lianghao.eight.second.daily_summary import daily_eight_secondbp
from analyzeserver.lianghao.eight.pifa.daily_summary import daily_eight_pifabp
from analyzeserver.lianghao.eight.pifa.personal_property import pifappbp
from analyzeserver.lianghao.eight.second.personal_property import secondppbp
from analyzeserver.perfect_choose.le_type import letypebp
from analyzeserver.home.clg_home import clghomebp
from analyzeserver.clg.shop_tran import clgtranshopbp

from analyzeserver.clg.plat_tran import clgtranplatbp
from analyzeserver.clg.clg_list import clglistbp
from analyzeserver.clg.good_tran import clgtrangoodbp
from analyzeserver.clg.user_tran import clgtranuserbp
from analyzeserver.clg.orderwater import clgorderdbp

app = Flask(__name__,instance_relative_config=True)
CORS(app, supports_credentials=True)

#测试
@app.route('/')
def hello():
    return 'Hello, World!'


# 7位
app.register_blueprint(userbp)
app.register_blueprint(tobp)
app.register_blueprint(opbp)
app.register_blueprint(pmbp)
app.register_blueprint(ocbp)
app.register_blueprint(typebp)
app.register_blueprint(sysuserbp)
app.register_blueprint(homebp)
app.register_blueprint(userrelatebp)
app.register_blueprint(ppbp)
app.register_blueprint(dailybp)

#8位类型列表
app.register_blueprint(letypebp)

# 8位二手
app.register_blueprint(platsecondbp)
app.register_blueprint(personsecondbp)
app.register_blueprint(daily_eight_secondbp)
app.register_blueprint(secondppbp)

# 8位批发
app.register_blueprint(platpfbp)
app.register_blueprint(personpfbp)
app.register_blueprint(daily_eight_pifabp)
app.register_blueprint(pifappbp)

# crm运营中心管理
app.register_blueprint(operateconbp)

#首页
app.register_blueprint(homebp)
app.register_blueprint(lhhomebp)
app.register_blueprint(lhpfhome8)
app.register_blueprint(clghomebp)

# 日志
app.register_blueprint(syslogbp)

# 公共
app.register_blueprint(uploadmodubp)
app.register_blueprint(chaddrbp)
app.register_blueprint(usertagbp)


#诚聊购
app.register_blueprint(clgtranshopbp)
app.register_blueprint(clgtranplatbp)
app.register_blueprint(clglistbp)
app.register_blueprint(clgtrangoodbp)
app.register_blueprint(clgtranuserbp)
app.register_blueprint(clgorderdbp)
if __name__ == "__main__":
    # app.run()
    app.run(host='0.0.0.0', port=8000, debug=True, threaded=True)
