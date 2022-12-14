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

from analyzeserver.user.auth_manage import  userauthbp
from analyzeserver.log.export_log import exportbp

from analyzeserver.home.clm_home import clmhomebp
from analyzeserver.clm.transfer import clmtranbp
from analyzeserver.clm.clm_list import clmlisttbp

from analyzeserver.data_board.data_board_settings import boardbp
from analyzeserver.lianghao.tran_summary import transummarybp
from analyzeserver.lianghao.eight.second.tran_summary import sectransumbp

from analyzeserver.lianghao.eight.second.data_board import boardsecondbp
from analyzeserver.lianghao.data_board import lhpersonboardbp
from analyzeserver.lianghao.quota import quotabp

app = Flask(__name__,instance_relative_config=True)
CORS(app, supports_credentials=True)

#??????
@app.route('/')
def hello():
    return 'Hello, World!'

# ????????????????????????
app.register_blueprint(boardbp)

# 7???
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
app.register_blueprint(transummarybp)
app.register_blueprint(lhpersonboardbp)
app.register_blueprint(quotabp)

#8???????????????
app.register_blueprint(letypebp)

# 8?????????
app.register_blueprint(platsecondbp)
app.register_blueprint(personsecondbp)
app.register_blueprint(daily_eight_secondbp)
app.register_blueprint(secondppbp)
app.register_blueprint(sectransumbp)
app.register_blueprint(boardsecondbp)

# 8?????????
app.register_blueprint(platpfbp)
app.register_blueprint(personpfbp)
app.register_blueprint(daily_eight_pifabp)
app.register_blueprint(pifappbp)

# crm??????????????????
app.register_blueprint(operateconbp)

#??????
app.register_blueprint(homebp)
app.register_blueprint(lhhomebp)
app.register_blueprint(lhpfhome8)
app.register_blueprint(clghomebp)

# ??????
app.register_blueprint(syslogbp)
app.register_blueprint(exportbp)

# ??????
app.register_blueprint(uploadmodubp)
app.register_blueprint(chaddrbp)
app.register_blueprint(usertagbp)


#?????????
app.register_blueprint(clgtranshopbp)
app.register_blueprint(clgtranplatbp)
app.register_blueprint(clglistbp)
app.register_blueprint(clgtrangoodbp)
app.register_blueprint(clgtranuserbp)
app.register_blueprint(clgorderdbp)

# ?????????
app.register_blueprint(clmhomebp)
app.register_blueprint(clmtranbp)
app.register_blueprint(clmlisttbp)

app.register_blueprint(userauthbp)
if __name__ == "__main__":
    # app.run()
    app.run(host='0.0.0.0', port=8000, debug=True, threaded=True)
