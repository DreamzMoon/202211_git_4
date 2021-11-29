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

app = Flask(__name__,instance_relative_config=True)
CORS(app, supports_credentials=True)

#测试
@app.route('/')
def hello():
    return 'Hello, World!'

@app.route('/hellook')
def hellohello():
    return 'hellohello'


from analyzeserver.other.user import userbp
app.register_blueprint(userbp)

from analyzeserver.lianghao.tranferorder import tobp
app.register_blueprint(tobp)

from analyzeserver.lianghao.operationcenter import opbp
app.register_blueprint(opbp)

from analyzeserver.lianghao.personal_market import pmbp
app.register_blueprint(pmbp)

from analyzeserver.perfect_choose.center import ocbp
app.register_blueprint(ocbp)

from analyzeserver.perfect_choose.type import typebp
app.register_blueprint(typebp)

from analyzeserver.user.sysuser import sysuserbp
app.register_blueprint(sysuserbp)

from analyzeserver.home.home_page import homebp
app.register_blueprint(homebp)

if __name__ == "__main__":
    # app.run()
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)
