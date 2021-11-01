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


# from analyzeserver.demo import demobp
# app.register_blueprint(demobp)

from analyzeserver.other.user import userbp
app.register_blueprint(userbp)

from analyzeserver.lianghao.tranferorder import lhbp
app.register_blueprint(lhbp)

if __name__ == "__main__":
    # app.run()
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)
