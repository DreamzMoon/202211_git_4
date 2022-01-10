# -*- coding: utf-8 -*-

# @Time : 2022/1/10 10:19

# @Author : xiaowangwang
# @Email : www@qq.com
# @File : uploadfile.py

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
import base64
import oss2
import time

uploadmodubp = Blueprint('upload', __name__, url_prefix='/upload')

# auth = oss2.Auth(AccessKeyID, AccessKeySecret) #详见文档
# endpoint = 'https://luke-analyze.oss-cn-beijing.aliyuncs.com/' #  地址
# bucket = oss2.Bucket(auth, endpoint, 'luke-analyze') # 项目名称


auth = oss2.Auth(AccessKeyID, AccessKeySecret)
bucket = oss2.Bucket(auth, endpoint, 'luke-analyze')

@uploadmodubp.route("img",methods=["POST"])
def upload_img():
    try:
        logger.info(request.json)
        token = request.headers["Token"]
        user_id = request.json["user_id"]

        if not user_id and not token:
            return {"code": "10001", "status": "failed", "msg": message["10001"]}

        check_token_result = check_token(token, user_id)
        if check_token_result["code"] != "0000":
            return check_token_result

        # 1 头像 2：身份证正面 3：身份证反面 4：人脸
        type = request.json.get("type")
        unionid = request.json.get("unionid")
        img = request.json.get("img")
        img_data = base64.b64decode(img)

        # 1 身份证前面  2身份证反面
        front_back = request.json.get("front_back")


        # if not front_back or not type or not unionid or not img:
        #     return {"code":"10001","msg":message["10001"],"status":"failed"}

        t = int(time.time()*1000)


        if type == 2 or type ==3:
            front_back = "front" if int(front_back) == 1 else "back"
        file_name = ""
        if type == 1:
            file_name = "usericon" + str(t)
        elif type == 2 or type == 3:
            file_name = "identify" + str(front_back) + str(t)
        elif type == 4:
            file_name = "userface" + str(t)
        else:
            return {"code":"11030","message":message["11030"],"status":"failed"}
        #如果是身份证正反面

        try:
            bucket.put_object('userinfo/%s/%s.jpg' %(unionid,file_name), img_data)
            return_url = "https://luke-analyze.oss-cn-beijing.aliyuncs.com/userinfo/%s/%s.jpg" %(unionid,file_name)
            return {"code":"0000","msg":"上传成功","data":return_url,"status":"success"}
        except:
            return {"code": "11031", "msg": message["11031"], "status": "failed"}

    except Exception as e:
        logger.error(e)
        logger.exception(traceback.format_exc())
        # 参数名错误
        return {"code": "10000", "status": "failed", "msg": message["10000"]}



