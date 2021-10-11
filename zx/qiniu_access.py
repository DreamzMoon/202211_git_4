from qiniu import Auth, put_file, BucketManager
from config import *
import hashlib

class QiniuAccess(object):
    # AccessKey
    ak = qn_access_key
    # SecretKey
    sk = qn_secre_key
    # 上传空间
    bucket = qn_bucket
    url = qn_url

    # 生成上传凭证
    def qiniu_token(self, key):
        '''
        :param bucked_name: 上传空间
        :param key: 上传至七牛后保存的文件名
        :return:
        '''
        q = Auth(access_key=self.ak, secret_key=self.sk)
        token = q.upload_token(self.bucket, key)
        return token

    def upload_img_for_internet(self, img_url, file_name):
        '''
        从网络获取图片
        :param img_url: 图片地址
        :param file_name: 图片名
        :return:
        '''
        q = Auth(access_key=self.ak, secret_key=self.sk)
        bm = BucketManager(q)
        file_name_split = file_name.split('.')
        key = self.md5(file_name_split[0]) + '.' +file_name_split[1]
        ret, info = bm.fetch(img_url, self.bucket, key)
        try:
            img_key = self.url + ret.get('key')
            return img_key
        except Exception as e:
            logger.error(f'图片上传异常，异常原因：{e}--上传结果：{ret},异常URL：{img_url}')
            return False

    def upload_img(self, file_path, file_name):
        '''
        从本地获取图片
        :param file_path: 本地图片地址
        :return:
        '''
        # 指定上传空间，获取token
        token = self.qiniu_token(file_name)
        ret, info = put_file(token, file_name, file_path)
        img_key = self.url + ret.get('key')
        return img_key

    def md5(self, strs):
        m = hashlib.md5()
        b = strs.encode(encoding='utf-8')
        m.update(b)
        return m.hexdigest()

