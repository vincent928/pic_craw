#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
@File  : download_gif.py
@Author: moon
@Date  : 2019/1/24 22:48
@Desc  : 动图下载
"""
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor

# 引入线程池进行下载任务
import pymongo
import redis
import requests

local_path = "f:\\file\\duowan_gif\\"
header = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
    "Host": "s1.dwstatic.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36",
    "Upgrade-Insecure-Requests": "1"
}

# redis连接池
# def connect_redis():
try:
    connection_pool = redis.ConnectionPool(host='172.81.238.222', password='moon@163.com', port='6777', db=2,
                                           decode_responses=True)
    print("Redis....connected success!")
except Exception as error:
    print("Redis....connected failed!", error)
redis_client = redis.Redis(connection_pool=connection_pool)
DUOWAN_GIF_SET_KEY = 'DUOWAN_GIF_SET_KEY'


# 连接mongodb
def connect_mongodb():
    mongo_client = pymongo.MongoClient("mongodb://localhost:27017")
    duowan_ = mongo_client["duowan"]
    return duowan_["duowan_gif"]


re_compile = re.compile(r"[|,|！|～|`|'|、|.|，|…|”|“|？|?|：|。|_|~|（|）|(|)|!|》|《|>|<|\"|+|=|/]")


# 下载gif
def download_gif(result):
    global local_path
    global header
    global re_compile
    title = str(result['title'])
    url = str(result['url'])
    title = re.sub(re_compile, "", title)
    file_name = local_path + title.replace(" ", "") + ".gif"
    # 比较是否重复
    if redis_client.sismember(DUOWAN_GIF_SET_KEY, file_name) is True:
        # 已经存在
        print("当前文件已经下载:", file_name)
        return

    if os.path.exists(file_name):
        redis_client.sadd(DUOWAN_GIF_SET_KEY, file_name)
        print("当前文件已经下载:", file_name)
    else:
        try:
            response = requests.get(url, headers=header)
            if response.status_code == 200:
                content = response.content
                response.close()
                print(file_name, ",当前线程为:", threading.current_thread().getName())
                with open(file_name, 'wb') as fp:
                    fp.write(content)
                redis_client.sadd(DUOWAN_GIF_SET_KEY, file_name)
                print(file_name, "下载完成！>>>")
        except Exception as error:
            print("发生错误:", error)


if __name__ == '__main__':
    mongodb = connect_mongodb()
    results = mongodb.find()

    start = time.clock()
    executor = ThreadPoolExecutor(max_workers=6)
    executor.map(download_gif, results)

    print("=======================================================================================")
    print("！！！！！！！！！！！！！！！！当前图片已经全部下载完毕！！！！！！！！！！！！！！！！！！")
    print("=======================================================================================")
    end = time.clock()
    print("耗时：", (end - start))
