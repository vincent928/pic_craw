#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
@File  : duowan_jpg_mongodb.py
@Author: moon
@Date  : 2019/1/25 11:53
@Desc  : 爬取多玩吐槽图片入库
"""

import requests
import json
import pymongo
import re
from bs4 import BeautifulSoup

# 连接mongodb
def connect_mongodb():
    mongo_client = pymongo.MongoClient("mongodb://localhost:27017")
    duowan_ = mongo_client["duowan"]
    return duowan_["duowan_jpg"]

# 获取gid集合 offset / 30
def get_gids(offset):
    url = 'http://tu.duowan.com/m/tucao'
    if offset is not None:
        params = {
            "order": "created",
            "offset": offset
        }
        header = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Host": "tu.duowan.com",
            "Referer": "http://tu.duowan.com/m/tucao",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest"
        }
        response = requests.get(url, params=params, headers=header)
        code = response.status_code
        if code == 200:
            content = response.content.decode("utf-8")
            html = BeautifulSoup(content, "lxml")
            find_all = html.findAll(name='a', attrs={"href": re.compile(r'^http://tu.duowan.com/gallery/')})
            list_ = []
            for a_tag in find_all:
                re_compile = re.compile('href="http://tu.duowan.com/gallery/(.*?).html".*?', re.S)
                findall = re.findall(re_compile, str(a_tag)).pop()
                list_.append(findall)
            return sorted(set(list_), key=list_.index)


# 解析网页数据
def get_document(gid):
    url = 'http://tu.duowan.com/index.php'
    params = {
        "r": "show/getByGallery/",
        "gid": gid
    }
    header = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Host": "tu.duowan.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest"
    }
    response = requests.get(url, params=params, headers=header)
    code = response.status_code
    if code == 200:
        json_loads = json.loads(response.content, encoding='UTF-8')
        return True, json_loads['picInfo']
    else:
        return False, None


# 获取json数据并进行解析
def get_json(id_, gid):
    url = 'http://tu.duowan.com/index.php'
    params = {
        "r": "show/getByGallery/",
        "gid": gid
    }
    header = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Host": "tu.duowan.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest"
    }
    response = requests.get(url, params=params, headers=header)
    code = response.status_code
    if code == 200:
        json_loads = json.loads(response.content, encoding='UTF-8')
        list_dict = []
        for pic_json in json_loads['picInfo']:
            id_ += 1
            mygif_ = {
                "_id": id_,
                "title": pic_json['add_intro'],
                "url": pic_json['url']
            }
            list_dict.append(mygif_)
        return list_dict, id_


def main(mongo_client, id):
    id_ = id
    print("========================================")
    print("========================================")
    print("=======准备执行多玩jpg吐槽图片入库任务=======")
    print("========================================")
    print("========================================")
    for offset in range(0, 210, 30):
        # 获取gid集合
        gids = get_gids(offset)
        for gid in gids:
            print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
            list_dict, id_ = get_json(id_, gid)
            mongo_client.insert_many(list_dict)
            print("当前gid:", gid, "的数据已经成功插入!")
            print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>...")
    print("=============================================")
    print("===========多玩jpg吐槽图片已经全部入库!===========")
    print("=============================================")


if __name__ == '__main__':
    mongodb = connect_mongodb()
    try:
        id__next = mongodb.find({}, {"_id": 1}).sort('_id', -1).next()
    except Exception as e:
        id__next = {"_id": 0}
    main(mongodb, id__next['_id'])





















