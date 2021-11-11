#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
=================================================
@Project -> File   ：Tushare -> doge_helper
@IDE    ：PyCharm
@Author ：Mr.Snail
@Date   ：2021/11/10 21:32
@Desc   ：
==================================================
"""
import requests
import json

import sqlite3


def get_doge_rich_info(top=100):
    """
    默认：获取狗狗币前100名的相关信息
    :param top:
    :return: [[top1, addr, num_trans, type, num_doges]]
    """
    ret_rich_data = []
    basse_url = 'https://doge.tokenview.com/api/address/richrange/doge/%s/10'
    for i in range(top//10):
        url = basse_url % (i+1)
        response = requests.get(url)
        json_data = json.loads(response.text)
        for index, data in enumerate(json_data['data']):
            balance = data['balance']
            num_trans = data['txCnt']
            addr = data['addr']
            balance = int(balance)  # balance 取整
            addr_type = 'wallet' if num_trans <= 5000 else 'market'  # 交换量大于5000 认为是交易所
            ret_rich_data.append(('Top%s' % (i*10 + index+1), addr, balance, num_trans, addr_type))

    return ret_rich_data


def insert_data_to_db(con, table, data):
    """
    将数据写入数据库
    :param data:
    :return:
    """
    cur = con.cursor()
    if table.lower() == "rich_info":
        cur.executemany('INSERT INTO rich_info VALUES (?,?,?,?,?)', data)
    con.commit()


def update_data_to_db(con, table, data):
    cur = con.cursor()
    if table.lower() == "rich_info":
        for da in data:
            top, addr, balance, trans, addr_type = da
            sql = "UPDATE rich_info SET addr='%s', balance=%s, trans=%s, addr_type='%s' WHERE top='%s'" % (
                addr, balance, trans, addr_type, top
            )
            print(sql)
            cur.execute(sql)
    con.commit()


def get_db_handle():
    """
    获取数据的光标
    :return:
    """
    con = sqlite3.connect("dogecoin.db")
    cur = con.cursor()
    # 创建数据库，狗狗币富豪榜
    sql = "CREATE TABLE IF NOT EXISTS rich_info(top TEXT PRIMARY KEY," \
          "addr TEXT," \
          "balance INTEGER," \
          "trans INTEGER," \
          "addr_type TEXT)"
    cur.execute(sql)
    return con

def get_trans_detail_info(trans_list):
    """
    获取每个交易的详细信息
    :param trans_list:
    :return:
    """
    ret_trans_detail = {}
    base_url = 'https://api.blockchair.com/dogecoin/dashboards/transaction/%s'
    for trans in trans_list:
        url = base_url % trans
        response = requests.get(url)
        json_data = json.loads(response.text)

def get_doge_rich_trans_trans_info(con):
    """
    获取狗狗币top100的交易记录
    :return:
    """
    ret_trans_data = {}
    cur = con.cursor()
    sql = 'SELECT top, addr FROM rich_info WHERE addr_type="wallet"'
    top_data = cur.execute(sql)

    base_url = 'https://api.blockchair.com/dogecoin/dashboards/address/%s?limit=5000'
    for da in top_data:
        _, addr = da
        url = base_url % addr
        response = requests.get(url)
        json_data = json.loads(response.text)
        trans_list = json_data['data'][addr]['transactions']
        trans_detail = get_trans_detail_info(trans_list)
        ret_trans_data['%s' % addr] = trans_detail

    return ret_trans_data


def main():
    con = get_db_handle()
    # 富豪榜
    # rich_data = get_doge_rich_info()
    # insert_data_to_db(con, 'rich_info', rich_data)  # 第一次使用
    # update_data_to_db(con, 'rich_info', rich_data)  # 更新用

    # 富豪榜个人交易记录
    rich_trans_data = get_doge_rich_trans_trans_info(con)
    update_data_to_db(con, '', rich_trans_data)
    pass


if __name__ == "__main__":
    main()