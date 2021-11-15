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
import asyncio
import sqlite3
import time

from aiohttp import ClientSession


def create_table_if_not_exist(con, table):
    cur = con.cursor()
    if table == "rich_info":
        # 创建数据库，狗狗币富豪榜
        sql = """CREATE TABLE IF NOT EXISTS rich_info (top TEXT, 
              addr TEXT, 
              balance INTEGER, 
              trans INTEGER, 
              addr_type TEXT)"""
        cur.execute(sql)
    elif table.endswith('_trans_info'):
        # 创建钱包交易信息数据库
        sql = """CREATE TABLE IF NOT EXISTS %s (hash TEXT, 
              doges INTEGER, 
              ustdts INTEGER, 
              trans_time TEXT, 
              trans_date TEXT, 
              from_wallets TEXT)""" % table
        cur.execute(sql)


def get_doge_rich_info(top=100):
    """
    默认：获取狗狗币前100名的相关信息
    :param top:
    :return: [[top1, addr, num_trans, type, num_doges]]
    """
    ret_rich_data = []
    basse_url = 'https://doge.tokenview.com/api/address/richrange/doge/%s/10'
    for i in range(top // 10):
        url = basse_url % (i + 1)
        response = requests.get(url)
        json_data = json.loads(response.text)
        for index, data in enumerate(json_data['data']):
            balance = data['balance']
            num_trans = data['txCnt']
            addr = data['addr']
            balance = int(balance)  # balance 取整
            addr_type = 'wallet' if num_trans <= 5000 else 'market'  # 交换量大于5000 认为是交易所
            ret_rich_data.append(('Top%s' % (i * 10 + index + 1), addr, balance, num_trans, addr_type))

    return ret_rich_data


def insert_data_to_db(con, table, data):
    """
    将数据写入数据库
    :param data:
    :return:
    """
    cur = con.cursor()
    if table.lower() == "rich_info":
        create_table_if_not_exist(con, 'rich_info')
        cur.executemany('INSERT INTO rich_info VALUES (?,?,?,?,?)', data)
        con.commit()


def update_data_to_db(con, table, data, addr=None):
    cur = con.cursor()
    if table.lower() == "rich_info":
        create_table_if_not_exist(con, "rich_info")
        for da in data:
            top, addr, balance, trans, addr_type = da
            sql = "UPDATE rich_info SET addr='%s', balance=%s, trans=%s, addr_type='%s' WHERE top='%s'" % (
                addr, balance, trans, addr_type, top
            )
            print(sql)
            cur.execute(sql)
        con.commit()
    elif table.lower() == "trans_info":
        # 创建数据库，钱包交易数据库
        create_table_if_not_exist(con, "%s_trans_info" % addr)
        cur.executemany('INSERT INTO %s_trans_info VALUES (?,?,?,?,?,?)' % addr, data)
        con.commit()


def get_trans_detail_info(response, latest, wallet):
    """
    获取每个钱包的交易的详细信息
    """
    ret_trans_detail = []
    for repos in response:
        json_data = json.loads(repos)
        for trans, data_info in json_data['data'].items():
            tmp_trans_data = {}
            transaction_data = data_info['transaction']
            inputs_data = data_info['inputs']
            outputs_data = data_info['outputs']

            # 计算交易时的价格& 交易完成时间; 若最新交易时间与数据库中一致则不需要更新
            trans_time = transaction_data['time']
            if trans_time == latest:
                break
            trans_date = transaction_data['date']
            price = transaction_data['fee_usd'] / (transaction_data['fee'] / 100000000)
            # 将发送者的doge至为负数
            for data in inputs_data:
                data['value'] *= -1

            # 遍历汇总地址的交易信息
            for data in inputs_data + outputs_data:
                addr = data['recipient']
                if addr in tmp_trans_data:
                    tmp_trans_data[addr] += data['value'] / 100000000
                else:
                    tmp_trans_data[addr] = data['value'] / 100000000
            # 转账金额小于1000 直接跳过
            if abs(tmp_trans_data[wallet]) < 1000:
                continue
            recived_flag = True if tmp_trans_data[wallet] > 0 else False
            # 保存交易记录 trans, doge usd time date from_wallet
            tmp_ret_info = []
            tmp_from_wallet = []
            for tmp_wallet, doges in tmp_trans_data.items():
                if tmp_wallet == wallet:
                    tmp_ret_info = [trans, int(doges), int(price * doges), trans_time, trans_date]
                elif recived_flag and doges < 0:
                    tmp_from_wallet.append(tmp_wallet)
                elif not recived_flag and doges > 0:
                    tmp_from_wallet.append(tmp_wallet)
            tmp_ret_info.append('|'.join(tmp_from_wallet))
            ret_trans_detail.append(tmp_ret_info)

    return ret_trans_detail


def get_wallet_latest_trans_time(cur, wallet):
    ret_latest_trans_time = ''
    try:
        sql = 'SELECT trans_time FROM %s_trans_info ORDER BY trans_time' % wallet
        latest_trnas_data = cur.execute(sql)
        ret_latest_trans_time = latest_trnas_data[0]
    except Exception as e:
        print(e)
    return ret_latest_trans_time


async def aio_get_url(url):
    # 异步请求url
    async with ClientSession() as session:
        async with session.get(url) as response:
            if response.status != 200:
                print(response)
            return await response.text()


def get_doge_rich_trans_info(con):
    """
    获取/更新 狗狗币top100的交易记录
    :return:
    """
    ret_trans_data = {}
    cur = con.cursor()
    # 获取前100名的 个人钱包地址
    sql = 'SELECT top, addr FROM rich_info WHERE addr_type="wallet"'
    cur.execute(sql)
    top_data = cur.fetchall()

    base_url = 'https://api.blockchair.com/dogecoin/dashboards/address/%s?limit=5000'
    for da in top_data:
        try:

            top, addr = da
            url = base_url % addr
            response = requests.get(url)
            json_data = json.loads(response.text)
            trans_list = json_data['data'][addr]['transactions']
            # 获取该钱包的最新交易时间，若新的交易时间与当前交易时间一致则直接退出
            latest_trans_time = get_wallet_latest_trans_time(cur, addr)

            tasks = []
            detail_base_url = 'https://api.blockchair.com/dogecoin/dashboards/transaction/%s'
            loop = asyncio.get_event_loop()

            # 将列表分组
            result = []
            step = 5
            tmp_trans_list = [trans_list[i:i + step] for i in range(0, len(trans_list), step)]
            for transes in tmp_trans_list:
                for trans in transes:
                    detail_url = detail_base_url % trans
                    task = asyncio.ensure_future(aio_get_url(detail_url))
                    tasks.append(task)
                result = loop.run_until_complete(asyncio.gather(*tasks))
                time.sleep(10)

            trans_detail = get_trans_detail_info(result, latest_trans_time, addr)
            print(top+'_'+addr)
            print(trans_detail)
            update_data_to_db(con, 'trans_info', trans_detail, addr)
        except Exception as e:
            print("[%s]: %s" % (top+'_'+addr, e))
        # ret_trans_data['%s' % addr] = trans_detail

    return ret_trans_data


def main():
    con = sqlite3.connect("dogecoin.db")
    # 富豪榜
    rich_data = get_doge_rich_info()
    # insert_data_to_db(con, 'rich_info', rich_data)  # 第一次使用
    update_data_to_db(con, 'rich_info', rich_data)  # 更新用

    # 富豪榜个人交易记录
    rich_trans_data = get_doge_rich_trans_info(con)
    # update_data_to_db(con, 'trans_info', rich_trans_data)


if __name__ == "__main__":
    main()