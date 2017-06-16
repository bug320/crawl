#!/usr/bin/python
# coding: utf-8
'''
获取采购信息-<结果公告>下的目标网页链接的列表
'''
import requests
from bs4 import BeautifulSoup
import re
import urllib
import urllib2
import time
import MySQLdb
import MySQLdb.cursors
#建立与thriftserver端的连接
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.protocol import TCompactProtocol
from hbase import Hbase
from hbase.ttypes import *
#对hbase的具体操作，比如查表，删表，插入Ｒｏｗ　Ｋｅｙ
from hbase.ttypes import ColumnDescriptor, Mutation, BatchMutation, TRegionInfo
from hbase.ttypes import IOError, AlreadyExists
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


class HbaseClient(object):
    def __init__(self, host='localhost', port=9090):
        transport = TTransport.TBufferedTransport(TSocket.TSocket(host, port))
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        self.client = Hbase.Client(protocol)
        transport.open()

    def get_tables(self):           #获取表
        return self.client.getTableNames()

    def create_table(self, table, *columns):            #创建表
        self.client.createTable(table, map(lambda column: ColumnDescriptor(column), columns))

    def put(self, table, row, columns, attributes=None):            #添加记录
        self.client.mutateRow(table, row, map(lambda (k,v): Mutation(column=k, value=v), columns.items()))

    def scan(self, table, start_row="", columns=None, attributes=None):         #获取记录
        scanner = self.client.scannerOpen(table, start_row, columns)
        while True:
            r = self.client.scannerGet(scanner)
            if not r:
                break
            yield dict(map(lambda (k, v): (k, v.value),r[0].columns.items()))

if __name__ == "__main__":
    client = HbaseClient("localhost", 9090)
    # client.create_table("information1", "title", "xinxi")

    print(client.get_tables())
    for v in client.scan("information1", columns=["title"]):
        print v["title:"]
        continue

    #
    # db = MySQLdb.connect(host='localhost', user='root', passwd='665687', db='caigou', charset='utf8')
    # db.autocommit(True)
    # cursor = db.cursor()
    # total = cursor.execute("select * from xinxi")  # 统计总数据数目
    # results = cursor.fetchmany(total)
    # Nu_url= 0
    # for r in results:
    #         a=r[0]
    #         urls = r[2]# 从mysql提取连接，从连接提取文本，放入hbase,并改变state的状态，
    #         cursor.execute("update xinxi set state = 'not' where id='%s'  "%a)
    #         titles = r[1].encode("utf-8")
    #         Nu_url+=1
    #         print "已经提取了id为%s的数据"%Nu_url
    #
    #         target_res = urllib2.urlopen(urls)
    #         newres = target_res.read()
    #         soup3 = BeautifulSoup(newres, "lxml")
    #
    #         agency = soup3.find_all("span", class_="Blue")[0].text.encode("utf-8")
    #         publishe = soup3.find_all("span", class_="Blue")[1].text.encode("utf-8")
    #         datailed_time = soup3.find_all("span", class_="Blue")[2].text.encode("utf-8")
    #
    #
    #         Dynamic_link = re.findall("/webfile/(.+).htm", newres)
    #         for url_first in Dynamic_link:
    #             url_second = "http://www.ccgp-henan.gov.cn/webfile/{url}.htm"  # 设置动态网页中不变的信息，变得信息用{url}填充
    #             news = url_second.format(url=url_first)  # 把url1（动态变换的地址）填入动态网页连接的不变地方（url)
    #             res2 = requests.get(news)
    #             res2.encoding = "utf-8"
    #             soup2 = BeautifulSoup(res2.text, "lxml")
    #             b = 0
    #             for txt in soup2.select("p"):
    #                 txts=txt.text.encode("utf-8")
    #                 client.put("information1", "%s"%b, {"title:": "%s"%titles, "xinxi:txt":"%s"%txts,"xinxi:time": "%s"%datailed_time,"xinxi:agency":"%s"%agency,"xinxi:publishe":"%s"%publishe})
    #                 b+=1
    #                 print txts