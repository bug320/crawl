#!/usr/bin/python
# _*_ coding: UTF-8 _*_

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from hbase import Hbase
from hbase.ttypes import ColumnDescriptor, Mutation


class HbaseClient(object):
    def __init__(self, host='localhost', port=9090):
        transport = TTransport.TBufferedTransport(TSocket.TSocket(host, port))
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        self.client = Hbase.Client(protocol)
        transport.open()

    def get_tables(self):
        """
        获取所有表
        """
        return self.client.getTableNames()

    def create_table(self, table, *columns):
        """
        创建表
        """
        self.client.createTable(table, map(lambda column: ColumnDescriptor(column), columns))

    def put(self, table, row, columns,attributes=None):
        """
        添加记录
        @:param columns {"k:1":"11"}
        """
        self.client.mutateRow(table, row, map(lambda (k,v): Mutation(column=k, value=v), columns.items()),attributes=None)

    def scan(self, table, start_row="", columns=None, attributes=None):
        """
        获取记录
        """

        scanner = self.client.scannerOpen(table, start_row, columns,attributes)
        while True:
            r = self.client.scannerGet(scanner)
            if not r:
                break
            yield dict(map(lambda (k, v): (k, v.value),r[0].columns.items()))
#192.168.242.128
if __name__ == "__main__":
    client = HbaseClient("localhost", 9090)
    #client.create_table("mycaigou", "page")
    print(client.get_tables())
    #client.put("mycaigou", "www.baidu.com", {"page:title": "woaizhongguo", "page:content": "qqqqqqqqqq", "page:org": 'hn',"page:name": "www", "page:date": "xxx"})
    for i in client.scan("mycaigou","www.baidu.com"):
        print i
    # create_table(self, table, *columns):
    # self.client.createTable(table, map(lambda column: ColumnDescriptor(column), columns))