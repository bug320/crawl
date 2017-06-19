# -*-coding:utf-8 -*-
# !/usr/bin/python
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from hbase.ttypes import ColumnDescriptor, Mutation, BatchMutation


class HbaseWriter:
    """ 
            IP地址 
            端口 
            表名 
    """

    def __init__(self, address, port, table='user'):
        self.tableName = table

        # 建立与hbase的连接
        self.transport = TTransport.TBufferedTransport(TSocket.TSocket(address, port))

        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)

        self.client = Hbase.Client(self.protocol)
        self.transport.open()

        tables = self.client.getTableNames()

        if self.tableName not in tables:
            print "not in tables"
            self.__createTable()

        self.write("hell,babay!!!")
        self.read()

        # 关闭

    def __del__(self):
        self.transport.close()

        # 建表

    def __createTable(self):
        col1 = ColumnDescriptor(name="person:", maxVersions=1)
        col2 = ColumnDescriptor(name="contents:", maxVersions=1)
        col3 = ColumnDescriptor(name="info:", maxVersions=1)
        self.client.createTable(self.tableName, [col1, col2, col3])

    def write(self, content):
        """
        #创建列名2个data:url data:word    
        tmp1= [Mutation(column="data:url", value="www.baidu.com")]  
        tmp2= [Mutation(column="data:word", value="YaGer")]  
        #新建2个列 (表名，行键， 列名)  
        client.mutateRow(_TABLE, row, tmp1)  
        client.mutateRow(_TABLE, row, tmp1) 
        :param content: 
        :return: 
        """
        row = "abc"
        mutations = [Mutation(column="person:", value=content), Mutation(column="info:", value=content)]
        self.client.mutateRow(self.tableName, row, mutations)

    def read(self):
        scannerId = self.client.scannerOpen(self.tableName, "", ["contents:", ])
        while True:
            try:
                result = self.client.scannerGet(scannerId)
            except:
                break
            contents = result.columns["contents:"].value
            # print contents
        self.client.scannerClose(scannerId)


if __name__ == "__main__":
    client = HbaseWriter("localhost", "9090", "person")