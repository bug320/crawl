#-*- coding:utf-8 -*-
from thrift import Thrift
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from hbase.ttypes import ColumnDescriptor, Mutation, BatchMutation, TRegionInfo
from hbase.ttypes import IOError, AlreadyExists  \



class MyHbase(object):
    def __init__(self,host="localhost",port="9090"):
        transport = TSocket.TSocket(host, port)
        # 可以设置超时
        transport.setTimeout(5000)
        # 设置传输方式（TFramedTransport或TBufferedTransport）
        trans = TTransport.TBufferedTransport(transport)
        # 设置传输协议
        protocol = TBinaryProtocol.TBinaryProtocol(trans)
        # 确定客户端
        client = Hbase.Client(protocol)
        # 打开连接
        transport.open()
        pass


if __name__ == "__main__":

    """ 链接数据库
    """
    # server端地址和端口
    host="localhost"
    port="9090"
    transport = TSocket.TSocket(host, port)
    # 可以设置超时
    transport.setTimeout(5000)
    # 设置传输方式（TFramedTransport或TBufferedTransport）
    trans = TTransport.TBufferedTransport(transport)
    # 设置传输协议
    protocol = TBinaryProtocol.TBinaryProtocol(trans)
    # 确定客户端
    client = Hbase.Client(protocol)
    # 打开连接
    transport.open()
    print "OK"
    """
    """
    # 获取表名
    client.getTableNames()
    # 创建新表
    _TABLE = "keyword"

    demo = ColumnDescriptor(name='data:', maxVersions=10)  # 列族data能保留最近的10个数据，每个列名后面要跟:号
    createTable(_TABLE, [demo])





