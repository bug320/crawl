# file name：hbaseconn.py
# ! /usr/bin/env python
# -*- coding: utf-8 -*-
# 提供建立连接的方法和取数据操作的方法
import logging
import traceback
import time, sys
from unittest import TestCase, main
import socket

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from hbase import Hbase

from hbase.ttypes import IOError as HbaseIOError, AlreadyExists
from hbase.ttypes import ColumnDescriptor, Mutation, BatchMutation

_TABLE = 'thrift_check'
_ROW = 'test_for_thrift'
_DATA = 'data:word'
_VALUE = 'Flag'


class DbConnError(Exception):
    """Conn Db exception. 

    Timeout or any except for connection from low layer api
    """
    pass


class Connection:
    def __init__(self, trans, client, addr, port):
        self.trans = trans
        self.client = client
        self.hp = addr
        self.port = port
        pass


class CenterDb:
    @classmethod
    def open(cls, host_port):

        cls.tc_list = []
        for hp in host_port:
            trans, client = cls._open(*hp)
            cls.tc_list.append(Connection(trans, client, hp[0], hp[1]))

        return cls.tc_list

    @classmethod
    def _open(cls, host, port):
        transport = TSocket.TSocket(host, port)
        transport.setTimeout(5000)
        trans = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(trans)
        client = Hbase.Client(protocol)
        ok = False
        try:
            trans.open()
            ok = True
        except TSocket.TTransportException, e:
            logerr('CenterDb(Hbase) Open error(%s, %d)' % (host, port))
            ok = False
        else:
            pass
        # dbg('CenterDb connected (%s, %d)' % (host, port))
        return trans, client

    @classmethod
    def _initTable(cls, client):
        dat = ColumnDescriptor(name='data', maxVersions=1)
        tmp = [Mutation(column=_DATA, value=_VALUE)]
        try:
            client.createTable(_TABLE, [dat])
            client.mutateRow(_TABLE, _ROW, tmp)
            dbg("Create Table For Thrift Test Success!")
            return True
        except AlreadyExists:
            return True
        return False

    @classmethod
    def _get(cls, client):
        client.getVer(_TABLE, _ROW, _DATA, 1)
        return True

    @classmethod
    def _reconnect(cls, trans):
        trans.close()
        trans.open()

    def __init__(self, transport, client):
        self.t = transport
        self.c = client

    def __str__(self):
        return 'CenterDb.open(%s, %d)' % (self.t, self.c)

    def __del__(self):
        self.t.close()

    def getColumns(self, table, row):

        tr_list = []
        tr_list = self._failover('getRow', table, row)
        if (not tr_list):
            return {}
        return tr_list[0].columns