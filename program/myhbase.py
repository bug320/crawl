from unittest import TestCase, main, result

from http import client
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from hbase import Hbase
from hbase.ttypes import ColumnDescriptor, Mutation, BatchMutation


class HBaseTester:
    def __init__(self, netloc, port, table="staftesttable"):
        self.tableName = table

        self.transport = TTransport.TBufferedTransport(
            TSocket.TSocket(netloc, port))
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
        self.client = Hbase.Client(self.protocol)
        self.transport.open()

        tables = self.client.getTableNames()
        if self.tableName not in tables:
            self.__createTable()

    def __del__(self):
        self.transport.close()

    def __createTable(self):
        name = ColumnDescriptor(name='name')
        foo = ColumnDescriptor(name='foo')

        self.client.createTable(self.tableName,
                                [name, foo])

    def put(self, key, name, foo):
        name = Mutation(column="name:v", value=name)
        foo = Mutation(column="foo:v", value=foo)

        self.client.mutateRow(self.tablename, key, [name, foo])

    def scanner(self, column):
        scanner = client.scannerOpen(self.tablename, "", [column])
        r = client.scannerGet(scanner)
        result = []

        while r:
            print r[0]
            result.append(r[0])
            r = client.scannerGet(scanner)
        print "Scanner finished"
        return result


class TestHBaseTester(TestCase):
    def setUp(self):
        self.writer = HBaseTester("localhost", 9090)

    def tearDown(self):
        name = self.writer.tableName
        client = self.writer.client
        client.disableTable(name)
        client.deleteTable(name)

    def testCreate(self):
        tableName = self.writer.tableName
        client = self.writer.client
        self.assertTrue(self.writer.tableName in client.getTableNames())
        columns = ['name:', 'foo:']
        for i in client.getColumnDescriptors(tableName):
            self.assertTrue(i in columns)

    def testPut(self):
        self.writer.put("r1", "n1", "f1")
        self.writer.put("r2", "n2", "f2")
        self.writer.put("r3", "n3", "")
        self.writer.scanner("name:")


if __name__ == "__main__":
    main()