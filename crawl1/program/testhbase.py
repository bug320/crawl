#!/usr/bin/python
import sys
import time
from thrift import Thrift
from thrift.transport import TSocket, TTransport
from thrift.protocol import TBinaryProtocol
from hbase import ttypes
from hbase.Hbase import *


def create_all_tables(client):
    '''store table'''
    table_name = 'store_table'
    col_name = ['store:']
    create_table(client, table_name, col_name)
    table_name2 = 'item_table'
    col_name2 = ['item:', 'item_page:']
    create_table(client, table_name2, col_name2)


def drop_all_tables(client):
    tables = ['store_table', 'item_table']
    for table in tables:
        if client.isTableEnabled(table):
            print "    disabling table: %s" % (table)
            client.disableTable(table)
        print "    deleting table: %s" % (table)
        client.deleteTable(table)


def create_table(client, table_name, table_desc):
    columns = []
    for name in table_desc:
        col = ColumnDescriptor(name)
        columns.append(col)
    try:
        print "creating table: %s" % (table_name)
        client.createTable(table_name, columns)
    except AlreadyExists, ae:
        print "WARN: " + ae.message
    except Exception, e:
        print "error happend: " + str(e)


def list_all_tables(client):
    # Scan all tables
    print "scanning tables..."
    for t in client.getTableNames():
        print "  -- %s" % (t)
        cols = client.getColumnDescriptors(t)
        print "  -- -- column families in %s" % (t)
        for col_name in cols.keys():
            col = cols[col_name]
            print "  -- -- -- column: %s, maxVer: %d" % (col.name, col.maxVersions)


def connect_hbase():
    # Make socket
    transport = TSocket.TSocket('localhost', 9090)
    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)
    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    # Create a client to use the protocol encoder
    client = Client(protocol)
    # Connect!
    transport.open()
    return client


def printRow(entry):
    print "row: " + entry.row + ", cols:",
    for k in sorted(entry.columns):
        print k + " => " + entry.columns[k].value,
    print "/n"


def scann_table(client):
    t = 'store_table'
    columnNames = []
    for (col, desc) in client.getColumnDescriptors(t).items():
        print "column with name: " + desc.name
        print desc
        columnNames.append(desc.name + ":")
    print columnNames

    print "Starting scanner..."
    scanner = client.scannerOpenWithStop(t, "", "", columnNames,None)

    r = client.scannerGet(scanner)
    i = 0
    while r:
        if i % 100 == 0:
            printRow(r[0])
        r = client.scannerGet(scanner)
        i += 1

    client.scannerClose(scanner)
    print "Scanner finished, total %d row" % i


if __name__ == '__main__':
    client = connect_hbase()
    create_all_tables(client)
    list_all_tables(client)
    scann_table(client)