#-*- coding:utf-8 -*-
import re
import shelve
import MySQLdb
from bs4 import BeautifulSoup
from bs4 import UnicodeDammit
import downloader
import crawler
MYSQL_TABLE = 'org1'
def selectAllTitle(table=MYSQL_TABLE,**attr):
    # csvfile = open('demo.html', 'w')
    try:
        conn = MySQLdb.connect("localhost", "root", "root", "bug320", charset='utf8')
        cur = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        # 查看有总共有几条记录
        cur.execute("SELECT count(*) FROM org1" )

        size = cur.fetchall()[0]
        size = size["count(*)"]
        sql = "SELECT * from org1 WHERE %s='%s'" % (attr.keys()[0],attr.values()[0])
        cur.execute(sql)
        count = 0
        for idict in  cur.fetchall():
            print count,
            for k,v in idict.iteritems():
                print " %s %s " % (k, v)
                # csvfile.write("<a href= '%s'> %d</a> <br/>" % (v,count))
                count += 1
                print "These have %s Title", count
            print ""
            pass
        pass
    except Exception as e:
        conn.rollback()
        print "mysql error --- ", e
        # 这里不能返回 否则数据库就没有关闭
        pass
    finally:
        cur.close()
        conn.close()
        # csvfile.close()
        pass
    pass

def getAll():
    # csvfile = open('demo.html', 'w')
    org = u"河南招标采购服务有限公司".encode('utf-8')
    result = []
    try:
        conn = MySQLdb.connect("localhost", "root", "root", "bug320", charset='utf8')
        cur = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        # 查看有总共有几条记录
        # cur.execute("SELECT count(*) FROM org1 WHERE title='%s' " % (title))
        cur.execute("SELECT count(*) FROM website")

        size = cur.fetchall()[0]
        size = size["count(*)"]
        # title = "%-225s" % title
        # print title
        sql = "SELECT * from website WHERE org='%s'" % (org)
        # sql = "SELECT DISTINCT title from org1"

        cur.execute(sql)
        count = 0
        for idict in cur.fetchall():
            result.append(idict)
            pass
        return result[:]
        pass
    except Exception as e:
        conn.rollback()
        print "mysql error --- ", e
        # 这里不能返回 否则数据库就没有关闭
        pass
    finally:
        cur.close()
        conn.close()
        pass
    pass
    pass


def saveIntoMySQL(itDict,table):


    keys = ""
    values = ""

    for key, value in itDict.items():
        keys = keys + "," + key
        values = values + ",\"" + value + "\""
        pass
    keys = keys[1:]
    values = values[1:]
    # keys += ",mysql,hbase"
    # values += ',"1","0"'
    # print keys
    # print values
    # return

    try:
        conn = MySQLdb.connect("localhost", "root", "root", "bug320", charset='utf8')
        cur = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        sql =  "SELECT url from %s where url = '%s' " % (table,itDict["url"])
        # print sql
        cur.execute(sql)
        p = cur.fetchall()
        if p == ():
            # print "p is empty"
            sql = "INSERT INTO %s( %s ) VALUES( %s )" % (table, keys, values)
            cur.execute(sql)
            conn.commit()
        else:
            print " %s have save into the mysql" % itDict["url"]
        # print p
        pass
    except Exception as e:
        conn.rollback()
        print "mysql error --- ", e
        # 这里不能返回 否则数据库就没有关闭
        pass
    finally:
        cur.close()
        conn.close()
    pass


def getOrgAll(person):
    result = []
    try:
        conn = MySQLdb.connect("localhost", "root", "root", "bug320", charset='utf8')
        cur = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        # 查看有总共有几条记录
        # cur.execute("SELECT count(*) FROM org1 WHERE title='%s' " % (title))
        # cur.execute("SELECT count(*) FROM org1")

        # sql = "SELECT * from org1 WHERE person ='%s'" % (person)
        sql = "SELECT * from org1"
        cur.execute(sql)
        count = 0
        for idict in cur.fetchall():
            result.append(idict)
            pass
        return result[:]
        pass
    except Exception as e:
        conn.rollback()
        print "mysql error --- ",e
        # 这里不能返回 否则数据库就没有关闭
        pass
    finally:
        cur.close()
        conn.close()
        pass
    pass


if __name__ == "__main__":
    # 从 website 插入数据到 org1
    # k = getAll()
    # for i in k :
    #     del i["id"]
    #     i["mysql"]='1'
    #     i["hbase"]='0'
    #     saveIntoMySQL(i,MYSQL_TABLE)
    # 按发部人分只有两个 刘哥 38 和 张博 16
    k = getOrgAll(person=u'刘歌'.encode('utf-8'))
    count = 0
    liu,zhang= [],[]
    l,z = 0,0
    person = set()

    # /home/bug320/Desktop/groupWeb/person
    # lshe  = shelve.open("/home/bug320/Desktop/groupWeb/person/liu/liu.she","c")
    # zshe  = shelve.open("/home/bug320/Desktop/groupWeb/person/zhang/zhang.she","c")

    lshe  = open("/home/bug320/Desktop/groupWeb/person/liu/liu.html","a")
    zshe  = open("/home/bug320/Desktop/groupWeb/person/zhang/zhang.html","w")

    for i in k:
        person.add(i["person"].encode('utf-8'))
        if i["person"] == u'刘歌':
            liu.append(i)
            l += 1
            row = u"<a href='%s' >%s</a><br>\n" % (i["url"],i["title"])
            lshe.write("%s" % row.encode('utf-8') )
            # lshe.write(u"<a href='%s' >%s</a><br>" % (i["url"],i["title"]))
            pass
        if i["person"] == u'张博':
            zhang.append(i)
            z += 1
            row = u"<a href='%s' >%s</a><br>\n" % (i["url"], i["title"])
            print row
            zshe.write("%s" % row.encode('utf-8'))
            pass
        count += 1
    lshe.close()
    zshe.close()
    print "Ok"
    # print count,liu,zhang,len(person)
    # for i in person:
    #     print i,

    pass