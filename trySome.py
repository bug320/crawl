#-*- coding:utf-8 -*-
from Crawl import *
from program import *
#from bs4 import UnicodeDammit
import MySQLdb

def returnMysql2():
    #hdb = hbase_mar.HbaseClient()
    #hdb.create_table("subpage", "page")
    mdb = MySQL.MySQL(host="localhost", usr="root", passwd="root", dbase='bug320')
    k = mdb.selectAll("subpage")
    for i,v in k.items():
        print i,v
    #k = mdb.selectAll("subpage")
    #3for i, v in k.items():
        #print i, v
    #i = 0
    #if True:
    #    #i += 1
    #    #for i, d in enumerate(k.items()):
    #    #   k, v = d
    #    #    print i, k, v
    #    #if i > 5:
    #    #    break
    print "OK"
def returnMysql():
    hdb=hbase_mar.HbaseClient()
    conn = MySQLdb.connect(
        host='localhost',
        port=9090,
        user='root',
        passwd='root',
        db='bug320',
        charset='utf8'
    )
    cur = conn.cursor(cursorclass = MySQLdb.cursors.DictCursor)

    # 获得表中有多少条数据
    aa = cur.execute("select * from subpage")
    print aa

    # 打印表中的多少数据
    info = cur.fetchmany(aa)
    print type(info)
    i =0
    for ii in info:
        i+=1
        if i == 2:
            break
        #print type(ii),ii
        #print ii["title"]
        ##print info
        a,b,c,d,e,f,g,h =ii
        print a,b,c,d,e,f,g,h
        #title url id person flag user time gongsi
        print ii[a],ii[b],ii[c],ii[d],ii[e],ii[f],ii[g],ii[h]
        print type(ii[b])
        #print "%s" % ii[b]
        #print "%s" %ii[b].encode("utf-8")
        html = download(ii[b].encode("utf-8"),headers)
        if html == None:
            print "download false"
        soup = BeautifulSoup(html,htmlparser)
        if soup == None:
            print "soup false"
        urls=getInnerPageURLs(soup)[0][0]
        #print urls
        #print type(urls)

        # the innner html URl
        url = "URL:%s%s" % (mainHTTP, urls.encode('utf-8'))

        #print "OK"
        #url = "%s%s" % (mainHTTP, urls.encode('utf-8'))
        print url


        #hdb.put("subpage","%s"%ii[c],{"page:%s"%b:"%s"%ii[b],"page:%s"%d:"%s"%ii[d].encode("utf-8"),"page%s"%h:"%s" % ii[h].encode("utf-8")})
    #info = cur.fetchall()[0]
    #print info["title"]
    #for k,v in info.items():
    #    print info[k]
    cur.close()
    conn.commit()
    conn.close()
    #return info
    pass


if __name__ == "__main__":
    #hdb = hbase_mar.HbaseClient()
    #hdb.create_table("subpage", "page")
    for i in range(5):
        a=returnMysql()

    #for i in range(5):
    #    a,b,c,d,e,f,g,h = returnMysql2()
    #    a1,a2 = a
    #    b1,b2 = b
    #    print a,b,c,d,e,f,g,h
    #    print a1,a2
    #for i in xrange(5):

    #print type(returnMysql())
    #for i in returnMysql():
    #    for k in i:
    #        print k,
    #    print ""

    # try:
    #     #saveIntoMySQL(SaveSubURLs)
    #     #saveIntoHbase(SaveSubURLs)
    #     pass
    # except Exception as e:
    #     print e
    #     pass
    # mdb = MySQL.MySQL(host="localhost", usr="root", passwd="root", dbase='bug320')
    # k=mdb.select("subpage")
    # i=0
    # while True:
    #     i+=1
    #     for i,d in enumerate(k.items()):
    #         k,v= d
    #         print i,k,v
    #     if i > 5:
    #        break
    # print "OK"
    # hdb=hbase_mar.HbaseClient()
    # hdb.create_table("subpage","page")
    #hdb.put("website","0",{"info:title":"bug320 test"})
    #hdb.put("website","0",{"info:infosize":"很大"})
    #hdb.put("website","1",{"info:title":"wed320","info:infosize":"很小"})
    #for i in hdb.scan("website","0"):
    #    print  i["info:infosize"]
    #print hdb.scan("website","0")
    #hdb.create_table("mycaigou", "page")#hdb.put("mycaigou", "www.baidu.com", {"page:title": "woaizhongguo", "page:content": "qqqqqqqqqq", "page:org": 'hn',"page:name": "www", "page:date": "xxx"})
    pass
