# -*- coding:utf-8 -*-

import shelve
import re
import MySQLdb
import csv
from bs4 import BeautifulSoup
from bs4 import UnicodeDammit

import downloader
import crawler

#
# <h1 class="TxtCenter Padding10 BorderEEEBottom Top5">气相色谱、紫外线可见分光光度计、
# 恒温培养箱等采购项目询价结果公示</h1>

htmlparser= "html.parser"
# 提取脚本中要 innerHTML 部分的 url
endUrlCmp=re.compile(r'\$\.get\(\"(.*\.htm)')
# 把 innerHTML 的 url 转换成绝对 url
mainHTTP = "http://www.hngp.gov.cn"
MYSQL_TABLE = 'website'


def getInnerPageURLs(soup):
	links= []
	for string in soup.stripped_strings:
		dammit = UnicodeDammit(string)
		tmpurl=endUrlCmp.findall(dammit.unicode_markup)
		if tmpurl != [] and tmpurl not in links:
			links.append(tmpurl)
	return links

download = downloader.Downloader()

def getBaseInfo(link):
    attr = [link]  # url,title=0,?,org=2,person=3,post_time=4,check_post=5 ,inner_html_url=6 #2 不知道是啥,添加完后要删掉
    keys = ["url","title","org","person","post_time","check_post","inner_html_url"] # 但是输出时不一定按这个顺序
    # print soup.prettify()
    try:
        soup = BeautifulSoup(download(link),htmlparser)
    except Exception as e:
        print "getBaseInfo :",e
        return None
        pass
    else:
        attr.append(soup.find("h1", attrs={"class": "TxtCenter Padding10 BorderEEEBottom Top5"}).string.encode("utf-8"))  # class="TxtCenters
        # print link
        # print "attr[0]",type(attr[0])
        attr.append(string.string for string in soup.find_all("span",attrs={"class":"Blue"}))
        i=0
        for zstring in soup.find_all("span", attrs={"class": "Blue"}):
            i += 1
            # if str(type(zstring)) == "<type 'generator'>":
            attr.append(zstring.string.encode("utf-8"))
            # print zstring.string.encode("utf-8")
            # print "atr[%s]"  % i ,
            # print type(attr[i])
            pass
        inner_url = getInnerPageURLs(soup)[0][0]
        inner_url = inner_url.encode("utf-8")
        inner_url = "%s%s" % (mainHTTP,inner_url)

        del attr[2]  # 执行后 title=0,org=1,person=2,time=3  # 原来的 [2] 不知道是啥
        attr.append(inner_url)
        result = dict(zip(keys, attr))
        return result
        # print "attr[2] = ",attr[2],"type = ",type(attr[2])
        # print attr[-1]
        #
        # print len(attr)
        # isi = 0
        # for at in attr:
        #     print isi,
        #     print at
        #     isi += 1
        #
        # print type(result)
        # print result.keys()
        # print result.values()
        # for i in result.keys():
        #     print "key=",i,"value=",result[i]
        # pass
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
    keys += ",mysql,hbase"
    values += ',"1","0"'
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



def selectAllFromMySQL(table):
    try:
        conn = MySQLdb.connect("localhost", "root", "root", "bug320", charset='utf8')
        cur = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        # 查看有总共有几条记录
        cur.execute("SELECT count(*) FROM %s" % (table))
        size = cur.fetchall()[0]
        size = size["count(*)"]

        sql = "SELECT * from %s " % (table)
        cur.execute(sql)

        for idict in  cur.fetchall():
            for k,v in idict.iteritems():
                print "%-20s %-10s" % (k, v)

            print ""
            pass
        print "These have %s L",size
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


def selectAllTitle(table,org):
    # csvfile = open('demo.html', 'w')
    try:
        conn = MySQLdb.connect("localhost", "root", "root", "bug320", charset='utf8')
        cur = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        # 查看有总共有几条记录
        # cur.execute("SELECT count(*) FROM website WHERE title='%s' " % (title))
        cur.execute("SELECT count(*) FROM website" )

        size = cur.fetchall()[0]
        size = size["count(*)"]
       # title = "%-225s" % title
        #print title
        sql = "SELECT url from website WHERE org='%s'" % (org)
        # sql = "SELECT DISTINCT title from website"

        cur.execute(sql)
        count = 0
        for idict in  cur.fetchall():
            for k,v in idict.iteritems():
                print "%d  %s %s " % (count,k, v)
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
        #spamwriter.close()
        # csvfile.close()
        pass
    pass


if __name__ == "__main__":
    seed_url = 'http://www.hngp.gov.cn/henan/ggcx?appCode=H60&channelCode=0102'
    crawl = crawler.Crawler(seed_url)

    print "Try to get the subpage html:"
    psdb = shelve.open("page.she","r")

    for k,v in psdb.iteritems():
        try:
            i = 0
            i += 1
            print "No.", i, "url s is start to dealing", v
            for link in crawl.getEachPageUrls(slink=v):
                print link
                idict = getBaseInfo(link)
                saveIntoMySQL(idict, MYSQL_TABLE)
            pass
        except Exception as e:
            lshe = shelve.open("log.she", "c")
            lshe[str(i)] = v
            lshe.sync()
            lshe.close()
            print "main error :",e
            pass
        finally:
            # selectAllFromMySQL(MYSQL_TABLE[:])
            # selectAllTitle(MYSQL_TABLE[:], u"河南招标采购服务有限公司".encode('utf-8'))
            pass

        pass

    pass