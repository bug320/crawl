#!/usr/bin/python
# _*_ coding: UTF-8 _*_
import threading
import urllib2
import Queue
import time
import re
from datetime import datetime, timedelta
import mysql_mar
import hbase_mar

import sys
reload(sys)
sys.setdefaultencoding('utf8')


# 打开url链接函数
def UrlRequest(url):
    headers = {
        'Accept-Language': 'zh-CN,zh;q=0.8',
        'Connection': 'keep-alive',
        'Host': 'www.hngp.gov.cn',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko)'
                      ' Chrome/57.0.2987.133 Safari/537.36'
         }
    req = urllib2.Request(url, headers=headers)

    response = urllib2.urlopen(req, timeout=20).read()

    return response

class Crawlers():
    def __init__(self):
        """
        队列
        """
        self.urlqueue = Queue.Queue(maxsize=30)
        """
        线程锁
        """
        self.threadlock = threading.Lock()
        """
        MySQL数据库操作
        """
        self.sqlmanager = mysql_mar.DBmanager()
        """
        Hbase数据库操作
        """
        self.client = hbase_mar.HbaseClient("192.168.242.128", 9090)

    def CrawlerQueue(self):
        time.sleep(2)
        """
        队列处理代码
        """
        judge = True
        while judge:
            if threading.activeCount() < 3:
                break
            i = 0
            judge1 = True
            """
            数据库提取url
            """
            try:
                result = self.sqlmanager.db_select()
            except Exception, e:
                print("Crawlers.CrawlerQueue sqlmanager.db_sto error is:", e)
                break
            while judge1:
                if self.urlqueue.full() is True:
                    #self.urlqueue.join()
                    if threading.activeCount() < 3:
                        judge = False
                        judge1 = False
                        print("Clawlers.ClawlerQueue Error:ClawlerPageHbase is Quit")
                    continue
                    """
                    判断数据库是否为空
                    """
                if len(result) == 0:
                    print("Clawlers.ClawlerQueue DB is None")
                    time.sleep(10)
                    if len(result) == 0:
                        judge = False
                        break
                else:
                    """
                    接收url
                     """
                    urlput = result[i][0]
                    """
                    数据库url入队
                    """
                    self.urlqueue.put(urlput)
                    self.urlqueue.task_done()
                    """
                    判断队列是否为空
                    """
                    if self.urlqueue.empty() is True:
                        print("Clawlers.ClawlerQueue Queue is None")
                        judge1 = False
                i += 1


    def CrawlerPageHbase(self):
        """
        注释：从队列提取链接爬取存入Hbase
        """
        time.sleep(5)
        head = "http://www.hngp.gov.cn"
        judge = True
        while judge:
            """
            判断队列是否为空
            """
            if self.urlqueue.empty() is True:
                time.sleep(10)
                """
                队列10秒内为空退出爬取
                """
                if self.urlqueue.empty() is True:
                    raise Exception("Crwlers.CrawlerPageHbase Queue is none")
            else:
                """
                线程加锁
                """
                self.threadlock.acquire()
                """
                处理多只爬虫取队列， 退出问题
                """
                if self.urlqueue.empty() is True:
                    raise Exception("Crwlers.CrawlerPageHbase queue is None")
                else:
                    """
                    url出队，爬取
                    """
                    myurl = self.urlqueue.get()
                    try:
                        results = self.sqlmanager.select_title(myurl)
                    except Exception, e:
                        print "ClawlerPageHbase select_title:", e
                        break
                    title = results[0][0].encode('GBK')
                    org = results[0][1].encode('GBK')
                    name = results[0][2].encode('GBK')
                    date = results[0][3]
                    print title
                    try:
                        string = UrlRequest(myurl)
                    except Exception, e:
                        print "ClawlerPageHbase UrlRequest:", e
                        break
                    result = re.compile('\$\.get\("(.*?.htm).*?').findall(string)
                    try:
                        page = UrlRequest(head+result[0])
                    except Exception, e:
                        print "ClawlerPageHbase UrlRequest:", e
                        break
                    try:
                        self.client.put("mycaigou", "%s" % myurl, {"page:title": "%s" % title, "page:org": "%s" % org,
                                                                   "page:name": "%s" % name, "page:date": "%s" % date,
                                                                   "page:content": "%s" % page})
                    except Exception, e:
                        print "ClawlerPageHbase put hbase:", e
                        break
                    try:
                        self.sqlmanager.db_update(myurl)
                    except Exception, e:
                        print "ClawlerPageHbase mysql update:", e
                        break

                    print "Thread:%s is %s" % (threading.currentThread(),   myurl)
                    time.sleep(2)
                    """
                    线程释放锁
                    """
                    self.threadlock.release()

    def CrawlerPageRank(self):
        """
        注释：此方法抓取网站新链接并存入MySQL
        """

        """
        信息url 标题 发布机构 发布人 发布日期 容器
        """
        item = {}
        items = []
        """
        数据库查询容器
        """
        result = []
        k = 0
        heads = "http://www.hngp.gov.cn"
        head = "http://www.hngp.gov.cn/henan/"
        m = 0
        judges = True
        while judges:
            l = 0
            items = []
            tables = ["myurl", "myurl1", "myurl2"]
            """
            数据库查询,链接数
            """
            try:
                result = self.sqlmanager.db_select_pagerank()
            except Exception, e:
                print "CrawlerPageRank select_pagerank:", e
                break
            if tables[m] == "myurl":
                url = "http://www.hngp.gov.cn/henan/ggcx?appCode=H60&channelCode=0102&bz=0&pageSize=30&pageNo=%d" % k
            if tables[m] == "myurl1":
                url = "http://www.hngp.gov.cn/henan/ggcx?appCode=H60&channelCode=0102&bz=1&pageSize=30&pageNo=%d" % k
            if tables[m] == "myurl2":
                url = "http://www.hngp.gov.cn/henan/ggcx?appCode=H60&channelCode=0102&bz=2&pageSize=30&pageNo=%d" % k
            judge = True
            while judge:
                time.sleep(2)
                try:
                    string = UrlRequest(url)
                except Exception, e:
                    print "CrawlerPageRank urlrequest:", e
                    judges = False
                    break

                """
                信息网页url获取
                """
                item["url"] = re.compile('<li><span class="Right Gray">.*?</span><a.*?href="(/henan/content\?infoId='
                                         '.*?channelCode=.*?&bz=\d)">.*?</a>',re.S).findall(string)
                item["title"] = re.compile('<li><span class="Right Gray">.*?</span><a.*?href="/henan/content\?infoId='
                                           '.*?channelCode=.*?&bz=\d">(.*?)</a>',re.S).findall(string)
                """
                如果信息url为空退出
                """
                if len(item["url"]) == 0:
                    judge = False
                    continue
                """
                查询数据库已爬取信息url,判断新增的网页是否已经爬取
                """
                if len(result) == 0:
                    print "Crawlers.Crawler_PageRank SQL_Select is None"
                    judge = False
                    continue
                """
                信息url对比
                """
                for j in range(0, len(item['url'])):
                    items.append(heads+item['url'][j])
                # print items[0]
                for i in range(0, len(items)):
                    for j in range(0, len(result)):
                        if items[i] == result[j][0]:
                            l += 1
                for i in range(0, len(item['url'])):
                    urls = heads+item['url'][i]
                    titles = item['title'][i]
                    strings = UrlRequest(urls)
                    list1 = re.compile('发布机构.*?<span class="Blue">(.*?)</span>.*?<span.*?class="Blue">.*?</span>'
                                       '.*?<span class="Blue">.*?</span>', re.S).findall(strings)
                    list2 = re.compile('发布机构.*?<span class="Blue">.*?</span>.*?<span.*?class="Blue">(.*?)</span>'
                                       '.*?<span class="Blue">.*?</span>', re.S).findall(strings)
                    list3 = re.compile('发布机构.*?<span class="Blue">.*?</span>.*?<span.*?class="Blue">.*?</span>'
                                       '.*?<span class="Blue">(.*?)</span>', re.S).findall(strings)
                    try:
                        self.sqlmanager.db_insert(urls, titles, list1[0], list2[0], list3[0])
                    except:
                        continue
                print tables[m]
                print "Same number of links=", l
                k += 1
                if l >= 10:
                    judge = False
                else:
                    continue
            m += 1
            if m == 3:
                break


    def CrawlerPage(self):
        """
        注释：此方法抓取网站链接存入MySQL
        """
        """
        信息url 标题 发布机构 发布人 发布日期 容器
        """
        item = {}
        """
        下一页主URL容器
        """
        urlnext = []
        heads = "http://www.hngp.gov.cn"
        head = "http://www.hngp.gov.cn/henan/"
        k = 0
        judges = True
        while judges:
            tables = ["myurl", "myurl1", "myurl2"]
            """
            查看数据库信息url是否存在
            """
            try:
                result = self.sqlmanager.db_select_myurl(tables[k])
            except Exception, e:
                print "CrawlerPage selectmyurl:", e
                break
            """
            如果不存在将url设置为默认
            """
            if len(result) == 0:
                print len(result)

                """
                默认url
                """
                if tables[k] == 'myurl':
                    url = "http://www.hngp.gov.cn/henan/ggcx?appCode=H60&channelCode=0102&bz=0&pageSize=30&pageNo=1"
                if tables[k] == 'myurl1':
                    url = "http://www.hngp.gov.cn/henan/ggcx?appCode=H60&channelCode=0102&bz=1&pageSize=30&pageNo=1"
                if tables[k] == 'myurl2':
                    url = "http://www.hngp.gov.cn/henan/ggcx?appCode=H60&channelCode=0102&bz=2&pageSize=30&pageNo=1"

                """
                默认值插入数据库
                """
                try:
                    self.sqlmanager.db_insert_myurl(url, tables[k])
                except Exception, e:
                    print "ClawlerPage insertmyurl:", e
                    judges = False
                    break
            else:
                url = result[0][0]
            judge = True
            while judge:
                """
                调用Urlrequest函数
                """
                print 'CrawlerPage Current crawl link:', url
                try:
                    string = UrlRequest(url)
                except Exception, e:
                    print "ClawlerPage urlrequest:", e
                    judges = False
                    break

                """
                信息的提取
                """
                item['url'] = re.compile('<li><span class="Right Gray">.*?</span><a.*?href="(/henan/content\?infoId=.*?'
                                         'channelCode=.*?&bz=\d)">.*?</a>', re.S).findall(string)
                item["title"] = re.compile('<li><span class="Right Gray">.*?</span><a.*?href="/henan/content\?infoId='
                                           '.*?channelCode=.*?&bz=\d">(.*?)</a>', re.S).findall(string)
                """
                如果为空抛出异常退出
                """
                if len(item['url']) == 0:
                    raise Exception("Message request is none")

                """
                下一页主信息url提取
                """
                urlnext = re.compile('<li class="nextPage">.*?<a href="(ggcx\?appCode=H60&channelCode=0102&bz=\d'
                                     '&pageSize=.*?&pageNo=.*?)">.*?</a>', re.S).findall(string)
                urlnextnum = re.compile('<li class="nextPage">.*?<a href="ggcx\?appCode=H60&channelCode=0102&bz=\d'
                                        '&pageSize=.*?&pageNo=(.*?)">.*?</a>', re.S).findall(string)
                """
                如果下一页主信息url为空以递增方式再次运行
                """
                if len(urlnext) == 0:
                    if tables[k] == "myurl":
                        urlnext[0] = "ggcx\?appCode=H60&channelCode=0102&bz=0&pageSize=30&pageNo=str(urlnextnum[0])"
                    if tables[k] == "myurl1":
                        urlnext[0] = "ggcx\?appCode=H60&channelCode=0102&bz=1&pageSize=30&pageNo=str(urlnextnum[0])"
                    if tables[k] == "myurl2":
                        urlnext[0] = "ggcx\?appCode=H60&channelCode=0102&bz=2&pageSize=30&pageNo=str(urlnextnum[0])"
                next = head+urlnext[0]
                """
                如果下一页主url与前一页主url相同网站爬取完毕退出
                """
                if next == url:
                    print "Crawlers.Crawler is over"
                    judge = False
                    continue
                # 信息url入库
                for i in range(0, len(item['url'])):
                    urls = heads+item['url'][i]
                    titles = item['title'][i]
                    try:
                        strings = UrlRequest(urls)
                    except Exception, e:
                        print "Crawlerpage urlquest:", e
                        judge = False
                        judges = False
                        break

                    list1 = re.compile('发布机构.*?<span class="Blue">(.*?)</span>.*?<span.*?class="Blue">.*?</span>'
                                       '.*?<span class="Blue">.*?</span>',re.S).findall(strings)
                    list2 = re.compile('发布机构.*?<span class="Blue">.*?</span>.*?<span.*?class="Blue">(.*?)</span>'
                                       '.*?<span class="Blue">.*?</span>',re.S).findall(strings)
                    list3 = re.compile('发布机构.*?<span class="Blue">.*?</span>.*?<span.*?class="Blue">.*?</span>'
                                       '.*?<span class="Blue">(.*?)</span>',re.S).findall(strings)
                    try:
                        self.sqlmanager.db_insert(urls, titles, list1[0], list2[0], list3[0])
                    except:
                        continue
                time.sleep(1)

                """
                改变已爬取信息url的状态
                """
                try:
                    self.sqlmanager.db_update_myurl(url, tables[k])
                except Exception, e:
                    print "CrawlerPage dbupdatemyurl", e
                    judges = False
                    break

                url = next

                """
                入库下一页信息url
                """
                try:
                    self.sqlmanager.db_insert_myurl(url, tables[k])
                except Exception, e:
                    print "CrawlerPage insertmyurl:", e
                    judges = False
                    break
            k += 1
def TimedTask():
    SECONDS_PER_DAY = 24 * 60 * 60
    curTime = datetime.now()
    print curTime
    desTime = curTime.replace(hour=8, minute=0, second=0, microsecond=0)
    print desTime
    delta = curTime - desTime
    skipSeconds = SECONDS_PER_DAY - delta.total_seconds()
    times = skipSeconds/3600
    print "Next day must sleep %d hours" % times
    time.sleep(skipSeconds)

def Mymain():
    """
    注释：主函数
    """
    a = Crawlers()
    """
    MySQL数据库提取链接，爬取存入Hbase
    """

    t1 = threading.Thread(target=a.CrawlerQueue)
    t1.start()
    t = threading.Thread(target=a.CrawlerPageHbase)
    t.start()

    """
    爬取链接存入MySQL，抓取新链接
    """
    a.CrawlerPage()
    a.CrawlerPageRank()
#Mymain()

if __name__ == "__main__":
    judge = True
    while judge:
        j = 0
        #TimedTask()
        while True:
            time.sleep(30)
            Mymain()
            j += 1
            if j == 5:
                judge = False
                break
