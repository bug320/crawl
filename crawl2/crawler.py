# -*- coding:utf-8 -*-

import urllib2
import urlparse
from bs4 import BeautifulSoup
from bs4 import UnicodeDammit

import downloader
import hbase_cache
import dbs
import shelve
####
htmlparser= "html.parser"

CDB = dbs.DBS()
####

class Crawler(object):
    """ the crawler for crawl web site and save the info 
    """
    def __init__(self,seed_url):
        self.seed_url=seed_url
        pass
    def lastURL(self,soup):
        return urlparse.urljoin(self.seed_url,soup.find("li", attrs={"class": "lastPage"}).find("a")["href"].encode("utf-8")) if soup else None
        pass
    def nextURL(self,soup):
        return urlparse.urljoin(self.seed_url,soup.find("li", attrs={"class": "nextPage"}).find("a")["href"].encode("utf-8")) if soup else None
        pass
    def firstURL(self,soup):
        return urlparse.urljoin(self.seed_url,soup.find("li", attrs={"class": "firstPage"}).find("a")["href"].encode("utf-8")) if soup else None
        pass
    def getLimitURLs(self):
        """
        获取为尾页状态下的 首页连接,下一页链接,和尾页链接的 tuple
        :return: (firstUrls,nextUrl,lastUrls)
        """
        try:
            # result =[]
            seed_url = self.seed_url
            download = downloader.Downloader()
            # 下载初始网页找到尾页状态下的：首页,下一页,和尾页的 URL
            soup = BeautifulSoup(download(seed_url), htmlparser)
            # 获取尾页临时尾页
            tmpURL = self.lastURL(soup)
            # 下载临时尾页
            soup = BeautifulSoup(download(tmpURL),htmlparser)
            # 获取尾页状态下的下一页的 url 和 页数
            nextPageUrl = self.nextURL(soup)
            # 获取尾页状态下的首页的 url 和 页数
            firstPageUrl = self.firstURL(soup)
            # 获取尾页状态下的尾页的 url 和 页数
            lastPageURL = soup.find("li", attrs={"class": "lastPage"}).find("a")["href"].encode('utf-8')
            lastPageURL = urlparse.urljoin(seed_url, lastPageURL)
            return (firstPageUrl, nextPageUrl, lastPageURL)
            pass
        except Exception as e:
            print "getPageURLs : %s ",e
            return  (None,None,None)
            pass
        pass

    def getEachPageUrls(self,slink,soup=None):
        """
        获取link的所有目的链接,并返该租连接的列表
        :param link: 
        :return: 
        """
        try:
            result = []
            soup =soup if soup else BeautifulSoup(downloader.Downloader()(slink),htmlparser)
            # class ="BorderBlue NoBorderTop Padding5"
            # list2 = soup.find("div", attrs={"class": "BorderBlue NoBorderTop Padding5"}).find("div",attrs={"class": "List2"})
            list2 = soup.find("div",attrs={"class": "List2"})
            for a in list2.find_all('a'):
                suburl = urlparse.urljoin(slink, a['href'].encode('utf-8'))
                result.append(suburl)
                # print suburl
            return result
            pass
        except Exception as e:
            print "getEachPageUrls():",e
            return None
            pass


    def wlog(self,url):
        fb = open("log.txt",'w')
        fb.writelines("%s" % url)
        fb.close()
        pass
    def rlog(self):
        url = ""
        try:
            fb = open("log.txt", 'r')
            url = fb.readline()
            fb.close()
            return url
            pass
        except Exception as e:
            print "Can't open the loog:",e
            return
            pass
        pass


    def getPageUrls(self,log=None,root=0):
        """
        get each page url from fisrtPage to lastPage
        :param root: set zero for no limit to loop 
        :return: 
        """
        result = []
        limtfirst,limtlast,limtnext = self.getLimitURLs()  # 获取限制信息
        download = downloader.Downloader()                 # 生产下载器
        # 从首页开始下载，返回每页的子链接，知道下一页等于limtlast或者等于limtnext
        isRun, runTime = True, 0
        #currenturl = limtfirst
        #nexturl = None  # 这里设为 limtfirst 所以,首页会多下载一次
        currenturl=limtfirst if not log else log
        while isRun:
            # 限制运行次数,仅作测试用,设为0 则没有强行退出
            runTime += 1
            isRun = False if runTime == root else isRun
            isRun = False if currenturl == limtnext or currenturl == limtlast else isRun
            print "Get the PageURl ",runTime,currenturl
            # loop body
            try: # 首页连续爬了两次,不过基本不影响,所以上面 runTime == root 改为了 runTime == root+1
                print
                #
                pshe = shelve.open("page.she","c")
                pshe[str(runTime)] = currenturl
                pshe.sync()
                pshe.close()

                soup = BeautifulSoup(download(currenturl),htmlparser)  # 获取当前页的 soup
                # #yield self.getEachPageUrls(currenturl,soup)            # 返回本页所有的 目的子链接
                suburls = self.getEachPageUrls(slink=currenturl,soup=None)
                if suburls != [] or suburls != None:
                    for ii,link in enumerate(suburls):
                        spshe = shelve.open("subpage.she", "c")
                        id = runTime * 100 + ii
                        spshe[str(id)] = link
                        spshe.sync()
                        spshe.close()

                        # CDB.saveIntoMySQL(link=link,soup=None)
                        # CDB.saveIntoMyHbase(link=link,soup=None)
                        pass
                    pass
                else:
                    self.wlog(currenturl)
                    return
                #     pass
                currenturl  = self.nextURL(soup)                       # 设置新的当前页
                pass
            except Exception as e:
                print "In function getPageUrls(self,root):",e
                self.wlog(currenturl)
                return
                pass
            if not isRun:
                self.wlog(currenturl)
                pass
            pass
        return
        pass


if __name__ == "__main__":
    #  todo: 实现了保存断点的队列　Log类, 保存能在　MySQL中, 并且整合到了, downloader类里面
    #  todo 但是由于 cache 机制的存在,只会记录请求服务器下载网页的时候的断点,对于从数据库或者本地返回的数据无法保存断点,因为不存在下载这个过程
    #  todo 所以还需要在程序中引入一个机制让在程序运行中可以　让程序真的实现断点重爬：
    # todo
    #  1. 让　downloader 类只负责下载过程中的下载错误的记录,并保存请求服务器段的发送错误的断点
    #  2. 在程序发送错误的错误重启或这自启动时候,先检出　downloader 类引发的错,这类错误称为　`下载错误`
    #  3. 程序运行过程中发生的错误成为　`系统错误`　,发生系统错误时候要首先记录下当前爬虫进行到那一大页,那一个小页,不对小页重爬,而是重爬发生错误所在的大页
    #  4. 在数据库添加一个字段,来记录每一条链接所在的大页.
    #  5. 在写一个　Logqueue　专门处理发生系统错误时候的记录 ,或者对 Logqueue　进行扩展,让其拥有一个新的机制来区分　系统错误　和　下载错误,对外黑盒,保持接口一致
    #
    # todo
    try:
        seed_url = 'http://www.hngp.gov.cn/henan/ggcx?appCode=H60&channelCode=0102'
        # seed_url = 'http://www.hngp.gov.cn/henan/ggcx?appCode=H60&channelCode=0102&bz=0&pageSize=20&pageNo=31'
        crawl =Crawler(seed_url)
        crawl.getPageUrls()
    except Exception as e:
        print "Have the error ,",e,e.__doc__
        ck = raw_input("Do you want to reload from the log(y/n)")
        if ck == "y" or ck == "Y":
            seed_url = crawl.rlog()
            crawl = Crawler(seed_url)
            crawl.getPageUrls()
            pass
        elif ck  == "n" or ck == "N":
            print  "the cralwer is exit"
            pass
    print "the log end with",crawl.rlog()



    # seed_url = 'http://www.hngp.gov.cn/henan/ggcx?appCode=H60&channelCode=0102'
    # seed_url = 'http://www.hngp.gov.cn/henan/ggcx?appCode=H60&channelCode=0102'
    # crawler=Crawler(seed_url)
    # #nextPageURls test
    # # pageInfo = crawler.getLimitURLs()
    # # for i in pageInfo:
    # #    print i
    #
    # #get all sublinks of the page
    # #crawler.getEachPageUrls(seed_url)
    #
    # #get sublinks in yield
    # #i=0
    # for sublink in crawler.getPageUrls(root=10):
    #     # if sublink ==None:
    #     #     break
    #     #i+=1
    #     for url in sublink:
    #         print url
    #     pass
    #     #print i
    # pass
    # crawler.getPageUrls(10)

    pass




#######
# 2017- yeild -beifen-cuowu:会自己断开 hbase thrift
#     def getPageUrls(self):
#         result = []
#         limtfirst, limtnext, limtlast = self.getLimitURLs()
#         # 如果 下一页 等于 limtnext 或者 等于 limtlast 则退出循环
#         download = downloader.Downloader()
#         try:
#             soup = BeautifulSoup(download(limtfirst), htmlparser)
#             pass
#         except Exception as e:
#             print "c:", e
#             self.logset.add(limtfirst)
#             soup = None
#             pass
#         if soup == None:
#             print "fisrt soup error"
#             return
#         curentPageUrl = limtfirst
#         nextPageUrl = self.nextURL(soup)
#         while True:
#             result = self.getEachPageUrls(curentPageUrl, soup)
#             try:
#                 soup = BeautifulSoup(download(nextPageUrl), htmlparser)
#                 pass
#             except Exception as e:
#                 print "getPageUrls", e
#                 pass
#             if soup == None:
#                 print "soup is None"
#                 return
#             curentPageUrl, nextPageUrl = nextPageUrl, self.nextURL(soup)
#             if nextPageUrl == limtnext or nextPageUrl == limtlast:
#                 return
#             yield result
#             pass
#         pass


#####