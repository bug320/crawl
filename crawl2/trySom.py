#-*-coding:utf-8-*-

from bs4 import BeautifulSoup
import MySQLdb
from DBS import MySQL
from DBS import hbase_mar
import hbase_cache
import downloader
import dbs
#####
htmlparser= "html.parser"
MySQL_TABLE = "subpage2"
HBASE_TABLE = "subpage"
#####

def getPageUrls(self):
    result = []
    limtfirst, limtnext, limtlast = self.getLimitURLs()
    # 如果 下一页 等于 limtnext 或者 等于 limtlast 则退出循环
    download = downloader.Downloader()
    try:
        soup = BeautifulSoup(download(limtfirst), htmlparser)
        pass
    except Exception as e:
        print "c:", e
        self.logset.add(limtfirst)
        soup = None
        pass
    if soup == None:
        print "fisrt soup error"
        return
    curentPageUrl = limtfirst
    nextPageUrl = self.nextURL(soup)
    while True:
        result = self.getEachPageUrls(curentPageUrl, soup)
        try:
            soup = BeautifulSoup(download(nextPageUrl), htmlparser)
            pass
        except Exception as e:
            print "getPageUrls", e
            pass
        if soup == None:
            print "soup is None"
            return
        curentPageUrl, nextPageUrl = nextPageUrl, self.nextURL(soup)
        if nextPageUrl == limtnext or nextPageUrl == limtlast:
            return
        yield result
        pass
    pass


pass

if __name__=="__main__":
    #db = dbs.DBS()
    #db.saveIntoMySQL("http://www.hngp.gov.cn/henan/content?infoId=1497520712855533&channelCode=H730202&bz=0",None)
    # D=downloader.Downloader()
    # url = "http://www.hngp.gov.cn/henan/content?infoId=1497520712855533&channelCode=H730202&bz=0"
    # html=D(url)
    # soup=BeautifulSoup(None,htmlparser)
    # print soup.prettify()
    cache=hbase_cache.Cache()
    cache["0"]="bug320"
    print cache["0"]
    pass

