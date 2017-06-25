#-*- coding:utf-8 -*-
import re
import MySQLdb
from bs4 import UnicodeDammit
from bs4 import BeautifulSoup
from DBS import MySQL
from DBS import hbase_mar
import hbase_cache
import downloader

#####
htmlparser= "html.parser"
MySQL_TABLE = "subpage21"
HBASE_TABLE = "subpage21"
# 提取脚本中要 innerHTML 部分的 url
endUrlCmp=re.compile(r'\$\.get\(\"(.*\.htm)')
# 把 innerHTML 的 url 转换成绝对 url
mainHTTP = "http://www.hngp.gov.cn"
DEFUALT_FALSE = "0"
DEFUALT_TRUE = '1'
#####

def getInnerPageURLs(soup):
	links= []
	for string in soup.stripped_strings:
		dammit = UnicodeDammit(string)
		tmpurl=endUrlCmp.findall(dammit.unicode_markup)
		if tmpurl != [] and tmpurl not in links:
			links.append(tmpurl)
	return links

class DBS(object):
    def __init__(self,mdb=None,hdb=None):
        self.mdb =mdb if mdb else  MySQL.MySQL(dbase="bug320")
        self.hdb =hdb if hdb else hbase_mar.HbaseClient()
        if HBASE_TABLE not in self.hdb.get_tables():
            self.hdb.create_table(HBASE_TABLE, "page")
        pass

    def saveIntoMySQL(self, link, soup=None):
        """
        save the info of the link to the mysql
        :param self: 
        :param link: 
        :param soup: 
        :return: 
        """
        save = DEFUALT_TRUE
        if soup == None:
            download = downloader.Downloader()
            try:
                soup = soup if soup else BeautifulSoup(download(link), htmlparser)
            except Exception as e:
                save = DEFUALT_FALSE
                self.mdb.insert(MySQL_TABLE, title="", url=link, flag=str(save), hbase=DEFUALT_FALSE)
                return
                pass
        if save != DEFUALT_FALSE:
            attr = []  # title=0,gongsi=1,person=2,time=3
            # print soup.prettify()
            attr.append(soup.find("h1", attrs={"class": "TxtCenter Padding10 BorderEEEBottom Top5"}).string.encode(
                "utf-8"))  # class="TxtCenters
            # attr.append(string.string for string in soup.find_all("span",attrs={"class":"Blue"}))
            for string in soup.find_all("span", attrs={"class": "Blue"}):
                attr.append(string.string.encode("utf-8"))
                pass
            # for at in attr:
            #    print at
            sql = "url = '%s'" % (link)
            sql = "select title from %s where %s" % (MySQL_TABLE, sql)
            if not self.mdb.selectSQL(MySQL_TABLE, sql):
                # print bool(self.mdb.selectSQL(MySQL_TABLE,sql))
                # self.mdb.execSQL(MySQL_TABLE,"set names utf8")
                self.mdb.insert(MySQL_TABLE, url=link, title=attr[0], gongsi=attr[1], person=attr[2], time=attr[3],
                                user=attr[4], flag=str(save), hbase=DEFUALT_FALSE)
                pass
            else:
                ##print  "OK"
                sql = "url = '%s'" % (link)
                sql = "url = '%s'" % (link)
                sql = "select flag from %s where %s" % (MySQL_TABLE, sql)
                # sql = "url = '%s'" % (link[1])
                if bool(self.mdb.selectSQL(MySQL_TABLE, sql)[0]['flag']) == DEFUALT_FALSE:
                    sql = "url = '%s'" % (link)
                    # title = attr[0], gongsi = attr[1], person = attr[2], time = attr[3], user = attr[4], flag = str(save)
                    self.mdb.update(title=attr[0], gongsi=attr[1], person=attr[2], time=attr[3], user=attr[4],
                                    flag=str(save),
                                    hbase=DEFUALT_FALSE
                                    , where=sql)
                    print "%s update into mysql" % link
                else:
                    print "%s have save into mysql" % link
                    # tmp = self.mdb.selectSQL(MySQL_TABLE,"select * from %s" % MySQL_TABLE)[0]
                    # for i in tmp.keys():
                    #     print i,tmp[i]
        pass
    def saveIntoMyHbase(self,link, soup=None):
        """
        save the info form the MySQL into  the Hbase,it's hbave the same 'id'
        :param link: the link are use to save 
        :param soup: the soup of the link if not will download the link first
        :return: 
        """

        sql = "url = '%s'" % (link)
        sql = "select *from %s where %s" % (MySQL_TABLE, sql)
        sql_data = self.mdb.selectSQL(MySQL_TABLE, sql)[0]
        # hbase_save = sql_data["hbase"]
        # hbase_save = hbase_save if isinstance(hbase_save,str) else hbase_save.encode('utf-8')
        hbase_save = DEFUALT_FALSE
        if hbase_save == DEFUALT_TRUE:
            # if  save into hbase,return
            return
            pass
        elif hbase_save == DEFUALT_FALSE:
            # if not save into hbase,then save
            download = downloader.Downloader()
            soup = soup if soup else BeautifulSoup(download(link), htmlparser)
            urls = getInnerPageURLs(soup)[0][0]
            url = "URL:%s%s" % (mainHTTP, urls.encode('utf-8'))
            dhtml = download(url)
            hbase_save = DEFUALT_TRUE if dhtml else DEFUALT_FALSE
            dsoup = BeautifulSoup(dhtml, htmlparser)
            saveTxt = ""
            for string in dsoup.stripped_strings:
                dammit = UnicodeDammit(string)
                saveTxt = saveTxt + "%s\n" % dammit.unicode_markup.encode('utf-8')
            # print saveTxt
            #self.hdb = hbase_mar.HbaseClient()
            #if HBASE_TABLE not in self.hdb.get_tables():
            #    self.hdb.create_table(HBASE_TABLE, "page")

            for key in sql_data.keys():
                if key == 'id':
                    continue
                v = sql_data[key] if isinstance(sql_data[key], str) else sql_data[key].encode('utf-8')
                id = "%d" % sql_data['id']
                self.hdb.put(HBASE_TABLE, "%s" % id, {"page:%s" % (key): "%s" % v})
            else:
                self.hdb.put(HBASE_TABLE, "%s" % id, {"page:data": "%s" % (saveTxt)})
                # show the info about the put
                #self.hdb.ggetrow(HBASE_TABLE, "%d" % sql_data['id'])
            print "%s save into hbase" % url

            pass
        else:
            pass
        pass

