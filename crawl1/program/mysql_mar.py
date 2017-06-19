#!/usr/bin/python
# _*_ coding: UTF-8 _*_
import MySQLdb

class DBmanager:
    def __init__(self):
        self.conn = MySQLdb.connect(host='127.0.0.1', user='root', passwd='root', db='caigou', charset='utf8')

    def db_insert(self, urls, titles, orgs, names, dates):
        """
        注释：链接，主要信息插入MySQl
        """
        try:
            url = urls
            title = titles
            org = orgs
            name = names
            date = dates
            status = "new"
            cur = self.conn.cursor()
            sql = 'INSERT INTO mycaigou (id, url, title, org, name, date, status) VALUES ' \
                  '(NULL ,"%s", "%s", "%s", "%s", "%s", "%s")' % (url, title, org, name, date, status)
            cur.execute(sql)
            self.conn.commit()
        except Exception, e:
            pass


    def db_select(self):
        """
        注释：未爬取链接查询
        """
        cur = self.conn.cursor()
        sql = "SELECT url FROM mycaigou WHERE status='new'"
        cur.execute(sql)
        result = cur.fetchall()
        cur.close()
        return result

    def db_update(self,myurl):
        """
        注释：已爬取链接更新状态
        """
        cur = self.conn.cursor()
        sql = 'UPDATE mycaigou SET status="run" WHERE url="%s"' %myurl
        cur.execute(sql)
        self.conn.commit()

    def db_insert_myurl(self, url, tables):
        """
        注释：主链接插入MySQL
        """
        status = "new"
        cur = self.conn.cursor()
        sql = 'INSERT INTO %s VALUES ("%s", "%s")' % (tables, url, status)
        cur.execute(sql)
        self.conn.commit()


    def db_select_myurl(self, tables):
        """
        注释：主链接查询
        """
        cur = self.conn.cursor()
        sql = "SELECT url FROM %s WHERE status='new'" % tables
        cur.execute(sql)
        result = cur.fetchall()
        cur.close()
        return result

    def db_update_myurl(self, urls, tables):
        """
        注释：主链接更新状态
        """
        cur = self.conn.cursor()
        sql = 'UPDATE %s SET status="run" WHERE url="%s"' % (tables, urls)
        cur.execute(sql)
        self.conn.commit()


    def db_select_pagerank(self):
        """
        注释:查询所有链接，为重排提供对比
        """
        cur = self.conn.cursor()
        sql = "SELECT url FROM mycaigou WHERE 1"
        cur.execute(sql)
        result = cur.fetchall()
        cur.close()
        return result

    def select_title(self, urls):
        """
        注释：查询链接标题信息
        """
        cur = self.conn.cursor()
        sql = "SELECT title,org,name,date FROM mycaigou WHERE url='%s'" %urls
        cur.execute(sql)
        result = cur.fetchall()
        cur.close()
        return result



    def close(self):
        self.conn.close()
