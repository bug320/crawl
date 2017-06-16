# -*- coding:utf-8 -*-


import MySQLdb as DB


class MySQL(object):
    """
        对 MySQLdb 的封装，select 输出为 key = valuse 的字典，而非 tuple
        __Init__
        close()
        insert()
        select()
        update()
        delete()
    """

    def __init__(self, host="localhost", usr="root", passwd="root", dbase=""):
        # type: (object, object, object, object) -> object
        # type: (object, object, object, object) -> object
        # type: (object, object, object, object) -> object
        """

        :rtype: object
        :param host: 主机地址
        :param usr: 用户名
        :param passwd: 密码`
        :param dbase: 数据库名
        设置查寻输出为字典模式
        """
        self.conn = DB.connect(host, usr, passwd, dbase, charset='utf8')
        # 这里让 查询结果不再是 tuple 类型 而是 字典的形式返回
        self.cursor = self.conn.cursor(cursorclass=DB.cursors.DictCursor)
        pass

    def close(self):
        """
        关闭数据库
        :return:
        """
        self.conn.close()
        pass

    def connect(self):
        """

        :return: 返回 connect 对象
        """
        return self.conn
        pass

    def cursor(self):
        """

        :return: 返回 cursor 对象
        """
        return self.cursor
        pass

    def version(self, format="TUPLE"):

        """

        :return: 返回当前数据库版本
        """
        self.cursor = self.conn.cursor()
        self.cursor.execute("SELECT VERSION()")
        data = self.cursor.fetchone()
        self.cursor = self.conn.cursor(cursorclass=DB.cursors.DictCursor)
        return data

    def insert(self, table, **item):
        """

        :param table: 操作的表
        :param item: 插入对 key = value 形式 将插入
        :return:
        """
        keys = ""
        values = ""

        for key, value in item.items():
            # debug start
            # print "key=",key
            # print "value=",value,"type(value)=",type(value)
            # debug end
            keys = keys + "," + key
            values = values + ",\"" + value + "\""
            # debug start
            # print "OO"
            # print "keys=",keys
            # print "values=",values
            # debug end
            pass
        keys = keys[1:]
        values = values[1:]
        ## debug start
        print keys
        print values
        ## debug end
        try:
            sql = "INSERT INTO %s( %s ) VALUES( %s )" % (table, keys, values)
            # debug start
            ##INSERT INTO SYS(loopSize,lengthSize) VALUES ("7","4")
            #  print "sql = ",sql
            # debug end
            self.cursor.execute(sql)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print "insert error --- ", e
        pass

    def selectAll(self,table):
       #result = []
       try:
           self.cursor.execute("SELECT * FROM %s" % table )
           result = self.cursor.fetchone()
           return result
       except Exception as e :
           self.conn.rollback()
           print "select error ---  ",e
           return None
       pass

    def select(self, table, where=None, *slc):
        """

        :param table: 查询打表,如果只有这一个参数,将打印返回全部内容
        :param where: 查询条件，不设置则对全表操作
        :param slc: 要查询的字段，如果没有表示全部字段
        :return: 返回查询结果
        """
        sln = len(slc)
        slec = ""
        if sln:
            for it in slc:
                slec = slec + "," + it
            slec = slec[1:]
        else:
            slec = "*"
        try:
            if where:
                sql = "SELECT %s FROM %s WHERE %s" % (slec, table, where)
            else:
                sql = "SELECT %s FROM %s " % (slec, table)
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            return result[0]
            pass
        except Exception as e:
            self.conn.rollback()
            print "select error ----", e
            return None
            pass

        pass

    def selectSQL(self, table, sql):
        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            return result
        except Exception as e:
            self.conn.rollback()
            print "select error --- ", e
        pass
    def execSQL(self,table,sql):
        try:
            self.cursor.execute(sql)
        except Exception as e:
            self.conn.rollback()
            print "select error --- ", e
        pass
    def update(self, table, **args):
        """

        :param table: 操作的表
        :param args: 以字店形式表示 字段=新值 插入数据,有一个额外 的字段 where = “条件”,表示更新条件,不舍表示无条件更新
        :return:
        """
        where = args.get("where")
        if where:
            del args["where"]
        sets = ""
        for key, value in args.items():
            sets = sets + "," + key + "=\"" + value + "\""
        try:
            if where:
                sql = "UPDATE %s SET %s WHERE %s" % (table, sets, where)
            else:
                sql = "UPDATE %s SET %s " % (table, sets)
            self.conn.commit()
            pass
        except Exception as e:
            self.conn.rollback()
            print "Update Error ---", e
            pass
        pass

    def updateSQL(self, table, sql):
        try:
            self.cursor.execute(sql)
            self.conn.commit()
        except Exception as e:
            self.conn.commit()
            print "Update Error ---", e
            pass

    pass

    def delete(self, table, **args):
        """

        :param table: 操作的表
        :param args: 字典参数，,其实只有一个 where 字典有效,表示要删除的条件,不设置表示删除整个表
        :return:
        """
        where = args.get("where")
        try:
            if where:
                sql = "DELETE FROM %s WHERE %s" % (table, where)
            else:
                sql = "DELETE FROM %s" % (table)
            self.cursor.execute(sql)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print  "Delete ERROR ---", e
        pass

    def deleteSQL(self, table, sql):
        # sql = "DELETE FROM EMPLOYEE WHERE AGE > '%d'" % (20)
        try:
            self.cursor.execute(sql)
            self.conn.commit()
            pass
        except Exception as e:
            self.conn.rollback()
            print "Delete Error! --- ", e
            pass
        pass


class MySQLcache(object):
    def __init__(self,client=None):
        self.client = MySQL(dbase="bug320") if client ==None else client
        
    pass

if __name__ == "__main__":
    db = MySQL(dbase='mysql')
    # db.insert("SYS",loopSize="7",lengthSize="4")
    # db.delete("SYS",where=" loopSize='17' ")
    # db.delete("SYS","DELETE FROM SYS WHERE loopSize='17' ")
    # result = db.select("SYS")
    # print type(result)
    # print result
    import os

    while True:
        menu = ur"""
            请选择功能：
            1. 查看
            2. 修改
            3. 插入
            4. 删除
            5. 退出
        """
        choess = raw_input(menu)
        if choess == '1':  # 查看
            print u"当前数据库版本为：%s" % db.version()
            print db.select("SYS")
            print str(db.select("SYS"))
            pass
        elif choess == '2':  # 修改
            pass
        elif choess == '3':  # 插入
            pass
        elif choess == '4':  # 删除
            pass
        elif choess == '5':  # 退出
            pass
        else:  # 其他
            print u"请输入正确的选择"
            pass

        raw_input("输入任何字符继续....")
        os.system("clear")
        pass
