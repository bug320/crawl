#-*- coding:utf-8 -*-
from DBS import MySQL

# 使用　mysql　中　数据库名为　DEFAULT_DBASE , 表名为 DEFAULT_TABLE
# 关键字段为 DEFAULT_KEY　,该字段存储断点的　url 的网页信息
# 默认字段为 DEFAULT_CODE ,该字段存储断点的　服务器返回的错误码
DEFAULT_DBASE = "bug320"            # 就是一个数据库名,要自己建,不想建可以直接改成 mysql
DEFAULT_TABLE = "download_log"      # 存储log 信息的 表名,这个真的需要自己建
DEFAULT_CODE = "code"               # 下载出错的 code
DEFAULT_KEY = "url"                 #  下载出错的 url ,也是　mysql 的主键
"""
create TABLE download_log(
  id int(20) AUTO_INCREMENT not null,
  url CHAR(225) not null,
  code CHAR(225),
  PRIMARY KEY (id,url)
)ENGINE= MYISAM CHARACTER SET utf8 ;
"""
class LogQueue(object):
    def __init__(self,host="localhost", usr="root", passwd="root",):
        try:
            self.db=MySQL.MySQL(host=host,usr=usr,passwd=passwd,dbase=DEFAULT_DBASE)
            result = self.db.select("select * from %s" % DEFAULT_TABLE)
            print "log info ::", " enter the table %s of the dbase %s the mysql is sueccess" % (DEFAULT_TABLE, DEFAULT_DBASE)
            pass
        except Exception as e:
            print "log error::","can not open the table %s of the dbase %s" %(DEFAULT_TABLE,DEFAULT_DBASE)
            pass
        pass
    def pop(self):
        if self.exists:
            try:
                Result = self.db.selectSQL('DEFAULT_TABLE', 'select * from %s order by id' % DEFAULT_TABLE)
                reslut = Result[0] if Result != () else None
                # print reslut
                if reslut != None:
                    self.db.deleteSQL(DEFAULT_TABLE,"DELETE FROM %s WHERE %s = '%s'" % (DEFAULT_TABLE,DEFAULT_KEY,reslut.get(DEFAULT_KEY)))
                    return reslut
            except Exception as e:
                raise Exception("log error::"+'when get the log of '+ url + ' have something error : '+str(e))
                return Nones
                pass
        else:
            raise KeyError("log error::",'when get the log of '+ url + ' does not exist')


        pass
    def push(self,url,code):
        if not self.exists(url):
            try:
                self.db.insert(DEFAULT_TABLE,url=url,code=code)
            except Exception as e:
                raise Exception("c"+"when insert the log of "+url+" to MySQl,find shting error"+str(e))
            pass
        else:
            # 这个不添加就好,不让它提示错误
            #raise Exception("Log　error::"+"when push the log of "+url+' find it is already exitst')
            pass
        pass

    def exists(self,url):
        Result = self.db.selectSQL('DEFAULT_TABLE','select * from %s' % DEFAULT_TABLE)
        result = Result if Result != () else {}
        for i in xrange(len(Result)):
            result = Result[i] if Result != () else {}
            if url in result.values():
                return True
        return False
        pass
    def show(self):
        Result = self.db.selectSQL('DEFAULT_TABLE', 'select * from %s order by id'  % DEFAULT_TABLE)
        print len(Result)
        if len(Result) == 0:
            print " |  |   |"
        for i in xrange(len(Result)):
            result = Result[i] if Result != () else {}
            for k,v in result.items():
                #print " | url == %s | code == %s |" % (result.get(k),code)
                print k,v
                pass
            pass
        pass
    def __len__(self):
        try:
            Result = self.db.selectSQL('DEFAULT_TABLE', 'select * from %s order by id' % DEFAULT_TABLE)
            return len(Result)
        except Exception as e:
            print "Log error::",e
            return 0
    pass


if __name__ == "__main__":
    log = LogQueue()
    isRun =True
    while isRun:
        choose = raw_input("please input your what you want to do:\na.push\nb.pop\nc.exit")
        if choose == 'a':
            url = raw_input("Please input the url")
            code = raw_input("Please inpput the code")
            try:
                log.push(url,code) # 插入 log 信息
            except Exception as e:
                print e
            pass
        elif choose == 'b':
            print log.pop()              # 删除 log 信息并返回字典　
            pass
        elif choose == 'c':
            isRun =False
            pass
        else:
            pass
        print "==================s"
        print "len =",len(log)
        log.show()
        pass
