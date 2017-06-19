#-*- coding:utf-8 -*-
from DBS import hbase_mar

# 创建一个表 表名为 cache ,列族为 html,用 url 当row_key ,每行有一个 "html:data" 字段,该字段存储完整的网页信息
DEFAULT_ROW = "html"
DEFAULT_TABLE = "cache"
DEFAULT_KEY_DATA = "html:data"
DEFAULT_KEY_URL = "html:url"
class Cache(object):
    def __init__(self,host="localhost",port="9090"):
        self.client=hbase_mar.HbaseClient(host,port)
        tables = self.client.get_tables()
        if DEFAULT_TABLE not in tables:
            self.client.create_table(DEFAULT_TABLE, DEFAULT_ROW)
        else:
            print "cache info :: the %s is alreay existed" % DEFAULT_TABLE
            pass
        pass
    def __getitem__(self, url):
        """Load data from hbase for this URL
            """
        path = url
        if self.exists(path):
            try :
                #data = self.client.scan(DEFAULT_TABLE,url)
                for  i in self.client.scan(DEFAULT_TABLE,url):
                    data = i[DEFAULT_KEY_DATA]
                    return data if isinstance(data,str) else data.encode("utf-8")
                pass
            except Exception as e:
                raise Exception('cache error :: get the cache of '+url+'have somthoing wrong:'+e)
                pass
            finally:
                pass
        else:
            # URL has not yet been cached
            raise KeyError('cache error :: when get the cache of '+ url + ' does not exist')
        pass
    def __setitem__(self, url, data):
        """Save data to hbase for this url
        """
        path =url
        try:
            if isinstance(data,unicode):
                data=data.encode("utf-8")
            self.client.put(DEFAULT_TABLE,path,{DEFAULT_KEY_URL:path,DEFAULT_KEY_DATA:data})
            pass
        except Exception as e:
            raise Exception("cache error ::"+"when save the "+url+"have the error:"+e)
            pass
        pass

    # def __delitem__(self, url):
    #     """Remove the value at this key and any empty cache
    #     """ I have not to write theh fuction  ## todo
    #     path = url
    #     try:
    #         pass
    #         #os.remove(path)
    #         #os.removedirs(os.path.dirname(path))
    #     except Exception:
    #         pass
    def exists(self,url):
        for i in self.client.scan(DEFAULT_TABLE):
            ii=i[DEFAULT_KEY_URL] if isinstance(i[DEFAULT_KEY_URL],unicode) else i[DEFAULT_KEY_URL].encode("utf-8")
            if url == ii:
                return True
        return False
        pass
    pass

if __name__ == "__main__":
    cache = Cache()
    cache["url"]="html"  # 存进 html 到　hbase 中，行健时　url
    print cache["url"]  # 打印　html
    pass

