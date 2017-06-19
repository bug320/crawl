#-*- coding:utf-8 -*-
import hbase_mar
import hbase_cache
DEFAULT_ROW = "html"
DEFAULT_TABLE = "cache"
DEFAULT_KEY_DATA = "html:data"
DEFAULT_KEY_URL = "html:url"

if __name__ == "__main__":
    #client =hbase_mar.HbaseClient()
    #tables =client.get_tables()
    #if DEFAULT_TABLE not in tables:
    #    client.create_table("cache","html")
    #else:
    #    print "the cache is alreay existed"
    #    pass
    #print dir(client.scan(DEFAULT_ROW,DEFAULT_ROW))
    #for i in xrange(5):
    #    client.put(DEFAULT_TABLE,"%d"%i,{DEFAULT_KEY_URL:"www.%d.com"%i,DEFAULT_KEY_DATA:"None"})
    #i = client.scan((DEFAULT_TABLE,"0"))
    #print i[DEFAULT_KEY_URL].next()
    #for i in client.scan(DEFAULT_TABLE,"-1"):
    #    print i[DEFAULT_KEY_URL]
    #    break
    # client = hbase_mar.HbaseClient()
    # client.create_table("cache","html")
    #print client.get_tables()
    #print client.get(DEFAULT_TABLE,"baidu","html:url")

    #print [{k: v.value} for (k,v) in client.getrow(DEFAULT_TABLE,"baidu")]
    #for i in [{k: v.value} for (k,v) in client.getrow(DEFAULT_TABLE,"baidu")]:
    #    print i
    #    print i['html:url']
    #print client.getrow(DEFAULT_TABLE,"baidu")
    cache = hbase_cache.Cache()
    cache["0"]="0"
    print cache["0"]
    #cache.gget()
    #cache.__setitem__("0","12")
    #cache["0"]="12"
    #cache["baidu"]="你好2"
    #print cache["baidu"]
    print "OK"
    pass
