# crawl重写方案

---

## 需求目标:

　1. 从采购网　seeｄ_url　中　爬下全部中标结果。
 
  2. 以字段 “序号”，“url”，“标题”，“时间” 的形式 存入 mysql 数据库
  
  3. 以字段 "序号","url","结果正文" 的形式存入 hbase 数据库
  
  4. 每天固定时间开始爬网站，并查中标结果是否更新，把更新内容保存到 数据库中
  
  5. 如果在爬取过程中出现中断，显示中断原因，并且设置断点，并且再次启动爬虫时候从断点出开始爬取
  
## 基本思路：

 a. 大框架:
   
  1. 设置四个爬虫：管理员爬虫 -- RootCrawl，网页下载爬虫 -- DownloadCrawl，mysql数据库爬虫 -- MySQLCrawl ，hbase数据库爬虫 -- HbaseCrawl。 
  
  2. 管理员爬虫读取配置文件 `conf.db` 和 记录文件 `log.db` ,查看上次爬去信息，根据启动 下载子程序:
  
      2.1. 网页下载爬虫启动，根据断点下载网页，并且，把一个页面的所有结果抓取完毕后，发送消息 “成功下载当前页，请求保存到数据库” 到 管理员爬虫。
      
      2.2. 管理员爬虫根据消息 “成功下载当前页，请求保存到数据库” 启动 mysql 数据库爬虫。
      
      2.3. mysql 数据库爬虫从结果中获取 “序号”，“url”，“标题”，“时间”，并在数据库中检索是否存在重复字段，保存新纪录。并把更新结果发送到管理员爬虫。
      
      2.4 如果管理爬虫收到 "mysql已经更新" 消息，启动 hbase 数据库爬虫 并存入更新数据，hbase 数据库爬虫把更新结果发送到 管理员爬虫。
      
      2.5 管理员爬虫检测本次保存记录，如果正常则把断点信息发送给网页下载爬虫，网页下载根据断点信息下载网页，重复 2.1 ~ 2.5 直到管理员收到 下载爬虫接收到 "网站已经爬去完毕"。如果本次保存记录出错，启动错误处理子程序。
  
  3. 管理爬虫启动处理子程序:
    
     3.1. "断网错误":
     
     3.2. "保存到 MySQL 失败":
     
     3.3. "保存到 HBase 失败"
     
     3.4. "下载错误"
     
        3.4.1 “3XX”错误
        
        3.4.2 "4XX"错误
        
        3.4.3 “5XX”错误
  
  4. 中断重爬子程序
	暂时没有开始想好。。。这个主要时打算以后让爬虫自己运行时候自启动时候用，中断处理一些系统错误，或者是分布式爬虫的扩展。

  注：
	以上时第二版改写时的参考思路（但是实现上会有些差异），因为二版还没有改成功，所以先发了一般，二版设计图见 design.txt ，一般就不多说，都是零散的方法，看见函数名就能知道函数时干啥的。