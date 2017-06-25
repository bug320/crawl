create TABLE subpage2(
  id int(20) not null AUTO_INCREMENT,
  url CHAR(225),
  title CHAR(225),
  org CHAR(225),
  person CHAR (225),
  post_time CHAR(40),
  check_post CHAR(40),
  inner_html_url CHAR(225),
  mysql char(5),
  hbase char(5),
  PRIMARY KEY (id)
)ENGINE= MYISAM CHARACTER SET utf8 ;

[
	"url",			"网页url",
	"title",		"文章标题",
	"org",			"发布机构",
	"person",		"发布人",
	"post_time",		"发布时间",
	"check_post",		"浏览次数",
	"inner_html_url,	"内嵌网页url",
	“mysql”，		“存入mysql标识符”，
	“hbase”，		“存入hbase标识符”
]
