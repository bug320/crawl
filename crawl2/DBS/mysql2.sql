create TABLE org1(
  id int(20) not null AUTO_INCREMENT,
  url CHAR(225),
  title VARCHAR(225),
  org VARCHAR(225),
  person VARCHAR (225),
  post_time VARCHAR(40),
  check_post VARCHAR(40),
  inner_html_url VARCHAR(225),
  mysql int(1),
  hbase int(1),
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
