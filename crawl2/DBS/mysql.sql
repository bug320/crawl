create TABLE subpage2(
  id int(20) not null AUTO_INCREMENT,
  url CHAR(225),
  title CHAR(225),
  gongsi CHAR(225),
  person CHAR (225),
  time CHAR(40),
  user CHAR(225),
  flag char(4),
  hbase char(4),
  PRIMARY KEY (id,url)
)ENGINE= MYISAM CHARACTER SET utf8 ;