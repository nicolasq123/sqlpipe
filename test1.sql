create database if not exists test1;
CREATE TABLE test1.`user` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `class_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT 'class id',
  `name` varchar(64) NOT NULL DEFAULT '' COMMENT 'name',
  `ct` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `ut` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

create database if not exists test2;
CREATE TABLE test2.`class` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `school_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT 'school id',
  `name` varchar(64) NOT NULL DEFAULT '' COMMENT 'name',
  `ct` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `ut` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

create database if not exists test3;
CREATE TABLE test3.`school` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(64) NOT NULL DEFAULT '' COMMENT 'name',
  `ct` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `ut` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



```
insert into test1.`user`(class_id, name) values(1, 'zhao1');
insert into test1.`user`(class_id, name) values(1, 'qian1');
insert into test1.`user`(class_id, name) values(1, 'sun1');
insert into test1.`user`(class_id, name) values(1, 'li1');

insert into test1.`user`(class_id, name) values(2, 'zhao2');
insert into test1.`user`(class_id, name) values(2, 'qian2');
insert into test1.`user`(class_id, name) values(2, 'sun2');
insert into test1.`user`(class_id, name) values(2, 'li2');
```

```
insert into test2.`class`(id, name, school_id) values(1, 'class 1', 1);
insert into test2.`class`(id, name, school_id) values(2, 'class 2', 2);
```

```
insert into test3.`school`(id, name) values(1, 'national school 1');
insert into test3.`school`(id, name) values(2, 'national school 2');
```
