## 建表，导入数据

```sql
#创建数据库
create database ods;
create database dwd;
create database dws;
create database ads;
```

## 导入原始数据放到ods层

```sql
#建表
create table ODS_DY(
    id bigint,
    title string,
    director string,
    movie_type string,
    producer_country string,
    language string,
    release_time string,
    film_length string,
    score double,
    comment_num string #这个字段建议用bigint
)row format delimited fields terminated by ','
tblproperties("skip.header.line.count"="1");
#导入数据
load data local inpath '/usr/local/soft/data/moviesData2.txt' into table ods_dy;
```

## 对原始数据进行处理，并放到dwd层 ，去除脏数据

```sql
create table dwd.dwd_dy like ods.ods_dy;
#对ods层数据进行清洗
insert overwrite table dwd.dwd_dy
select id,
title,
director,
split(movie_type,"|")[0] as movie_type,
split(producer_country,"|")[0] as producer_country,
split(language,"|")[0] as language,
substring(release_time,1,4) as release_time,
film_length,
score,
comment_num
from ods.ods_dy where release_time is not null;
#根据dwd表的格式，创建dws层的表
create table dws.dws_dy like dwd.dwd_dy;
insert overwrite table dws.dws_dy
select * from dwd.dwd_dy;
```

## 对dws层的数据进行指标分析，并把结果保存到ads层

```sql
#1、电影数量分析（按年）(按年分组 统计每年电影数量)
create table ads.ads_dyYear(
release_time string,
movies_num bigint
)row format delimited fields terminated by ',';

insert overwrite table ads.ads_dyYear
select release_time,count(1) as movies_num
from dws.dws_dy
group by release_time;
#2、电影数量分析（按国家）
create table ads.ads_dycountry(
producer_country string,
country_movies_num bigint
)row format delimited fields terminated by ',';

insert overwrite table ads.ads_dycountry
select producer_country,count(1) as movies_num
from dws.dws_dy
group by producer_country;
#3、每年评分的均值
create table ads.ads_dyavgscore(
release_time string,
avg_score double
)row format delimited fields terminated by ',';

insert overwrite table ads.ads_dyavgscore
select release_time,round(avg(score),3) as avg_num
from dws.dws_dy
group by release_time;
#（4）中国大陆与其他国家电影评分对比；
# 需要将每个国家的平均分查询出来，以此为基础，让各个国家与中国的平均分数进行比较，如果中国的分数低于目标国家，则显示判断为0，如果高于则为1。
create table ads.ads_countryCompare(
producer_country string,
compareResult int
)row format delimited fields terminated by ',';

select producer_country,round(avg(score),3)
from dws.dws_dy
where producer_country != '中'
group by producer_country;

select producer_country,round(avg(score),3)
from dws.dws_dy
where producer_country = '中'
group by producer_country;

insert overwrite table ads.ads_countryCompare
select a.producer_country,
(case when a.avg_score > b.avg_score then '0' else '1' end) as compareResult
from(
select producer_country,round(avg(score),3) as avg_score
from dws.dws_dy
where producer_country != '中'
group by producer_country
) a,(
select round(avg(score),3) as avg_score
from dws.dws_dy
where producer_country = '中'
) b;
# （5）按照烂片率由高到低排序，烂片定义：评分低于6.35分；
# 烂片率 = 这个类别评分低于6.35的电影总数量/这个类别的电影总数量
create table ads.ads_dytypeScore(
movie_type string,
type_sc double
)row format delimited fields terminated by ',';

select m.movie_type,count(1) as count_num
from dws.dws_dy m
where m.score <= 6.35 and m.score > 0.0
group by m.movie_type;

select w.movie_type,count(1) as count_num
from dws.dws_dy w
group by w.movie_type;

insert overwrite table ads.ads_dytypeScore
select a.movie_type,round((a.count_num/b.count_num),4) as compare
from (
select m.movie_type,count(1) as count_num
from dws.dws_dy m
where m.score <= 6.35 and m.score > 0.0
group by m.movie_type
) a left join (
select w.movie_type,count(1) as count_num
from dws.dws_dy w
group by w.movie_type
) b on a.movie_type = b.movie_type
order by compare;
```

## 自己的指标

```sql
#自己的指标:电影评分的均值（按照国家）
create table ads.xuwnag_country_avgscore(
producer_country string,
avg_score double
)row format delimited fields terminated by ',';

insert overwrite table ads.xuwnag_country_avgscore
select producer_country,round(avg(score),3) as country_avg_num
from dws.dws_dy
group by producer_country;

# 按照烂片率由高到低排序，烂片定义：评分低于7.5分；
# 好片率 = 这个类别评分大于7.5的电影总数量/这个类别的电影总数量
create table ads.xuwang_goodmovie(
movie_type string,
type_sc double
)row format delimited fields terminated by ',';

insert overwrite table ads.xuwang_goodmovie
select a.movie_type,round((a.count_num/b.count_num),4) as compare
from (
select m.movie_type,count(1) as count_num
from dws.dws_dy m
where m.score >= 7.5 and m.score < 10.0
group by m.movie_type
) a left join (
select w.movie_type,count(1) as count_num
from dws.dws_dy w
group by w.movie_type
) b on a.movie_type = b.movie_type
order by compare;

#评价人数最多的前50部电影
#需要把dws层的comment_num的字段类型转换成bigint，原类型为string，排序后会错误
alter table dws.dws_dy change column comment_num comment_num bigint;

create table ads.xuwang_comment_num(
title string,
comment_num bigint
)row format delimited fields terminated by ',';

insert overwrite table ads.xuwang_comment_num
select title,comment_num
from dws.dws_dy
order by comment_num desc
limit 50;
```

