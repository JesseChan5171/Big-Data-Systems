create external table movielens_sm (
            user_id INT,
            mov_id INT)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde' with serdeproperties (
    "separatorChar" = ",",
    "quoteChar" = "\'")
stored as textfile
location '/user/s1155124983/movie_large';

CREATE TABLE user_num AS
select user_id, count(*) as mov_count1
from movielens_sm 
group by user_id;

CREATE TABLE user_num2 AS
select user_id As user_id2, count(*) as mov_count2
from movielens_sm 
group by user_id;

CREATE TABLE join_u1u2 AS
select t1.user_id as user_id1, t2.user_id as user_id2, count(*) AS co_watch
from movielens_sm as t1 join movielens_sm as t2 
on (t1.mov_id == t2.mov_id)
group by t1.user_id, t2.user_id;

CREATE TABLE join_u1u2_num1 AS
select u1u2.user_id1, user_id2, co_watch, mov_count1
from join_u1u2 as u1u2 join user_num as unum 
on (u1u2.user_id1 == unum.user_id);

CREATE TABLE join_u1u2_num2 AS
select u1u2_1.user_id1, u1u2_1.user_id2, co_watch, mov_count1, mov_count2 
from join_u1u2_num1 as u1u2_1 join user_num2 as unum2 
on (u1u2_1.user_id2 == unum2.user_id2);

CREATE TABLE Sim_t AS
select t2.user_id1, t2.user_id2, co_watch/(mov_count1+mov_count2-co_watch) As sim
from join_u1u2_num2 as t2
where user_id1 != user_id2;

CREATE TABLE Sim_t_top AS
select user_id1,user_id2,sim,
ROW_NUMBER() OVER (PARTITION BY user_id1 ORDER BY sim DESC) as rank
from Sim_t;


CREATE TABLE Sim_t_top_3 AS
select user_id1,user_id2, sim from Sim_t_top
where rank < 4;

CREATE TABLE Sim_t_top_3_format AS
select st3.user_id1, concat_ws(',', collect_list(st3.user_id2)) 
from Sim_t_top_3 as st3
group by user_id1;

CREATE TABLE Ans_115512_4983 AS
select *
from Sim_t_top_3_format as st3f
where st3f.user_id1 = 4983 or st3f.user_id1 = 14983 or st3f.user_id1 = 24983  or st3f.user_id1 = 34983 or st3f.user_id1 = 44983  or st3f.user_id1 = 54983 or st3f.user_id1 = 64983 or st3f.user_id1 = 74983 or st3f.user_id1 = 84983 or st3f.user_id1 = 94983
order by st3f.user_id1 desc;

