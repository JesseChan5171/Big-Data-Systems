create external table bigram_a (
            bigram STRING,
            year INT,
            match_count INT,
            volume_count INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
stored as textfile
location '/user/s1155124983/bigram_1a';

create external table bigram_b (
            bigram STRING,
            year INT,
            match_count INT,
            volume_count INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
stored as textfile
location '/user/s1155124983/bigram_1b/';


CREATE TABLE bigram_ab as
SELECT * FROM
(select * from bigram_a UNION ALL select * from bigram_b) 
unioned;

INSERT OVERWRITE DIRECTORY "hdfs:///user/s1155124983/hive_bigram_ab" 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT *
FROM bigram_ab;
