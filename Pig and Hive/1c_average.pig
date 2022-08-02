bigram_ab = LOAD 'hdfs:///user/s1155124983/bigram_tot/bigram_tot' USING PigStorage('\t') AS 
    (bigram:chararray,
    year:int,
    match_count:int,
    volume_count:int
    );

groupByGR = GROUP bigram_ab BY bigram;


Avg_table = FOREACH groupByGR GENERATE group AS bigram, AVG(bigram_ab.match_count) AS AVG;

Ord_word = ORDER Avg_table by bigram;
STORE Ord_word INTO 'hdfs:///user/s1155124983/bigram_1c' USING PigStorage('\t');

