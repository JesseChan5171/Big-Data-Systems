bigram_avg = LOAD 'bigram_1c/part-v004-o000-r-00000' USING PigStorage('\t') AS 
    (bigram:chararray,
    avg_occ:float
    );

Ord_occ = ORDER bigram_avg by avg_occ DESC;

dump_t = LIMIT Ord_occ 20;
dump dump_t;

STORE dump_t INTO 'hdfs:///user/s1155124983/bigram_1d' USING PigStorage('\t');
