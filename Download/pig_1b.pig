bigram_a = LOAD 'bigram_1a/googlebooks-eng-all-1gram-20120701-a' USING PigStorage('\t') AS 
    (bigram:chararray,
    year:int,
    match_count:int,
    volume_count:int
	);

bigram_b = LOAD 'bigram_1b/googlebooks-eng-all-1gram-20120701-b' USING PigStorage('\t') AS 
    (bigram:chararray,
    year:int,
    match_count:int,
    volume_count:int
	);

bigram_ab = UNION bigram_a, bigram_b;

STORE bigram_ab INTO 'hdfs:///user/s1155124983/bigram_ab' USING PigStorage('\t');



