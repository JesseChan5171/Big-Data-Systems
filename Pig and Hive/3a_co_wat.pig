movielens = LOAD 'hdfs:///user/s1155124983/movie_small/movielens_small.csv' USING PigStorage(',') AS 
    (user_id:int,
    mov_id:int
    );

movielens_grpd = GROUP movielens BY mov_id;
movielens_grpd_dbl = FOREACH movielens_grpd GENERATE group, movielens.user_id AS userId1, movielens.user_id AS userId2;

cowatch = FOREACH movielens_grpd_dbl GENERATE FLATTEN(userId1) as userId1, FLATTEN(userId2) as userId2;
cowatch_filtered = FILTER cowatch BY userId1 < userId2;

dump_t = LIMIT cowatch_filtered 20;
dump dump_t;
