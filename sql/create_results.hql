USE team16_projectdb;

DROP TABLE results;

CREATE TABLE results (mean_average_precision double, ndcg double, r2 double, rmse double, model string) row format delimited fields terminated by ',';

LOAD DATA INPATH '/user/team16/project/output/realeval/evaluation.csv' OVERWRITE INTO TABLE results;

SELECT * FROM results;