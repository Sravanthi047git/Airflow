-- Summary table having URI and dte

-- HIVE CONF
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

-- external parameters
-- set RUN_DATE=2021-01-05;
-- USAGE:
-- hive -hiveconf RUN_DATE='2021-01-05' -f summary.hql


CREATE EXTERNAL TABLE IF NOT EXISTS prod.summary_access_log(
    username STRING,
    count INT)
PARTITIONED BY (dte STRING)
STORED AS orc
LOCATION 's3://twinkle-de-playground/prod/summary-access-log/';


INSERT OVERWRITE TABLE prod.summary_access_log
PARTITION(dte)
  SELECT
    username,
    COUNT(username) as count,
    dte
    FROM prod.clean_access_log
    GROUP BY dte,username ;


---select count(1) from prod.summary_access_log;
---select username,count from prod.summary_access_log;