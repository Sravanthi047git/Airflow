-- Creating a ETL script by passing run_date value

-- HIVE CONF
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;


-- external parameters
-- set RUN_DATE=2021-01-17;
-- USAGE:
-- hive -hiveconf RUN_DATE='2021-01-17' -f script.hql


CREATE SCHEMA IF NOT EXISTS src;
CREATE SCHEMA IF NOT EXISTS prod;


DROP TABLE src.access_log_temp;

CREATE EXTERNAL TABLE IF NOT EXISTS src.access_log_temp
(
    address string,
    birthdate string,
    blood_group string,
    company string,
    current_location string,
    job string,
    mail string,
    name string,
    residence string,
    sex string,
    ssn string,
    username string,
    website string
)
row format delimited fields terminated by ","
---lines terminated by '\n'
---ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION 's3://twinkle-de-playground/raw/access-log/${hiveconf:RUN_DATE}/'
TBLPROPERTIES ("skip.header.line.count"="1");


CREATE EXTERNAL TABLE IF NOT EXISTS prod.clean_access_log(
    address string,
    birthdate string,
    blood_group string,
    company string,
    current_location string,
    job string,
    mail string,
    name string,
    residence string,
    sex string,
    ssn string,
    username string,
    website string
)
PARTITIONED BY (dte STRING)
STORED AS ORC
LOCATION 's3://twinkle-de-playground/prod/access-log/'
;


INSERT OVERWRITE TABLE prod.clean_access_log
PARTITION(dte)
select
   address ,
    birthdate ,
    blood_group ,
    company ,
    current_location ,
    job ,
    mail ,
    name ,
    residence ,
    sex ,
    ssn ,
    username ,
    website ,
   '${hiveconf:RUN_DATE}' as dte
from src.access_log_temp
;

DROP TABLE src.access_log_temp;

--- verify data
--- select address from src.access_log_temp;
--- select address from prod.clean_access_log limit 5;

--- select birthdate from prod.clean_access_log limit 5;
--- select dte, count(1) from prod.clean_access_log group by dte;