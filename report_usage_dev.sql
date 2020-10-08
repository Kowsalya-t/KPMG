CREATE EXTERNAL TABLE report_usage(
  `applicationname` string,
  `userid` string,
  `id` string,
  `sourcename` string,
  `reportname` string,
  `description` string,
  `timezonename` string,
  `submittime` string,
  `state` string,
  `lastinfo` string,
  `emails` string,
  `stackname` string,
   `start_date` string,
   `end_date` string,
  `execution_time` int
  )
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES (
   'separatorChar' = ',',
   'quoteChar' = '"'
   )
STORED AS TEXTFILE
LOCATION
  's3://mtdata-datalake-dev/report_usage_query_output/'
TBLPROPERTIES (
  'skip.header.line.count'='1'
  );
