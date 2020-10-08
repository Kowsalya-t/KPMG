CREATE EXTERNAL TABLE report_usage_inter(
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
  `currentdatetime` string
  )
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES (
   'separatorChar' = ',',
   'quoteChar' = '"'
   )
STORED AS TEXTFILE
LOCATION
  's3://mtdata-datalake-dev/report_usage_inter/'
TBLPROPERTIES (
  'skip.header.line.count'='1'
  );
