select applicationname,userid,id,sourcename,reportname,description,timezonename,submittime,state,max(lastinfo) lastinfo,emails,stackname,
min(date_parse(currentdatetime, '%Y-%m-%d %H:%i:%s')) start_date,
max(date_parse(currentdatetime, '%Y-%m-%d %H:%i:%s')) end_date,
date_diff('second',min(date_parse(currentdatetime, '%Y-%m-%d %H:%i:%s')),max(date_parse(currentdatetime, '%Y-%m-%d %H:%i:%s'))) time_diff
from report_usage_inter
where currentdatetime is not null
group by applicationname,userid,id,sourcename,reportname,description,timezonename,submittime,state,emails,stackname;