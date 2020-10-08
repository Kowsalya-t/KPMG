from sql_utils import ScriptReader
import os
import boto3
import time

athena_client = boto3.client('athena')
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

DATABASE = os.environ['DATABASE']
S3_QUERY_OUTPUT = os.environ['S3_QUERY_OUTPUT']
S3_OUTPUT = os.environ['S3_OUTPUT']
History_Path = "report_usage_history"

def aggregate_record(event, context):
    rollup_query = ScriptReader.get_script('rollup_query.sql')
    queryExecutionId = athena_execution(rollup_query)
    response = s3_client.delete_object(
        Bucket=S3_OUTPUT,
        Key='report_usage_query_output/'+queryExecutionId+'.csv.metadata')
    move_intermediate_data_to_history()    

def athena_execution(QueryString):    
    startQueryResponse = athena_client.start_query_execution(
        QueryString=QueryString,
        QueryExecutionContext={
            'Database': DATABASE
        },
        ResultConfiguration={
            'OutputLocation': S3_QUERY_OUTPUT,
        }
    )
    queryExecutionId = startQueryResponse['QueryExecutionId']
    queryStatus = athena_client.get_query_execution(QueryExecutionId=queryExecutionId)
    queryExecutionStatus = queryStatus['QueryExecution']['Status']['State']
    
    while queryExecutionStatus == 'QUEUED' or queryExecutionStatus == 'RUNNING' :
        time.sleep(10)
        queryStatus = athena_client.get_query_execution(QueryExecutionId=queryExecutionId)
        queryExecutionStatus = queryStatus['QueryExecution']['Status']['State']

    response = athena_client.get_query_results(
    QueryExecutionId=queryExecutionId
    )
    return queryExecutionId

def move_intermediate_data_to_history():
    no_of_files = len(s3_client.list_objects_v2(Bucket=S3_OUTPUT, Prefix='report_usage_inter/')['Contents'])
    index = 0
    while index < no_of_files:
        #Copy file from  report_usage_inter to  report_usage_inter_history
        filename = s3_client.list_objects_v2(Bucket=S3_OUTPUT, Prefix='report_usage_inter/')['Contents'][0]['Key'].split('/')[1]
        s3_resource.Object(S3_OUTPUT, "report_usage_inter_history/"+filename).copy_from(CopySource=S3_OUTPUT+"/report_usage_inter/"+filename)
        
        # Delete the object from report_usage_inter after copy
        s3_resource.Object(S3_OUTPUT, "report_usage_inter/"+filename).delete()

        index = index + 1

