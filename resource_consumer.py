import csv
import boto3
import xml.etree.ElementTree as ET
import sys
import os
from datetime import datetime , date, timedelta
import re

client = boto3.client('s3')
s3 = boto3.resource('s3')

S3_OUTPUT = os.environ['S3_OUTPUT']
TARGET_BUCKET = os.environ['TARGET_BUCKET']

def report(event, context):
    key = get_event_data(event)['key']
    sequencer = get_event_data(event)['sequencer']
    stackname = get_event_data(event)['stackname']
    
    timeNow = get_cur_time()['timeNow']
    timeNowMinute = get_cur_time()['timeNowMinute']
    timeNowPrevMinute = timeNowMinute - timedelta(minutes=1)
    
    obj = s3.Object(TARGET_BUCKET, key)
    body = obj.get()['Body'].read()
    root = ET.fromstring(body)
    
    fileOutput = '/tmp/report_request.csv'
    csv_columns =  ['ApplicationName','UserID','ID','SourceName','ReportName','Description','TimezoneName','SubmitTime','State','LastInfo','Emails','StackName','CurrentTime']
    
    csvfile = open(fileOutput, 'w',newline='', encoding='utf-8')
    writer = csv.writer(csvfile)
    writer.writerow(csv_columns)

    DATA_EXTRACTED = 0
    for request in root.findall('./Requests/ReportRequest[last()]'):
        #Convert SubmitTime to the format of current date time
        submitTime = request.find('SubmitTime')
        submitTime = submitTime.text
        hourMin = submitTime.split('T')[1]
        submitTime = submitTime.split('T')[0] + ' ' + hourMin.split(':')[0] + ':' + hourMin.split(':')[1]
        
        if submitTime in (timeNowMinute.strftime("%Y-%m-%d %H:%M"), timeNowPrevMinute.strftime("%Y-%m-%d %H:%M")):
            report_data = []

            withQuotes = 1
            withOutQuotes = 0

            report_data = assign_value(request,'ApplicationName',report_data,withOutQuotes)
            report_data = assign_value(request,'UserID',report_data,withOutQuotes)
            report_data = assign_value(request,'ID',report_data,withOutQuotes)
            report_data = assign_value(request,'SourceName',report_data,withOutQuotes)
            report_data = assign_value(request,'ReportName',report_data,withOutQuotes)
            report_data = assign_value(request,'Description',report_data,withQuotes)
            report_data = assign_value(request,'TimezoneName',report_data,withQuotes)
            report_data = assign_value(request,'SubmitTime',report_data,withOutQuotes)
            report_data = assign_value(request,'state',report_data,withOutQuotes)
            report_data = assign_value(request,'LastInfo',report_data,withOutQuotes)
            report_data = assign_value(request,'Emails',report_data,withOutQuotes)
            report_data.append(stackname) 
            report_data.append(timeNow)

            writer.writerow(report_data)
            DATA_EXTRACTED = 1
    csvfile.close()
    
    if DATA_EXTRACTED != 0:
        s3.meta.client.upload_file('/tmp/report_request.csv', S3_OUTPUT, 'report_usage_inter/report_request_'+sequencer+'.csv')                           
        
def get_event_data(event):
    key = event['Records'][0]['s3']['object']['key']
    sequencer = event['Records'][0]['s3']['object']['sequencer']
    stackname = event['Records'][0]['s3']['object']['key'].split('/')[0]
    return {'key': key, 'sequencer': sequencer, 'stackname':stackname }

def get_cur_time():
    timeNow = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    timeNowMinute = datetime.utcnow()
    return {'timeNow' : timeNow, 'timeNowMinute' : timeNowMinute }

def assign_value(request,field,report_data,quotes):
    param = request.find(field)
    if param != None:
        if quotes != 0:
            if param.text != None:
                param = '"' + re.sub('\s+',' ',param.text) + '"'
            else:
                param = ""    
        else:
            if param.text != None:
                param = re.sub('\s+',' ',param.text)
            else:
                param = ''      
    else:
        if quotes != 0:
            param = ""
        else:
            param = ''
    report_data.append(param)
    return  report_data          
