# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json
import boto3
import time
import os
from datetime import datetime
from decimal import Decimal

glue = boto3.client('glue')
sns = boto3.client('sns')
dynamo = boto3.resource('dynamodb')

region = os.environ.get('REGION')
account = os.environ.get('ACCOUNT')
WORKER_LIMIT = os.environ.get('WORKER_LIMIT')
TIMEOUT_LIMIT = os.environ.get('TIMEOUT_LIMIT')
SNS_TOPIC = os.environ.get('SNS_TOPIC')
DDB_TABLE = os.environ.get('DDB_TABLE')

dynamo_tbl = dynamo.Table(DDB_TABLE)

def lambda_handler(event, context):
    print(event)
    
    event_job_name = event['detail']['jobName']
    event_job_run_id = event['detail']['jobRunId']
    
    print('calling glue get job run...')
    get_glue_job = glue.get_job_run(
        JobName = event_job_name,
        RunId = event_job_run_id
        )
    print(get_glue_job)
    
    json_data = json.dumps(get_glue_job, default=str)
    
    glue_job = json.loads(json_data, parse_float=lambda x: round(Decimal(x),2))

    job_name = glue_job['JobRun']['JobName']
    job_run_id = glue_job['JobRun']['Id']
    num_workers = glue_job['JobRun']['NumberOfWorkers']
    job_timeout = glue_job['JobRun']['Timeout']
    run_attempt = glue_job['JobRun']['Attempt']
    job_state = glue_job['JobRun']['JobRunState']
    worker_type = glue_job['JobRun']['WorkerType']
    job_runtime = glue_job['JobRun']['ExecutionTime']
    job_start_time = glue_job['JobRun']['StartedOn']
    job_end_time = glue_job['JobRun']['CompletedOn']
    job_version = glue_job['JobRun']['GlueVersion']
    exec_class = glue_job['JobRun']['ExecutionClass']
    glue_id = str(job_run_id + '_' + job_start_time)
    
    def check_autoscaling():
        if 'DPUSeconds' in glue_job['JobRun']:
            dpu_seconds = glue_job['JobRun']['DPUSeconds']
            is_autoscaled = 'true'
            return dpu_seconds, is_autoscaled
        else:
            dpu_seconds = 0
            is_autoscaled = 'false'
            return dpu_seconds, is_autoscaled
            
    # set pricing rate between flex or standard
    def add_rate():
        if exec_class == 'STANDARD':
            rate = Decimal('0.44')
            return rate
        elif exec_class == 'FLEX':
            rate = Decimal('0.29')
            return rate
        else:
            rate = Decimal('0.44')
            return rate
    
    dpu_seconds, is_autoscaled = check_autoscaling()
    rate = add_rate()
    
    job_run_link = 'https://{}.console.aws.amazon.com/gluestudio/home?region={}#/job/{}/run/{} '.format(region, region, job_name, job_run_id)
    
    sns_flagged = "The Glue job: {} for job run id {} completed successfully but had limit violation(s), please refer to below and adjust accordingly.\n\n"\
                  "Provisioned workers = {}.\n"\
                  "Worker limit = {}.\n"\
                  "Provisioned job timeout = {}.\n"\
                  "Timout limit = {}.\n"\
                  "Follow this link to go to the job run - {}".format(job_name, job_run_id, num_workers, WORKER_LIMIT, job_timeout, TIMEOUT_LIMIT, job_run_link)
    
    sns_alert = "The Glue job: {} for job run id {}: is in the status of {} and run attempt #{}.\n\n"\
                "Follow this link to go to the job run - {}".format(job_name, job_run_id, job_state, run_attempt, job_run_link)

    def send_sns(message):
        sns.publish(
        TopicArn = SNS_TOPIC,
        Message = message,
        Subject = 'AWS Glue Notification'
        )
    
    def dynamo_ttl():
        current_time = int(time.time())
        add_week = 604800
        ddb_ttl = current_time + add_week
        return ddb_ttl
    
    job_tags = glue.get_tags(
        ResourceArn='arn:aws:glue:{}:{}:job/{}'.format(region, account, job_name)
    )
    # print(job_tags)
    
    # log job run to DynamoDB for later aggregation and insights
    dynamo_tbl.put_item(
        Item = {
            'glue_id': glue_id,
            'job_name': job_name,
            'job_run_id': job_run_id,
            'job_name': job_name,
            'num_workers': num_workers,
            'job_timeout': job_timeout,
            'run_attempt': run_attempt,
            'job_state': job_state,
            'worker_type': worker_type,
            'job_runtime': job_runtime,
            'job_start_time': job_start_time,
            'job_end_time': job_end_time,
            'job_version': job_version,
            'dpu_seconds': dpu_seconds,
            'exec_class': exec_class,
            'is_autoscaled': is_autoscaled,
            'rate': rate,
            'ttl': dynamo_ttl()
        }
    )
    
    try:
        if 'remediate' in job_tags['Tags'] and job_tags['Tags']['remediate'] == 'false':
            print('ignoring job')
            return {
                'status': 'PASS',
                'job_name': job_name
            }
        elif (num_workers > int(WORKER_LIMIT) or job_timeout > int(TIMEOUT_LIMIT)) and job_state == 'SUCCEEDED':
            print('Workers and/or timeout is over allowed limit. The job Succeeded, but notifying about the limit discrepancy...')
            send_sns(sns_flagged)
            return {
                'status': 'FLAGGED',
                'num_workers': num_workers,
                'job_timeout': job_timeout,
                'worker_limit': int(WORKER_LIMIT),
                'timeout_limit': int(TIMEOUT_LIMIT),
                'job_name': job_name,
                'sns_message': sns_flagged
            }
        elif job_state == 'FAILED' or job_state == 'TIMEOUT':
            print('Glue job {} is in {} state, sending alert...'.format(job_name, job_state))
            send_sns(sns_alert)
            return {
                'status': 'ALERT',
                'job_name': job_name,
                'job_state': job_state,
                'sns_message': sns_alert
            }
        else:
            return {
                'status': 'PASS',
                'num_workers': num_workers,
                'worker_limit': int(WORKER_LIMIT),
                'job_name': job_name
            }
    except Exception as e:
        print('Error occured while assigning the payloads: ', e)
    
