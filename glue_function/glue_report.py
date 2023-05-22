# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path
import os
import boto3
import awswrangler as wr
import pandas as pd
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

region = os.environ.get('REGION')
account = os.environ.get('ACCOUNT')
DDB_TABLE = os.environ.get('DDB_TABLE')
SES_EMAIL = os.environ.get('SES_EMAIL')
# Create a new SES resource and specify a region.
ses = boto3.client('ses', region_name=region)


def lambda_handler(event, context):

    df = wr.dynamodb.read_partiql_query(
        query=f"SELECT * FROM \"{DDB_TABLE}\""
    )

    pdf = pd.DataFrame(df)
    pdf['job_runtime_mins'] = pdf['job_runtime'].astype(float).div(60)
    pdf['num_workers'] = pdf['num_workers'].astype(float)
    pdf['dpu_seconds'] = pdf['dpu_seconds'].astype(float)
    pdf['rate'] = pdf['rate'].astype(float)
    pdf['job_version'] = pdf['job_version'].astype(float)
    pdf['price_mult'] = pdf['price_mult'].astype(float)

    # DPU hours = (DPU Seconds /60 /60)
    # autoscaling job cost = DPU hours * DPU rate/hr (.44)

    minimum_billed_dur = 1
    minimum_billed_dur_legacy = 10

    def calc_cost(row):
        if row['job_runtime_seconds'] == 0:
            return 0
        elif row['job_version'] > 2.0:
            if row['is_autoscaled'] == 'true' and row['job_runtime_mins'] > minimum_billed_dur and row['dpu_seconds'] > 0:
                return ((row['dpu_seconds'] / 60) / 60) * row['rate']
            elif row['is_autoscaled'] == 'false' and row['job_runtime_mins'] > minimum_billed_dur:
                return (row['job_runtime_mins'] / 60) * row['num_workers'] * row['rate'] * row['price_mult']
            else:
                return (minimum_billed_dur / 60) * row['num_workers'] * row['rate'] * row['price_mult']
        else:
            if row['job_runtime_mins'] > minimum_billed_dur_legacy:
                return (row['job_runtime_mins'] / 60) * row['num_workers'] * row['rate']
            else:
                return (minimum_billed_dur_legacy / 60) * row['num_workers'] * row['rate']

    pdf['est_cost'] = pdf.apply(calc_cost, axis=1)

    # Total job runtime, total attempts, average workers per job
    result = pdf.groupby('job_name').aggregate({'job_runtime_mins': 'sum', 'run_attempt': 'sum', 'num_workers': 'mean', 'est_cost': 'sum'}).sort_values(
        by='job_runtime_mins', ascending=False).round(3).rename({'run_attempt': 'retries', 'num_workers': 'avg_workers'}, axis=1)

    data = result.to_html()

    # Job run counts by status
    pdf['successful'] = pdf['job_state'].str.contains('SUCCEEDED')
    pdf['timeout'] = pdf['job_state'].str.contains('TIMEOUT')
    pdf['failed'] = pdf['job_state'].str.contains('FAILED')
    pdf['stopped'] = pdf['job_state'].str.contains('STOPPED')

    status_count = pdf.groupby('job_name')[['successful', 'timeout', 'failed', 'stopped']].sum(
    ).sort_values(by='failed', ascending=False)
    print(status_count)
    stats = status_count.to_html()

    # Count job runs by worker type
    worker_jobs = pdf.groupby('worker_type')[['worker_type']].count().rename(
        {'worker_type': 'count'}, axis=1)
    worker_jobs_html = worker_jobs.to_html()

    # Count Standard and Flex jobs
    job_run_type = pdf.groupby('exec_class')[['exec_class']].count().rename({
        'exec_class': 'count'}, axis=1)
    job_run_html = job_run_type.to_html()

    # Count number of job runs by Glue version
    job_versions = pdf.groupby('job_version')[['job_version']].count().rename({
        'job_version': 'count'}, axis=1)
    job_version_html = job_versions.to_html()

    SENDER = SES_EMAIL

    # Replace recipient@example.com with a "To" address. If your account
    # is still in the sandbox, this address must be verified.
    RECIPIENT = SES_EMAIL

    # The subject line for the email.
    SUBJECT = "AWS Glue Job Run Report"

    # The email body for recipients with non-HTML email clients.
    BODY_TEXT = ("AWS Glue Weekly Job Run Report\r\n")

    # The HTML body of the email.
    BODY_HTML = """<html>
    <head></head>
    <body>
      <h1>AWS Glue Weekly Job Run Report</h1>
      <h4>
        Total job runtime (in minutes), total attempts, average workers per job, estimated cost.
      </h4>
      <p>
        {}
      </p>
      <h4>
        Job runs by status, per job.
      </h4>
      <p>
        {}
      </p>
      <h4>
        Breakdown of number of job runs per worker type.
      </h4>
      <p>
        {}
      </p>
      <h4>
        Breakdown of number of job runs per execution class (Standard vs Flex).
      </h4>
      <p>
        {}
      </p>
      <h4>
        Breakdown of number of job runs by Glue version.
      </h4>
      <p>
        {}
      </p>
    </body>
    </html>
                """.format(data, stats, worker_jobs_html, job_run_html, job_version_html)

    # The character encoding for the email.
    CHARSET = "UTF-8"

    # Try to send the email.
    try:
        print("sending email")
        # Provide the contents of the email.
        response = ses.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT,
                ],
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data': BODY_HTML,
                    },
                    'Text': {
                        'Charset': CHARSET,
                        'Data': BODY_TEXT,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER
        )
    # Display an error if something goes wrong.
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])
