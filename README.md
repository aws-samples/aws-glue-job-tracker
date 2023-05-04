# aws-glue-job-tracker

# Deploy the solution
This AWS SAM application provisions the following resources:

•	Two EventBridge rules
•	Two Lambda functions
•	An SNS topic and subscription
•	A DynamoDB table
•	An SES subscription
•	The required IAM roles and policies

To deploy the AWS SAM application, complete the following steps:

1.	Clone the aws-samples GitHub repository: 

git clone https://github.com/aws-samples/glue-job-tracker.git

2.	Deploy the AWS SAM application:

cd glue-job-tracker
sam deploy –guided

3.	Provide the following parameters:
a.	GlueJobWorkerLimit – Enter the maximum number of workers you want an AWS Glue job to be able to run with. The default is 10.
b.	GlueJobTimeoutLimit – Enter the maximum time you want an AWS Glue job to run before it times out. The default is 480 minutes (8 hours).
c.	GlueJobNotifications – Enter an email or distribution list of those who need to be notified through Amazon SNS and Amazon SES. You can go to the SNS topic after the deployment is complete and add emails as needed.

To receive emails from Amazon SNS and Amazon SES, you must confirm your subscriptions. After the stack is deployed, check your email that was specified in the template and confirm by choosing the link in each message. 

When the application is successfully provisioned, it will begin monitoring your AWS Glue ETL job environment. The next time a job fails, times out, or exceeds a specified limit, you will receive an email via Amazon SNS. For example, the following screenshot shows an SNS message about a job that succeeded but had a timeout limit violation.

You might have jobs that need to run at a higher worker or timeout limit, and you don’t want the solution to evaluate them. You can simply tag that job with the key/value of remediate and false. The step function will still be invoked, but will use the PASS state when it recognizes the tag. For more information on job tagging, refer to AWS tags in AWS Glue.

License

Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
