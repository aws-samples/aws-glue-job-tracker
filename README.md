# aws-glue-job-tracker

# Deploy the solution
This AWS SAM application provisions the following resources:

*	Two EventBridge rules
*	Two Lambda functions
*	An SNS topic and subscription
*	A DynamoDB table
*	An SES subscription
*	The required IAM roles and policies

To deploy the AWS SAM application, complete the following steps:

1.	Clone the aws-samples GitHub repository: 
```
git clone https://github.com/aws-samples/glue-job-tracker.git
```
2.	Deploy the AWS SAM application:
```
cd aws-glue-job-tracker
sam deploy --guided
```

3.	Provide the following parameters:

* GlueJobWorkerThreshold – Enter the maximum number of workers you want an AWS Glue job to be able to run with before sending threshold alert. The default is 10. An alert will be sent if a Glue job runs with higher workers than specified.
* GlueJobDurationThreshold – Enter the maximum duration in minutes you want an AWS Glue job to run before sending threshold alert. The default is 480 minutes (8 hours). An alert will be sent if a Glue job runs with higher job duration than specified.
* GlueJobNotifications – Enter an email or distribution list of those who need to be notified through Amazon SNS and Amazon SES. You can go to the SNS topic after the deployment is complete and add emails as needed.

To receive emails from Amazon SNS and Amazon SES, you must confirm your subscriptions. After the stack is deployed, check your email that was specified in the template and confirm by choosing the link in each message. When the application is successfully provisioned, it will begin monitoring your AWS Glue for Apache Spark job environment. The next time a job fails, times out, or exceeds a specified threshold, you will receive an email via Amazon SNS. For example, the following screenshot shows an SNS message about a job that succeeded but had a job duration threshold violation

You might have jobs that need to run at a higher worker or timeout limit, and you don’t want the solution to evaluate them. You can simply tag that job with the key/value of remediate and false. The step function will still be invoked, but will use the PASS state when it recognizes the tag. For more information on job tagging, refer to AWS tags in AWS Glue.

# License

This library is licensed under the MIT-0 License. See the LICENSE file.
