import optparse
import sys
import boto3


def put_alarm_metric(alarm_name, cloudwatch):
    ret = cloudwatch.put_metric_alarm(
        AlarmName=alarm_name,
        ComparisonOperator='LessThanThreshold',
        EvaluationPeriods=15,
        MetricName='NumberOfMessagesSent',
        Namespace='AWS/SQS',
        Period=3600,
        Statistic='Sum',
        Threshold=1,
        ActionsEnabled=True,
        AlarmActions=[
            'arn:aws:sns:us-east-1:992173438675:PagerDuty-Core-Alerts',
        ],
        AlarmDescription=alarm_name,
        Dimensions=[
            {
              'Name': 'QueueName',
              'Value': alarm_name
            },
        ],
        Unit='Seconds'
    )
    return ret


if __name__ == "__main__":
    parser = optparse.OptionParser("<alarm-name>")
    (options, args) = parser.parse_args()

    if len(args) != 1:
        parser.print_help()
        sys.exit(1)
    else:
        # Get the service resource
        cloudwatch = boto3.client('cloudwatch')
        response = put_alarm_metric(args[0], cloudwatch)
        if response:
            print("created alarm: ", response)
        else:
            print("unable to create alarm")
            sys.exit(1)
