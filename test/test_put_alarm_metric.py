import unittest

import mock
import put_alarm_metric

class Test(unittest.TestCase):
    @mock.patch('boto3.resources.base.ServiceResource')
    def testPutAlarmMetric(self, cloudwatch):
        alarm_name = 'alarm'
        put_alarm_metric.put_alarm_metric(alarm_name, cloudwatch)
        cloudwatch.put_metric_alarm.assert_called_with(ActionsEnabled=True, AlarmActions=['arn:aws:sns:us-east-1:992173438675:PagerDuty-Core-Alerts'], AlarmDescription=alarm_name, AlarmName=alarm_name, ComparisonOperator='LessThanThreshold', Dimensions=[{'Name': 'QueueName', 'Value': alarm_name}], EvaluationPeriods=15, MetricName='NumberOfMessagesSent', Namespace='AWS/SQS', Period=3600, Statistic='Sum', Threshold=1, Unit='Seconds')


if __name__ == '__main__':
    unittest.main()
