import unittest

import mock
import sendsqs

class Test(unittest.TestCase):
    @mock.patch('boto3.resources.base.ServiceResource')
    def testSendWithNoQueueCreated(self, sqs):
        sqs.get_queue_by_name.side_effect = Exception()
        sendsqs.send_message('aqueue', 'message', sqs)
        sqs.create_queue.assert_called_with(QueueName='aqueue')

    @mock.patch('boto3.resources.base.ServiceResource')
    def testSendWithQueueCreatedAlready(self, sqs):
        sendsqs.send_message('aqueue', 'message', sqs)
        sqs.get_queue_by_name.assert_called_with(QueueName='aqueue')
        sqs.get_queue_by_name(QueueName='aqueue')\
            .send_message.assert_called_with(MessageBody='message')
        sqs.create_queue.assert_not_called()

if __name__ == '__main__':
    unittest.main()
