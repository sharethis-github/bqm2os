
import optparse
import sys
import boto3


def send_message(queue: str, message: str, sqs):
    try:
        """ Get the queue. This returns an SQS.Queue instance """
        queue = sqs.get_queue_by_name(QueueName=queue)
    except Exception:
        # let's create the queue and recurse
        queue = sqs.create_queue(QueueName=queue)

    if queue:
        return queue.send_message(MessageBody=message)
    else:
        return None


if __name__ == "__main__":
    parser = optparse.OptionParser("<sqs-queue> <message>")
    (options, args) = parser.parse_args()

    if len(args) != 2:
        parser.print_help()
        sys.exit(1)
    else:
        # Get the service resource
        sqs = boto3.resource('sqs')
        response = send_message(args[0], args[1], sqs)
        if response:
            print("sent message: ", response.get('MessageId'))
        else:
            print("unable to send message")
            sys.exit(1)
