from aws_cdk import Stack
from aws_lambda import Runtime as LambdaRuntime
from aws_lambda_python_alpha import PythonFunction
from aws_dynamodb import Table, Attribute, AttributeType, BillingMode
from aws_sqs import Queue
from aws_lambda_event_sources import SqsEventSource
from constructs import Construct

class ServerlessWebCrawlerStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # Initialize DynamoDB table for visited URLs
        table = Table(
            self, "VisitedURLs",
            table_name="VisitedURLs",
            partition_key=Attribute(name="visitedURL", type=AttributeType.STRING),
            sort_key=Attribute(name="runId", type=AttributeType.STRING),
            billing_mode=BillingMode.PAY_PER_REQUEST
        )

        # Initialize SQS queues for crawling tasks and dead-letter handling
        crawler_queue = Queue(self, "CrawlerQueue", queue_name="CrawlerQueue")
        dead_letter_queue = Queue(self, "CrawlerDLQ", queue_name="CrawlerDLQ")

        # Define lambda functions for initiating and handling crawling
        initiator_function = PythonFunction(
            self, "InitiatorFn",
            entry="./lambda/",
            runtime=LambdaRuntime.PYTHON_3_9,
            index="initiator.py",
            handler="handle"
        )

        crawler_function = PythonFunction(
            self, "CrawlerFn",
            entry="./lambda/",
            runtime=LambdaRuntime.PYTHON_3_9,
            index="crawler.py",
            handler="handle",
            reserved_concurrent_executions=2,
            dead_letter_queue_enabled=True,
            dead_letter_queue=dead_letter_queue
        )

        # Setup permissions for SQS and DynamoDB interactions
        crawler_queue.grant_send_messages(initiator_function)
        crawler_queue.grant_send_messages(crawler_function)
        crawler_queue.grant_consume_messages(crawler_function)
        table.grant_read_write_data(initiator_function)
        table.grant_read_write_data(crawler_function)

        # Attach the SQS queue as an event source for the crawler lambda function
        crawler_function.add_event_source(SqsEventSource(crawler_queue, batch_size=1))
