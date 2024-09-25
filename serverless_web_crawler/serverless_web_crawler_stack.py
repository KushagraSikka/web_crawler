from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_lambda_python_alpha as _alambda,
    aws_dynamodb as _dynamodb,
    aws_sqs as _sqs,
    aws_lambda_event_sources as _event_source,
)
from constructs import Construct

class ServerlessWebCrawlerStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Constants
        DYNAMODB_TABLE_NAME = "VisitedURLs"
        QUEUE_NAME = "Crawler"
        DLQ_NAME = "Crawler-DLQ"
        LAMBDA_ENTRY = "./lambda/"
        RUNTIME = _lambda.Runtime.PYTHON_3_9

        # DynamoDB Table: VisitedURLs
        visited_urls_table = _dynamodb.Table(
            self,
            "VisitedURLsTable",
            table_name=DYNAMODB_TABLE_NAME,
            partition_key=_dynamodb.Attribute(
                name="visitedURL",
                type=_dynamodb.AttributeType.STRING
            ),
            sort_key=_dynamodb.Attribute(
                name="runId",
                type=_dynamodb.AttributeType.STRING
            ),
            billing_mode=_dynamodb.BillingMode.PAY_PER_REQUEST,
        )

        # Dead-Letter Queue
        crawler_dlq = _sqs.Queue(
            self,
            "CrawlerDLQ",
            queue_name=DLQ_NAME
        )

        # SQS Queue with Dead-Letter Queue
        crawler_queue = _sqs.Queue(
            self,
            "CrawlerQueue",
            queue_name=QUEUE_NAME,
            dead_letter_queue=_sqs.DeadLetterQueue(
                max_receive_count=3,
                queue=crawler_dlq
            ),
        )

        # Environment Variables for Lambdas
        lambda_environment = {
            "VISITED_URLS_TABLE_NAME": DYNAMODB_TABLE_NAME,
            "CRAWLER_QUEUE_URL": crawler_queue.queue_url,
        }

        # Initiator Lambda Function
        initiator_function = _alambda.PythonFunction(
            self,
            "InitiatorFunction",
            entry=LAMBDA_ENTRY,
            runtime=RUNTIME,
            index="initiator.py",
            handler="handle",
            environment=lambda_environment,
        )

        # Crawler Lambda Function
        crawler_function = _alambda.PythonFunction(
            self,
            "CrawlerFunction",
            entry=LAMBDA_ENTRY,
            runtime=RUNTIME,
            index="crawler.py",
            handler="handle",
            reserved_concurrent_executions=2,
            environment=lambda_environment,
        )

        # Grant Permissions to Lambda Functions
        visited_urls_table.grant_read_write_data(initiator_function)
        visited_urls_table.grant_read_write_data(crawler_function)

        crawler_queue.grant_send_messages(initiator_function)
        crawler_queue.grant_send_messages(crawler_function)
        crawler_queue.grant_consume_messages(crawler_function)

        # Add SQS Event Source to Crawler Function
        crawler_function.add_event_source(
            _event_source.SqsEventSource(crawler_queue, batch_size=1)
        )
