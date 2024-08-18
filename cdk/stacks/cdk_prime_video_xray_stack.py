# Built-in imports
import os

# External imports
from aws_cdk import (
    Duration,
    aws_dynamodb,
    aws_lambda,
    aws_logs,
    aws_iam,
    aws_events,
    aws_s3,
    aws_sqs,
    aws_stepfunctions as aws_sfn,
    aws_stepfunctions_tasks as aws_sfn_tasks,
    aws_events_targets as aws_targets,
    CfnOutput,
    RemovalPolicy,
    Stack,
    Tags,
)
from constructs import Construct


class PrimeVideoXRayStack(Stack):
    """
    Class to create the ChatbotAPI resources, which includes the API Gateway,
    Lambda Functions, DynamoDB Table, Streams and Async Processes Infrastructure.
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        main_resources_name: str,
        app_config: dict[str],
        **kwargs,
    ) -> None:
        """
        :param scope (Construct): Parent of this stack, usually an 'App' or a 'Stage', but could be any construct.
        :param construct_id (str): The construct ID of this stack (same as aws-cdk Stack 'construct_id').
        :param main_resources_name (str): The main unique identified of this stack.
        :param app_config (dict[str]): Dictionary with relevant configuration values for the stack.
        """
        super().__init__(scope, construct_id, **kwargs)

        # Input parameters
        self.construct_id = construct_id
        self.main_resources_name = main_resources_name
        self.app_config = app_config
        self.deployment_environment = self.app_config["deployment_environment"]

        # Main methods for the deployment
        self.create_s3_bucket()
        self.create_dynamodb_table()
        self.create_lambda_layers()
        self.create_lambda_functions()
        self.create_state_machine_tasks()
        self.create_state_machine_definition()
        self.create_state_machine()
        self.create_event_bridge_rules()
        self.configure_iam_permissions()

        # Generate CloudFormation outputs
        self.generate_cloudformation_outputs()

    def create_s3_bucket(self):
        """
        Method to create the S3 bucket for storing the images and videos.
        """
        self.s3_bucket = aws_s3.Bucket(
            self,
            "S3-Bucket",
            bucket_name=f"{self.app_config['s3_bucket_prefix']}-{self.account}",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            versioned=True,
            encryption=aws_s3.BucketEncryption.S3_MANAGED,
            event_bridge_enabled=True,  # Enable EventBridge notifications for S3
        )

    def create_dynamodb_table(self):
        """
        Create DynamoDB table for storing the aggregated images metadata.
        """
        self.dynamodb_table = aws_dynamodb.Table(
            self,
            "DynamoDB-Table",
            table_name=self.app_config["table_name"],
            partition_key=aws_dynamodb.Attribute(
                name="PK", type=aws_dynamodb.AttributeType.STRING
            ),
            sort_key=aws_dynamodb.Attribute(
                name="SK", type=aws_dynamodb.AttributeType.STRING
            ),
            stream=aws_dynamodb.StreamViewType.NEW_IMAGE,
            billing_mode=aws_dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )
        Tags.of(self.dynamodb_table).add("Name", self.app_config["table_name"])

    def create_lambda_layers(self) -> None:
        """
        Create the Lambda layers that are necessary for the additional runtime
        dependencies of the Lambda Functions.
        """

        # Layer for "LambdaPowerTools" (for logging, traces, observability, etc)
        self.lambda_layer_powertools = aws_lambda.LayerVersion.from_layer_version_arn(
            self,
            "Layer-PowerTools",
            layer_version_arn=f"arn:aws:lambda:{self.region}:017000801446:layer:AWSLambdaPowertoolsPythonV2:71",
        )

        # Layer for "common" Python requirements (opencv-headless, etc)
        self.lambda_layer_common = aws_lambda.LayerVersion(
            self,
            "Layer-Common",
            code=aws_lambda.Code.from_asset("lambda-layers/common/modules"),
            compatible_runtimes=[
                aws_lambda.Runtime.PYTHON_3_11,
            ],
            description="Lambda Layer for Python with <common> library",
            removal_policy=RemovalPolicy.DESTROY,
            compatible_architectures=[aws_lambda.Architecture.X86_64],
        )

    def create_lambda_functions(self) -> None:
        """
        Create the Lambda Functions for the solution.
        """
        # Get relative path for folder that contains Lambda function source
        # ! Note--> we must obtain parent dirs to create path (that"s why there is "os.path.dirname()")
        PATH_TO_LAMBDA_FUNCTION_FOLDER = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
            "backend",
        )

        # Lambda Function for converting video to images
        self.lambda_sm_convert_video_to_images = aws_lambda.Function(
            self,
            "Lambda-SM-ConvertVideoToImages",
            runtime=aws_lambda.Runtime.PYTHON_3_11,
            handler="state_machine/state_machine_handler.lambda_handler",
            function_name=f"{self.main_resources_name}-convert-video-to-images",
            code=aws_lambda.Code.from_asset(PATH_TO_LAMBDA_FUNCTION_FOLDER),
            timeout=Duration.minutes(10),
            memory_size=1024,
            environment={
                "ENVIRONMENT": self.app_config["deployment_environment"],
                "LOG_LEVEL": self.app_config["log_level"],
                "S3_BUCKET_NAME": self.s3_bucket.bucket_name,
                "DYNAMODB_TABLE_NAME": self.dynamodb_table.table_name,
            },
            layers=[
                self.lambda_layer_powertools,
                self.lambda_layer_common,
            ],
        )

        # Lambda Function for processing images with Rekognition
        self.lambda_sm_process_images = aws_lambda.Function(
            self,
            "Lambda-SM-ProcessImages",
            runtime=aws_lambda.Runtime.PYTHON_3_11,
            handler="state_machine/state_machine_handler.lambda_handler",
            function_name=f"{self.main_resources_name}-process-images",
            code=aws_lambda.Code.from_asset(PATH_TO_LAMBDA_FUNCTION_FOLDER),
            timeout=Duration.minutes(1),
            memory_size=512,
            environment={
                "ENVIRONMENT": self.app_config["deployment_environment"],
                "LOG_LEVEL": self.app_config["log_level"],
                "S3_BUCKET_NAME": self.s3_bucket.bucket_name,
                "DYNAMODB_TABLE_NAME": self.dynamodb_table.table_name,
            },
            layers=[
                self.lambda_layer_powertools,
                self.lambda_layer_common,
            ],
        )

    def create_state_machine_tasks(self) -> None:
        """
        Method to create the tasks for the Step Function State Machine.
        """

        # TODO: create abstraction to reuse the definition of tasks

        self.task_convert_video_to_images = aws_sfn_tasks.LambdaInvoke(
            self,
            "Task-ConvertVideoToImages",
            state_name="Convert Video to Images",
            lambda_function=self.lambda_sm_convert_video_to_images,
            payload=aws_sfn.TaskInput.from_object(
                {
                    "event.$": "$",
                    "params": {
                        "class_name": "ConvertVideoToImages",
                        "method_name": "convert_video_to_images",
                    },
                }
            ),
            output_path="$.Payload",
        )

        self.task_process_images = aws_sfn_tasks.LambdaInvoke(
            self,
            "Task-ProcessImages",
            state_name="ProcessImages",
            lambda_function=self.lambda_sm_process_images,
            payload=aws_sfn.TaskInput.from_object(
                {
                    "event.$": "$",
                    "params": {
                        "class_name": "ProcessImages",
                        "method_name": "process_images",
                    },
                }
            ),
            output_path="$.Payload",
        )

        # Define Distributed Map for enabling huge processing of images
        self.task_map_distributed = aws_sfn.DistributedMap(
            self,
            "Task-MapDistributedState",
            state_name="Map Distributed",
            # Used to iterate over this specific object (JSON with a list inside)
            item_reader=aws_sfn.S3JsonItemReader(
                bucket=self.s3_bucket,  # TODO: when available in CDK, make it dynamic
                key="maps/00_distributed_map.json",  # TODO: when available in CDK, make it dynamic
            ),
            # Used to write outputs of the processing to an S3 object
            result_writer=aws_sfn.ResultWriter(
                bucket=self.s3_bucket,
                prefix="maps/output/",  # TODO: when available in CDK, make it dynamic
            ),
            max_concurrency=100,  # Default max is 10, can be updated to 1000
        )
        # Add the item processor for the Distributed Map State
        self.task_map_distributed.item_processor(self.task_process_images)

        # Pass States to simplify State Machine UI understanding
        self.task_pass_initialize = aws_sfn.Pass(
            self,
            "Task-Initialize",
            comment="Initialize",
            state_name="Initialize",
        )

        self.task_not_implemented = aws_sfn.Pass(
            self,
            "Task-NotImplemented",
            comment="Not implemented yet",
        )

        self.task_process_success = aws_sfn_tasks.LambdaInvoke(
            self,
            "Task-Success",
            state_name="Process Success",
            lambda_function=self.lambda_sm_convert_video_to_images,
            payload=aws_sfn.TaskInput.from_object(
                {
                    "event.$": "$",
                    "params": {
                        "class_name": "Success",
                        "method_name": "process_success",
                    },
                }
            ),
            output_path="$.Payload",
        )

        self.task_process_failure = aws_sfn_tasks.LambdaInvoke(
            self,
            "Task-Failure",
            state_name="Process Failure",
            lambda_function=self.lambda_sm_convert_video_to_images,
            payload=aws_sfn.TaskInput.from_object(
                {
                    "event.$": "$",
                    "params": {
                        "class_name": "Failure",
                        "method_name": "process_failure",
                    },
                }
            ),
            output_path="$.Payload",
        )

        self.task_success = aws_sfn.Succeed(
            self,
            id="Succeed",
            comment="Successful execution of State Machine",
        )

        self.task_failure = aws_sfn.Fail(
            self,
            id="Exception Handling Finished",
            comment="State Machine Exception or Failure",
        )

    def create_state_machine_definition(self) -> None:
        """
        Method to create the Step Function State Machine definition.
        """

        # State Machine main definition
        self.state_machine_definition = self.task_convert_video_to_images.next(
            self.task_map_distributed
        )
        self.task_map_distributed.next(self.task_process_success)
        self.task_process_success.next(self.task_success)

        # TODO: Add failure handling for the State Machine with "process_failure"
        # self.task_process_failure.next(self.task_failure)

    def create_state_machine(self) -> None:
        """
        Method to create the Step Function State Machine for processing the messages.
        """

        log_group_name = f"/aws/vendedlogs/states/{self.main_resources_name}"
        self.state_machine_log_group = aws_logs.LogGroup(
            self,
            "StateMachine-LogGroup",
            log_group_name=log_group_name,
            removal_policy=RemovalPolicy.DESTROY,
        )
        Tags.of(self.state_machine_log_group).add("Name", log_group_name)

        self.state_machine = aws_sfn.StateMachine(
            self,
            "StateMachine-PrimeVideoXRay",
            state_machine_name=f"{self.main_resources_name}-process-video",
            state_machine_type=aws_sfn.StateMachineType.STANDARD,
            definition_body=aws_sfn.DefinitionBody.from_chainable(
                self.state_machine_definition,
            ),
            logs=aws_sfn.LogOptions(
                destination=self.state_machine_log_group,
                include_execution_data=True,
                level=aws_sfn.LogLevel.ALL,
            ),
        )

    def create_event_bridge_rules(self):
        """
        Method to create the EventBridge rules to trigger the State Machine.
        """
        # DLQ for the input State Machine event
        dlq_input_to_state_machine = aws_sqs.Queue(
            self,
            "SQS-DLQ-InputToStateMachine",
            queue_name=f"{self.main_resources_name}-s3-event-to-state-machine-dlq",
            retention_period=Duration.days(7),
            visibility_timeout=Duration.minutes(30),
        )

        # Rule to trigger the State Machine when a new video is uploaded to the S3 bucket
        aws_events.Rule(
            self,
            "EventBridge-Rule-S3TriggerStateMachine",
            rule_name=f"{self.main_resources_name}-s3-rule-to-state-machine",
            event_pattern=aws_events.EventPattern(
                detail_type=["Object Created"],
                source=["aws.s3"],
                detail={
                    "bucket": {"name": [self.s3_bucket.bucket_name]},
                    "object": {"key": [{"wildcard": "videos/*.mp4"}]},
                },
            ),
            targets=[
                aws_targets.SfnStateMachine(
                    machine=self.state_machine,
                    dead_letter_queue=dlq_input_to_state_machine,
                ),
            ],
        )

    def configure_iam_permissions(self):
        """
        Method to configure additional IAM permissions for the resources.
        """
        # Grant permissions to the State Machine
        self.s3_bucket.grant_read_write(self.state_machine)

        # Grant permissions to the Lambda Functions
        self.s3_bucket.grant_read_write(self.lambda_sm_convert_video_to_images)
        self.dynamodb_table.grant_read_write_data(
            self.lambda_sm_convert_video_to_images
        )
        self.s3_bucket.grant_read_write(self.lambda_sm_process_images)
        self.dynamodb_table.grant_read_write_data(self.lambda_sm_process_images)
        self.lambda_sm_process_images.role.add_managed_policy(
            aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                "AmazonRekognitionFullAccess"
            ),
        )

    def generate_cloudformation_outputs(self) -> None:
        """
        Method to add the relevant CloudFormation outputs.
        """

        CfnOutput(
            self,
            "DeploymentEnvironment",
            value=self.app_config["deployment_environment"],
            description="Deployment environment",
        )
