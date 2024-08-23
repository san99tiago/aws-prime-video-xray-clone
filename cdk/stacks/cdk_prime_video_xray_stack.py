# Built-in imports
import os

# External imports
from aws_cdk import (
    Duration,
    aws_autoscaling,
    aws_certificatemanager,
    aws_ec2,
    aws_dynamodb,
    aws_elasticloadbalancingv2,
    aws_events,
    aws_iam,
    aws_lambda,
    aws_logs,
    aws_route53,
    aws_route53_targets,
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

        # Methods for the backend resources
        self.create_s3_bucket()
        self.create_dynamodb_table()
        self.create_lambda_layers()
        self.create_lambda_functions()
        self.create_state_machine_tasks()
        self.create_state_machine_definition()
        self.create_state_machine()
        self.create_event_bridge_rules()

        # Methods for the frontend UI resources
        self.import_networking_resources()
        self.create_security_groups()
        self.create_roles()
        self.create_servers()

        # !Only enable this section if you have a custom domain for the application
        # You have to already own the Route53 hosted zone, otherwise, it will fail!
        if self.app_config["enable_custom_domain"]:
            self.create_alb()
            self.import_route_53_hosted_zone()
            self.configure_acm_certificate()
            self.configure_alb_listeners()
            self.configure_target_groups()
            self.configure_route_53_records()

        # Methods for the IAM permissions
        self.configure_iam_permissions()

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
            result_path="$.Payload",  # Add original payload to the result
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

        self.task_arrange_final_results = aws_sfn_tasks.LambdaInvoke(
            self,
            "Task-ArrangeFinalResults",
            state_name="ArrangeFinalResults",
            lambda_function=self.lambda_sm_convert_video_to_images,
            payload=aws_sfn.TaskInput.from_object(
                {
                    "event.$": "$",
                    "params": {
                        "class_name": "ArrangeFinalResults",
                        "method_name": "arrange_final_results",
                    },
                }
            ),
            output_path="$.Payload",
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
        self.task_map_distributed.next(self.task_arrange_final_results)
        self.task_arrange_final_results.next(self.task_process_success)
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

    def import_networking_resources(self):
        """
        Method to import existing networking resources for the deployment.
        """
        # If a specific VPC is needed, can be imported here later..
        self.vpc = aws_ec2.Vpc.from_lookup(
            self,
            "VPC",
            is_default=True,
        )

    def create_security_groups(self):
        """
        Method to create security groups for the UI.
        """
        # ALB Security Group on port 443 (HTTPS)
        self.sg_alb = aws_ec2.SecurityGroup(
            self,
            "SG-ALB",
            vpc=self.vpc,
            security_group_name=f"{self.app_config['short_name']}-alb-sg",
            description=f"Security group for {self.app_config['short_name']} UI ALB",
            allow_all_outbound=True,
        )
        self.sg_cidrs_list = self.app_config["sg_cidrs_list"]
        for cidr in self.sg_cidrs_list:
            self.sg_alb.add_ingress_rule(
                peer=aws_ec2.Peer.ipv4(cidr),
                connection=aws_ec2.Port.tcp(443),
                description=f"Allow HTTPS traffic to ALB for {cidr} CIDR",
            )

        # ASG Security Group
        self.sg_asg = aws_ec2.SecurityGroup(
            self,
            "SG",
            vpc=self.vpc,
            security_group_name=f"{self.app_config['short_name']}-asg-sg",
            description=f"Security group for {self.app_config['short_name']} UI ASG",
            allow_all_outbound=True,
        )

        # Allow inbound traffic from ALB to ASG on port 80 (HTTP)
        self.sg_alb.connections.allow_from(
            self.sg_asg,
            port_range=aws_ec2.Port.tcp(80),
            description="Allow HTTP traffic from ALB to ASG",
        )

    def create_roles(self):
        """
        Method to create roles for the infrastructure.
        """
        self.instance_role = aws_iam.Role(
            self,
            "InstanceRole",
            role_name=f"{self.app_config['short_name']}-instance-role",
            description=f"Role for {self.app_config['short_name']} servers",
            assumed_by=aws_iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                # aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                #     "EC2InstanceConnect"
                # ),
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonSSMManagedInstanceCore"
                ),
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "CloudWatchAgentServerPolicy"
                ),
            ],
        )

    def create_servers(self):
        """
        Method to create servers for the infrastructure.
        """
        self.asg = aws_autoscaling.AutoScalingGroup(
            self,
            "ASG",
            auto_scaling_group_name=self.app_config["short_name"],
            vpc=self.vpc,
            vpc_subnets=aws_ec2.SubnetSelection(
                subnet_type=aws_ec2.SubnetType.PUBLIC,
            ),
            instance_type=aws_ec2.InstanceType(self.app_config["instance_type"]),
            machine_image=aws_ec2.MachineImage.latest_amazon_linux2(),
            min_capacity=self.app_config["min_capacity"],
            max_capacity=self.app_config["max_capacity"],
            desired_capacity=self.app_config["desired_capacity"],
            security_group=self.sg_asg,
            role=self.instance_role,
        )

        # Add user data Environment Variables to the ASG/EC2 initialization
        self.asg.add_user_data(
            f"echo export S3_BUCKET_NAME={self.s3_bucket.bucket_name} >> /etc/profile"
        )
        self.asg.add_user_data(
            f"echo export DYNAMODB_TABLE_NAME={self.dynamodb_table.table_name} >> /etc/profile"
        )
        self.asg.add_user_data(
            f"echo export AWS_DEFAULT_REGION={self.region} >> /etc/profile"
        )

        PATH_TO_USER_DATA = os.path.join(
            os.path.dirname(__file__), "user_data_script.sh"
        )
        with open(PATH_TO_USER_DATA, "r") as file:
            user_data_script = file.read()
            self.asg.add_user_data(user_data_script)

    def configure_iam_permissions(self):
        """
        Method to configure additional IAM permissions for the resources.
        """
        # Grant permissions to the State Machine
        self.s3_bucket.grant_read_write(self.state_machine)

        # Grant permissions to the Lambda Functions inside the State Machine
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

        # Grant permissions to the ASG instances to access S3 and DynamoDB
        self.s3_bucket.grant_read_write(self.asg)
        self.dynamodb_table.grant_read_write_data(self.asg)

    def create_alb(self):
        """
        Method to create the Application Load Balancer for the UI.
        """
        self.alb = aws_elasticloadbalancingv2.ApplicationLoadBalancer(
            self,
            "ALB",
            vpc=self.vpc,
            internet_facing=True,
            load_balancer_name=self.app_config["short_name"],
            security_group=self.sg_alb,
        )

    def import_route_53_hosted_zone(self):
        """
        Method to import the Route 53 hosted zone for the application.
        """
        # IMPORTANT: The hosted zone must be already created in Route 53!
        self.hosted_zone_name = self.app_config["hosted_zone_name"]
        self.domain_name = f"prime.{self.hosted_zone_name}"
        self.hosted_zone = aws_route53.HostedZone.from_lookup(
            self,
            "HostedZone",
            domain_name=self.hosted_zone_name,
        )

    def configure_acm_certificate(self):
        """
        Method to configure the SSL certificate for the ALB.
        """
        self.certificate = aws_certificatemanager.Certificate(
            self,
            "Certificate",
            domain_name=self.domain_name,
            validation=aws_certificatemanager.CertificateValidation.from_dns(
                hosted_zone=self.hosted_zone,
            ),
        )

    def configure_alb_listeners(self):
        """
        Method to configure the ALB listeners for the UI.
        """
        self.https_listener = self.alb.add_listener(
            "ALB-HTTPS-Listener",
            open=True,
            port=443,
            protocol=aws_elasticloadbalancingv2.ApplicationProtocol.HTTPS,
            certificates=[self.certificate],
        )

    def configure_target_groups(self):
        """
        Method to configure the target groups for the ALB.
        """
        self.https_listener_target_group = self.https_listener.add_targets(
            "ALB-HTTPS-TargetGroup",
            port=80,  # Intentionally set to 80 for the ASG
            protocol=aws_elasticloadbalancingv2.ApplicationProtocol.HTTP,  # Intentionally set to HTTP for the ASG
            targets=[self.asg],
            health_check=aws_elasticloadbalancingv2.HealthCheck(
                path="/",
                protocol=aws_elasticloadbalancingv2.Protocol.HTTP,
                timeout=Duration.seconds(15),
                interval=Duration.minutes(30),
            ),
        )

    def configure_route_53_records(self):
        """
        Method to configure the Route 53 records for the ALB.
        """
        aws_route53.ARecord(
            self,
            "ALB-Record",
            zone=self.hosted_zone,
            target=aws_route53.RecordTarget.from_alias(
                aws_route53_targets.LoadBalancerTarget(self.alb)
            ),
            record_name=self.domain_name,
            comment=f"ALB DNS for {self.domain_name} for {self.app_config['short_name']} application",
        )

        # Outputs for the custom domain and ALB DNS
        CfnOutput(
            self,
            "APP-DNS",
            value=f"https://{self.domain_name}",
            description="Application custom DNS",
        )
        CfnOutput(
            self,
            "ALB-DNS",
            value=f"https://{self.alb.load_balancer_dns_name}",
            description="ALB DNS",
        )
