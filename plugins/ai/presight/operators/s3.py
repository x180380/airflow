from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.utils.context import Context
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from collections.abc import Sequence
from airflow.exceptions import AirflowException

class S3CopyFolderOperator(AwsBaseOperator[S3Hook]):
    """
    Copy all objects from a source S3 folder to a destination S3 folder.
    
    This operator copies all objects matching the source prefix to the destination,
    maintaining the folder structure. It supports copying within the same bucket
    or across different buckets using different connections.
    
    :param source_conn_id: The Airflow connection for the source S3 bucket
    :param dest_conn_id: The Airflow connection for the destination S3 bucket  
    :param source_prefix: Source folder prefix (without trailing slash)
    :param dest_prefix: Destination folder prefix (without trailing slash)
    :param source_bucket_name: Source bucket name (can be None if in connection)
    :param dest_bucket_name: Destination bucket name (can be None if in connection)
    :param file_suffix: File suffix/extension to filter files (e.g., '.parquet', '.json')
    """
    
    template_fields: Sequence[str] = aws_template_fields(
        "source_prefix",
        "dest_prefix", 
        "source_bucket_name",
        "dest_bucket_name",
        "file_suffix",
    )
    aws_hook_class = S3Hook

    def __init__(
        self,
        *,
        source_conn_id: str,
        dest_conn_id: str,
        source_prefix: str,
        dest_prefix: str,
        source_bucket_name: str | None = None,
        dest_bucket_name: str | None = None,
        file_suffix: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.source_conn_id = source_conn_id
        self.dest_conn_id = dest_conn_id
        self.source_prefix = source_prefix
        self.dest_prefix = dest_prefix
        self.source_bucket_name = source_bucket_name
        self.dest_bucket_name = dest_bucket_name
        self.file_suffix = file_suffix

    def execute(self, context: Context):
        # Create separate hooks for source and destination
        source_hook = S3Hook(aws_conn_id=self.source_conn_id)
        dest_hook = S3Hook(aws_conn_id=self.dest_conn_id)
        
        # Get bucket names from connections if not provided
        source_bucket = self.source_bucket_name
        if not source_bucket:
            source_conn = source_hook.get_connection(self.source_conn_id)
            if source_conn.schema:
                source_bucket = source_conn.schema
            else:
                raise AirflowException(
                    f"Source bucket name not provided and not found in connection {self.source_conn_id}"
                )
        
        dest_bucket = self.dest_bucket_name  
        if not dest_bucket:
            dest_conn = dest_hook.get_connection(self.dest_conn_id)
            if dest_conn.schema:
                dest_bucket = dest_conn.schema
            else:
                raise AirflowException(
                    f"Destination bucket name not provided and not found in connection {self.dest_conn_id}"
                )

        # Ensure prefixes end with / for proper folder handling
        source_prefix = self.source_prefix.rstrip('/') + '/'
        dest_prefix = self.dest_prefix.rstrip('/') + '/'
        
        self.log.info(
            "Starting copy from s3://%s/%s to s3://%s/%s",
            source_bucket, source_prefix, dest_bucket, dest_prefix
        )
        
        # List all objects in source folder
        source_objects = source_hook.list_keys(
            bucket_name=source_bucket,
            prefix=source_prefix
        )
        
        if not source_objects:
            self.log.warning("No objects found in source folder s3://%s/%s", source_bucket, source_prefix)
            return
                
        copied_count = 0
        for source_key in source_objects:
            if self.file_suffix and not source_key.endswith(self.file_suffix):
                continue
            # Calculate destination key by replacing source prefix with dest prefix
            if source_key.startswith(source_prefix):
                relative_path = source_key[len(source_prefix):]
                dest_key = dest_prefix + relative_path
            else:
                # Fallback if key doesn't start with prefix (edge case)
                dest_key = dest_prefix + source_key
            
            try:
                # Use the destination hook to copy the object
                dest_hook.copy_object(
                    source_bucket_key=source_key,
                    dest_bucket_key=dest_key,
                    source_bucket_name=source_bucket,
                    dest_bucket_name=dest_bucket
                )
                copied_count += 1
                self.log.info("Copied s3://%s/%s to s3://%s/%s", 
                             source_bucket, source_key, dest_bucket, dest_key)
                
            except Exception as e:
                self.log.error("Failed to copy s3://%s/%s to s3://%s/%s: %s",
                             source_bucket, source_key, dest_bucket, dest_key, str(e))
                raise AirflowException(
                    f"Failed to copy object {source_key}: {str(e)}"
                )
        
        self.log.info("Successfully copied %d objects from s3://%s/%s to s3://%s/%s",
                     copied_count, source_bucket, source_prefix, dest_bucket, dest_prefix)
