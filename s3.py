import os

import boto3
import polars as pl
import pyarrow.dataset as ds
import pyarrow.fs as pafs
import pyarrow.parquet as pq
from cloudpathlib import AnyPath, S3Client
from cloudpathlib.local import LocalS3Client

AWS_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID"
AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY"
AWS_PROFILE = "AWS_PROFILE"
AWS_ENDPOINT_URL = "AWS_ENDPOINT_URL"
AWS_DEFAULT_REGION = "AWS_DEFAULT_REGION"


class Storage:
    def __init__(
        self,
        profile_name: str | None = None,
        access_key: str | None = None,
        secret_key: str | None = None,
        endpoint_url: str | None = None,
        region_name: str = "us-east-1",
        verify_ssl: bool | str = True,
        local_s3: bool = False,
        local_s3_dir: str = "~",
    ):
        self.profile_name = profile_name
        self.access_key = access_key
        self.secret_key = secret_key
        self.endpoint_url = endpoint_url
        self.region_name = region_name
        self.verify_ssl = verify_ssl
        self.local_s3 = local_s3
        self.local_s3_dir = os.path.abspath(os.path.expanduser(local_s3_dir))

        if local_s3:
            self._init_local_s3()
        else:
            self._init_s3()

    def _init_s3(self):
        self._session = boto3.Session(
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region_name,
            profile_name=self.profile_name,
        )
        self._client = self._session.client(
            "s3", endpoint_url=self.endpoint_url, verify=self.verify_ssl
        )

        credentials = self._session.get_credentials()
        frozen_creds = credentials.get_frozen_credentials()
        access_key = frozen_creds.access_key
        secret_key = frozen_creds.secret_key

        self._s3_client = S3Client(
            boto3_session=self._session, endpoint_url=self.endpoint_url
        )
        self.s3_fs = pafs.S3FileSystem(
            access_key=access_key,
            secret_key=secret_key,
            endpoint_override=self.endpoint_url,
            region=self.region_name,
        )

    def _init_local_s3(self):
        self._session = None
        self._client = None
        self._s3_client = LocalS3Client(local_storage_dir=self.local_s3_dir)
        self.s3_fs = pafs.LocalFileSystem()

    @classmethod
    def from_env(cls, **kwargs):
        profile_name = os.environ.get(AWS_PROFILE)
        access_key = os.environ.get(AWS_ACCESS_KEY_ID)
        secret_key = os.environ.get(AWS_SECRET_ACCESS_KEY)
        endpoint_url = os.environ.get(AWS_ENDPOINT_URL)
        region_name = os.environ.get(AWS_DEFAULT_REGION, "us-east-1")

        return cls(
            profile_name=profile_name,
            access_key=access_key,
            secret_key=secret_key,
            endpoint_url=endpoint_url,
            region_name=region_name,
            **kwargs,
        )

    def is_s3_path(self, path: str | AnyPath):
        return str(path).startswith("s3://")

    def path(self, path: str | AnyPath):
        if not isinstance(path, str):
            return path
        elif self.is_s3_path(path):
            return self._s3_client.CloudPath(path)
        else:
            return AnyPath(path)

    def get_arrow_path(self, s3_uri: str | AnyPath):
        if not self.is_s3_path(s3_uri):
            raise ValueError(
                f"Invalid URI: '{s3_uri}'. Storage interface strictly requires 's3://' prefix "
                f"to ensure environment-agnostic path management."
            )
        raw_path = str(s3_uri).replace("s3://", "").lstrip("/")
        if self.local_s3:
            return os.path.join(self.local_s3_dir, raw_path)
        return raw_path

    def scan_parquet(self, uri: str | AnyPath, **kwargs):
        if self.is_s3_path(uri):
            target_path = self.get_arrow_path(uri)

            dataset = ds.dataset(
                target_path,
                filesystem=self.s3_fs,
                format="parquet",
                partitioning="hive",
            )

            return pl.scan_pyarrow_dataset(dataset, **kwargs)
        else:
            return pl.scan_parquet(uri, **kwargs)

    def read_parquet(self, uri: str | AnyPath):
        df = self.scan_parquet(uri=uri).collect()
        return df

    def write_parquet(
        self,
        df: pl.DataFrame,
        uri: str | AnyPath,
        partition_cols: list[str] | str | None = None,
        **kwargs,
    ):
        if not self.is_s3_path(uri):
            df.write_parquet(uri, **kwargs)
            return

        if isinstance(partition_cols, str):
            partition_cols = [partition_cols]

        target_path = self.get_arrow_path(uri)
        table = df.to_arrow()

        if partition_cols:
            ds.write_dataset(
                table,
                base_dir=target_path,
                basename_template="part-{i}.parquet",
                format="parquet",
                partitioning=partition_cols,
                partitioning_flavor="hive",
                filesystem=self.s3_fs,
                existing_data_behavior="overwrite_or_ignore",
                **kwargs,
            )
        else:
            if self.local_s3:
                os.makedirs(os.path.dirname(target_path), exist_ok=True)
            pq.write_table(table, target_path, filesystem=self.s3_fs, **kwargs)
