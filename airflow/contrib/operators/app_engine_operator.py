from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.exceptions import AirflowException
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import json
import time


class AppEngineOperator(BaseOperator):
    template_fields = ('command_params', 'job_id',)

    @apply_defaults
    def __init__(self,
                 task_id,
                 http_conn_id,
                 bucket,
                 command_params,
                 job_id,
                 google_cloud_conn_id='google_cloud_storage_default',
                 **kwargs):
        super(AppEngineOperator, self).__init__(task_id=task_id, **kwargs)
        self.http_conn_id = http_conn_id
        self.bucket = bucket
        command_params['job_id'] = job_id
        self.command_params = command_params
        self.job_id = job_id
        self.google_cloud_conn_id = google_cloud_conn_id

    def schedule_job(self):
        hook = HttpHook(
            method='POST',
            http_conn_id=self.http_conn_id)
        hook.run(
            endpoint='/api/airflow/schedule_job',
            headers={'content-type': 'application/json', 'Accept': 'text/plain'},
            data=json.dumps(self.command_params),
            extra_options=None)

    def poll_status_files(self):
        success_file_name = '%s/succeeded' % self.job_id
        fail_file_name = '%s/failed' % self.job_id
        i = 0
        while True:
            time.sleep(5 * 2**i)
            i += 1
            if check_gcs_file_exists(success_file_name, self.google_cloud_conn_id, self.bucket):
                return
            if check_gcs_file_exists(fail_file_name, self.google_cloud_conn_id, self.bucket):
                raise AirflowException('found failure file %s/%s' % (self.bucket, fail_file_name))

    def execute(self, context):
        # It seems that when an operator returns, it is considered successful,
        # and an operator fails if and only if it raises an AirflowException.
        # Good luck finding documentation saying that though.
        self.schedule_job()
        self.poll_status_files()


def check_gcs_file_exists(file_name, google_cloud_conn_id, bucket):
    hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=google_cloud_conn_id)
    return hook.exists(bucket, file_name)

# TODO Test schedule job works and fails
# TODO test find success file -> succeeds
# TODO test find fail file -> fail
# TODO test jinja render job id correctly
