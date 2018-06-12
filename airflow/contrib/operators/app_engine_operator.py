from airflow import configuration
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.exceptions import AirflowException, AirflowTaskTimeout
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator, XCOM_RETURN_KEY
from airflow.utils.decorators import apply_defaults
from datetime import datetime
import json
import logging
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
        start_time = datetime.utcnow()
        i = 0
        # Bluecore App Engine backend instances timeout after an hour
        while (datetime.utcnow() - start_time).total_seconds() < 3600:
            time.sleep(min(60, 5 * 2**i))
            i += 1
            if check_gcs_file_exists(success_file_name, self.google_cloud_conn_id, self.bucket):
                return
            if check_gcs_file_exists(fail_file_name, self.google_cloud_conn_id, self.bucket):
                raise AirflowException('found failure file %s/%s' % (self.bucket, fail_file_name))

        raise AirflowTaskTimeout()

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


class AppEngineOperatorV2(BaseOperator):
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
        super(AppEngineOperatorV2, self).__init__(task_id=task_id, **kwargs)
        self.http_conn_id = http_conn_id
        self.bucket = bucket
        command_params['job_id'] = job_id
        self.command_params = command_params
        self.job_id = job_id
        self.google_cloud_conn_id = google_cloud_conn_id

    def schedule_job(self, context):
        hook = HttpHook(
            method='POST',
            http_conn_id=self.http_conn_id)
        hook.run(
            endpoint='/api/airflow/schedule_job',
            headers={
                'content-type': 'application/json',
                'Accept': 'text/plain',
                'X-Airflow-Dag-Id': self.dag_id,
                'X-Airflow-Task-Id': self.task_id,
                'X-Airflow-Execution-Date': context['execution_date'].isoformat(),
                'X-Airflow-Enable-Xcom-Pickling': str(configuration.getboolean('core', 'enable_xcom_pickling')),
                'X-Airflow-Mysql-Host': configuration.get('mysql', 'host'),
                'X-Airflow-Mysql-Db': configuration.get('mysql', 'db'),
                'X-Airflow-Mysql-User': configuration.get('mysql', 'username'),
                'X-Airflow-Mysql-Password': configuration.get('mysql', 'password'),
            },
            data=json.dumps(self.command_params),
            extra_options=None)

    def safe_xcom_pull(self, context, task_ids, dag_id=None, key=XCOM_RETURN_KEY, include_prior_dates=None):
        """
        Wraps the existing xcom_pull method, but returns None if there is any exception.
        :param context:
        :param task_ids:
        :param dag_id:
        :param key:
        :param include_prior_dates:
        :return:
        """
        try:
            return self.xcom_pull(
                context=context,
                task_ids=task_ids,
                dag_id=dag_id,
                key=key,
                include_prior_dates=include_prior_dates)
        except:
            return None

    def poll_status(self, context):
        start_time = datetime.utcnow()
        i = 0
        # Bluecore App Engine backend instances timeout after an hour
        while (datetime.utcnow() - start_time).total_seconds() < 3600:
            retval = self.xcom_pull(context=context, task_ids=self.task_id)
            if retval == '__EXCEPTION__':
                exc_message = self.safe_xcom_pull(
                    context=context,
                    task_ids=self.task_id,
                    key='__EXCEPTION_MESSAGE'
                )

                exc_type = self.safe_xcom_pull(
                    context=context,
                    task_ids=self.task_id,
                    key='__EXCEPTION_TYPE'
                )

                exc_callstack = self.safe_xcom_pull(
                    context=context,
                    task_ids=self.task_id,
                    key='__EXCEPTION_CALLSTACK'
                )

                logging.error(
                    "Found exception %s: %s" %
                    (exc_type or '<UNKNOWN>', exc_message or '<UNKNOWN>')
                )

                if exc_callstack:
                    logging.error(str(exc_callstack))

                raise AirflowException(exc_message)
            elif retval is not None:
                return

            # sleep for a while and try again
            time.sleep(min(60, 2**i))
            i += 1

        raise AirflowTaskTimeout()

    def execute(self, context):
        # It seems that when an operator returns, it is considered successful,
        # and an operator fails if and only if it raises an AirflowException.
        # Good luck finding documentation saying that though.
        self.schedule_job(context)
        self.poll_status(context)
