from airflow.models import BaseOperator
import logging
import re
import subprocess
import tempfile
import time
import yaml


class KubernetesJobOperator(BaseOperator):
    """
    KubernetesJobOperator will:
    1. Create a job given a Kubernetes job yaml
    2. Poll for the job's success/failure
    3. Clean up the job after success/failure

    :param job_yaml_string: Kubernetes job yaml as a formatted string, the job name
        should be unique to avoid overwriting already running jobs
    :type job_yaml_string: string
    :param sleep_seconds_between_polling: number of seconds to sleep between polling
        for job completion, defaults to 60
    :type sleep_seconds_between_polling: int
    """
    def __init__(self,
                 job_yaml_string,
                 sleep_seconds_between_polling=60,
                 *args,
                 **kwargs):
        super(KubernetesJobOperator, self).__init__(*args, **kwargs)
        self.job_yaml_string = job_yaml_string
        self.sleep_seconds_between_polling = sleep_seconds_between_polling

    def clean_up(self):
        """
        Deletes the job. Deleting the job deletes are related pods.
        """
        result = subprocess.check_output(args=['kubectl', 'delete', 'job', self.job_name])
        logging.info(result)

    def on_kill(self):
        """
        Run clean up. Fail the task.
        """
        self.clean_up()
        raise Exception('Job %s was killed.' % self.job_name)

    def poll_job_completion(self):
        """
        Polls for completion of the created job.
        Sleeps for sleep_seconds_between_polling between polling.
        Any failed pods will raise an error and fail the KubernetesJobOperator task.
        """
        logging.info('Polling for completion of job: %s' % self.job_name)
        running_job_count = 1
        while running_job_count > 0:
            time.sleep(self.sleep_seconds_between_polling)

            job_description = subprocess.check_output(args=['kubectl', 'describe', 'job', self.job_name])
            matched = re.search(r'(\d+) Running / \d+ Succeeded / (\d+) Failed', job_description)
            logging.info('Current status is: %s' % matched.group(0))

            running_job_count = int(matched.group(1))
            failed_job_count = int(matched.group(2))
            if failed_job_count != 0:
                raise Exception('%s has failed pods, failing task.' % self.job_name)

    def execute(self, context):
        self.job_name = yaml.safe_load(self.job_yaml_string)['metadata']['name']

        with tempfile.NamedTemporaryFile(suffix='.yaml') as f:
            f.write(self.job_yaml_string)
            f.flush()
            result = subprocess.check_output(args=['kubectl', 'apply', '-f', f.name])
            logging.info(result)

        try:
            self.poll_job_completion()
            self.clean_up()
        except Exception as e:
            raise e
