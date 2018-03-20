from airflow.models import BaseOperator
from airflow.utils.kubernetes_utils import generate_yaml
import logging
import re
import subprocess
import tempfile
import time
import uuid

# Amount of time to sleep between polling for the job status
SLEEP_TIME_BETWEEN_POLLING = 60


class KubernetesJobOperator(BaseOperator):
    """
    The KubernetesJobOperator spins up, executes, and cleans up a Kubernetes job.
    It does so by following these steps:
        1. Creates a Kubernetes Job yaml from a Job yaml template and dictionary of values
        2. Instatiates the Job
        3. Polls and wait for Job completion
        4. Deletes the Job (and created Pods)

    :param kubernetes_job_name: name of Kubernetes Job to run
    :type kubernetes_job_name: string
    :param kubernetes_job_yaml_dictionary: key/value pairs to fill the Job yaml
    :type kubernetes_job_yaml_dictionary: dictionary
    :kubernetes_job_yaml_template: string representation of a Job yaml,
        if None will use default
    :type kubernetes_job_yaml_template: string
    """
    def __init__(self,
                 kubernetes_job_name,
                 kubernetes_job_yaml_dictionary=None,
                 kubernetes_job_yaml_template=None,
                 *args,
                 **kwargs):
        super(KubernetesJobOperator, self).__init__(*args, **kwargs)
        self.kubernetes_job_name = kubernetes_job_name
        self.kubernetes_job_yaml_dictionary = kubernetes_job_yaml_dictionary or {}
        self.kubernetes_job_yaml_template = kubernetes_job_yaml_template

    def clean_up(self, error_message=None):
        """
        Deleting the job removes the job and all related pods.
        Iff error_message, raise Exception.
        """
        result = subprocess.check_output(args=['kubectl', 'delete', 'job', self.unique_job_name])
        logging.info(result)
        if error_message:
            raise Exception(error_message)

    def on_kill(self):
        """
        Run clean up.
        Raising an error will fail the KubernetesJobOperator task.
        """
        self.clean_up(error_message='Job %s was killed.' % self.unique_job_name)

    def poll_job_completion(self):
        """
        Polls for Job completion every 60 seconds.
        Any Failed pods will raise an error and fail the KubernetesJobOperator task.
        """
        logging.info('Polling for completion of job: %s' % self.unique_job_name)
        running_job_count = 1
        while running_job_count > 0:
            job_description = subprocess.check_output(args=['kubectl', 'describe', 'job', self.unique_job_name])
            matched = re.search(r'(\d+) Running / \d+ Succeeded / (\d+) Failed', job_description)
            logging.info('Current status is: %s' % matched.group(0))

            running_job_count = int(matched.group(1))
            failed_job_count = int(matched.group(2))
            # If any Jobs fail, fail KubernetesJobOperator task
            if failed_job_count != 0:
                return '%s has failed pods, failing task.' % self.unique_job_name

            time.sleep(SLEEP_TIME_BETWEEN_POLLING)

    def execute(self, context):
        self.unique_job_name = '%s-%s' % (self.kubernetes_job_name, uuid.uuid4())
        self.kubernetes_job_yaml_dictionary['job_name'] = self.unique_job_name

        yaml = generate_yaml(self.kubernetes_job_yaml_dictionary,
                             self.kubernetes_job_yaml_template)

        with tempfile.NamedTemporaryFile(suffix='.yaml') as f:
            f.write(yaml)
            f.flush()
            result = subprocess.check_output(args=['kubectl', 'apply', '-f', f.name])
            logging.info(result)

        message = self.poll_job_completion()
        self.clean_up(error_message=message)
