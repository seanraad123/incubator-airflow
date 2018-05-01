from airflow.models import BaseOperator
from airflow.version import version
from airflow.contrib.utils.kubernetes_utils import generate_yaml, KubernetesContainerInformation
import logging
import re
import subprocess
import tempfile
import time
import yaml
import uuid
import ast


class KubernetesJobOperator(BaseOperator):
    
    template_fields = ['extra_param_dict']

    """
    KubernetesJobOperator will:
    1. Create a job given a Kubernetes job yaml
    2. Poll for the job's success/failure
    3. a. If job succeeds, delete job (and related pods)
       b. If pod fails, raise Exception and do not delete.
          This will allow for easier debugging.
          A separate process should be run to clean old dead jobs.

    :param job_yaml_string: Kubernetes job yaml as a formatted string, the job name
        should be unique to avoid overwriting already running jobs
    :type job_yaml_string: string
    :param job_param_dict: Kubernetes job parameters as a dictionary, the job name
        should be unique to avoid overwriting already running jobs
    :type job_param_dict: dictionary
    :param sleep_seconds_between_polling: number of seconds to sleep between polling
        for job completion, defaults to 60
    :type sleep_seconds_between_polling: int
    :param clean_up_successful_jobs: Flag to indicate whether or not successful jobs
        and related pods should be deleted after completion. (Failed jobs and pods
        are currently never deleted, they will have to be deleted manually.)
    :type clean_up_successful_jobs: boolean
    :param do_xcom_push: return the stdout which also get set in xcom by airflow platform
    :type do_xcom_push: bool
    """
    def __init__(self,
                 job_yaml_string=None,
                 job_param_dict=None,
                 sleep_seconds_between_polling=60,
                 clean_up_successful_jobs=True,
                 extra_param_dict=None,
                 do_xcom_push=False,
                 *args,
                 **kwargs):
        super(KubernetesJobOperator, self).__init__(*args, **kwargs)
        self.job_yaml_string = job_yaml_string
        self.job_param_dict = job_param_dict or {}
        self.sleep_seconds_between_polling = sleep_seconds_between_polling
        self.clean_up_successful_jobs = clean_up_successful_jobs
        self.do_xcom_push = do_xcom_push
        self.extra_param_dict = extra_param_dict or {}
        self.extra_param_dict.setdefault('labels', {}).update(
            {'airflow-version': 'v' + version.replace('.', '-').replace('+', '-')})
        
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
        # create job_yaml_string from the parameter dictionary.
        # append extra parameters to commands inside the job
        if not self.job_yaml_string and self.job_param_dict and 'containers' in self.job_param_dict:
            logging.info("extra_param_dict is {}".format(self.extra_param_dict))
            container_list = self.job_param_dict['containers']
            for container in container_list:
                command_list = ast.literal_eval(container.command)
                for key, value in self.extra_param_dict.iteritems():
                    if isinstance(value, dict):
                        # we only append the first pair in the vlaue.
                        command = "--{}={}={}".format(key, value.items()[0][0], value.items()[0][1])
                        command_list.append(command)
                    else:
                        command = "--{}={}".format(key, value)
                        command_list.append(command)
                container.command = str(command_list)
            unique_job_name = '%s-%s' % (self.job_param_dict['job_name'], uuid.uuid4())
            self.job_param_dict.update({'job_name': unique_job_name})
            self.job_yaml_string = generate_yaml(self.job_param_dict)
            logging.info("job_yaml_string is {}".format(self.job_yaml_string))

        # if we don't a job yaml, make the job fail.
        if not self.job_yaml_string:
            raise Exception("No job yaml.")
        else:
            self.job_name = yaml.safe_load(self.job_yaml_string)['metadata']['name']

        with tempfile.NamedTemporaryFile(suffix='.yaml') as f:
            f.write(self.job_yaml_string)
            f.flush()
            result = subprocess.check_output(args=['kubectl', 'apply', '-f', f.name])
            logging.info(result)

        try:
            self.poll_job_completion()
            if self.clean_up_successful_jobs:
                self.clean_up()

            # returning output if do_xcom_push is set
            if self.do_xcom_push:
                job_description = subprocess.check_output(args=['kubectl', 'describe', 'job', self.job_name])
                matched = re.search(r'Created pod: (.+?)\n', job_description)
                pod = matched.group(1)
                output = subprocess.check_output(args=['kubectl', 'logs', pod])
                return output
        except Exception as e:
            raise e
