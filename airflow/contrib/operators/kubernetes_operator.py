from airflow.models import BaseOperator
from jinja2 import Template
from subprocess import PIPE
import logging
import re
import subprocess
import time
import uuid

YAML_TEMPLATE = """
apiVersion: batch/v1
kind: Job
metadata:
  name: {{ job_name }}
spec:
  template:
    spec:
      containers: {% for container in containers %}
      - name: {{ container.name }}
        image: {{ container.image }}
        command: {{ container.command }} {% endfor %}
      restartPolicy: Never
  backoffLimit: 0
"""


class KubernetesContainerInformation():
    def __init__(self,
                 name,
                 image,
                 command):
        self.name = name
        self.image = image
        self.command = command


class KubernetesJobOperator(BaseOperator):
    """
    The KubernetesJobOperator spins up, executes, and cleans up a Kubernetes job.
    It does so by following these steps:
        1. Create a Kubernetes Job yaml from job_name and kubernetes_container_information_list
        2. Instatiate the Job
        3. Poll and wait for Job completion
        4. Delete the Job (and Pod)

    # TODO: Expand yaml creation or allow people to pass custom Kubernetes Job yamls.
            That way we can specify things like: resources, env_vars, volumes, etc.

    :param job_name: meaningful name for the job
    :type job_name: string
    :kubernetes_container_information_list: list of containers required to execute the Job
    :type kubernetes_container_information_list: list of type KubernetesContainerInformation
    """
    def __init__(self,
                 job_name,
                 kubernetes_container_information_list,
                 *args,
                 **kwargs):
        super(KubernetesJobOperator, self).__init__(*args, **kwargs)
        self.job_name = job_name
        self.kubernetes_container_information_list = kubernetes_container_information_list

    def run_subprocess(self, args, stdin=None):
        try:
            result = subprocess.Popen(args=args, stdin=stdin, stdout=PIPE)
            logging.info(result.stdout.read())
        except Exception as e:
            raise e

    def clean_up(self):
        """
        Deleting the job removes the job and all related pods.
        Cleans old yamls off Airflow worker.
        """
        self.run_subprocess(args=['kubectl', 'delete', 'job', '%s' % self.unique_job_name])
        self.run_subprocess(args=['rm', '%s' % self.filename])

    def on_kill(self):
        """
        Run clean up.
        Raising an error will fail the KubernetesJobOperator task.
        """
        self.clean_up()
        raise Exception('Job %s was killed.' % self.unique_job_name)

    def poll_job_completion(self):
        """
        Polls for Job completion every 60 seconds.
        Any Failed pods will raise an error and fail the KubernetesJobOperator task.
        """
        logging.info('Polling for completion of job: %s' % self.unique_job_name)
        while True:
            job_description = subprocess.Popen(args=['kubectl', 'describe', 'job', self.unique_job_name], stdout=PIPE)
            status_description = subprocess.Popen(args=['grep', 'Pods Statuses:'], stdin=job_description.stdout, stdout=PIPE)
            status_line = status_description.stdout.read()
            logging.info('Current status is: %s' % status_line)

            matched = re.search(r'(\d+) Running / \d+ Succeeded / (\d+) Failed', status_line)
            # If any Jobs fail, fail KubernetesJobOperator task
            if int(matched.group(2)) != 0:
                self.clean_up()
                raise Exception('%s has failed pods, failing task.' % self.unique_job_name)

            if int(matched.group(1)) == 0:
                break
            else:
                time.sleep(60)

    def write_yaml(self):
        """
        Write Kubernetes job yaml to the Airflow worker.
        """
        logging.info('Writing yaml file to worker: %s' % self.filename)
        template = Template(YAML_TEMPLATE)
        yaml = template.render(job_name=self.unique_job_name, containers=self.kubernetes_container_information_list)
        with open(self.filename, 'a') as yaml_file:
            yaml_file.write(yaml)

    def execute(self, context):
        self.unique_job_name = '%s-%s' % (self.job_name, uuid.uuid4())
        self.filename = '%s.yaml' % self.unique_job_name

        self.write_yaml()

        self.run_subprocess(args=['kubectl', 'apply', '-f', '%s' % self.filename])

        self.poll_job_completion(self.unique_job_name)

        self.clean_up()
