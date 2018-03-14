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

    def on_kill(self):
        """
        Overrides method.
        Clean up jobs (-> pods) when a task instance is killed.
        Raising an error will fail the task.
        """
        self.run_subprocess(args=['kubectl', 'delete', 'job', '%s' % self.unique_job_name])
        raise Exception('Task %s was killed.' % self.unique_job_name)

    def run_subprocess(self, args, stdin=None):
        try:
            result = subprocess.Popen(args=args, stdin=stdin, stdout=PIPE)
            logging.info(result.stdout.read())
        except Exception as e:
            raise e

    def poll_job_completion(self, unique_job_name):
        """
        Polls for Job completion every 60 seconds.
        Fails the task if the Job has failed so it will be surfaced in Airflow.
        """
        while True:
            job_description = subprocess.Popen(args=['kubectl', 'describe', 'job', '%s' % unique_job_name], stdout=PIPE)
            status_description = subprocess.Popen(args=['grep', 'Pods Statuses:'], stdin=job_description.stdout, stdout=PIPE)
            status_line = status_description.stdout.read()
            logging.info('status_line is: %s' % status_line)

            matched = re.search(r'(\d+) Running / \d+ Succeeded / (\d+) Failed', status_line)
            # If any Jobs fail, fail KubernetesJobOperator task
            if int(matched.group(2)) != 0:
                raise Exception('JOB FAILED, FAIL TASK.')

            if int(matched.group(1)) == 0:
                break
            else:
                time.sleep(60)

    def write_yaml(self, unique_job_name, filename):
        """
        Write Kubernetes job yaml to the Airflow worker.
        """
        template = Template(YAML_TEMPLATE)
        yaml = template.render(job_name=unique_job_name, containers=self.kubernetes_container_information_list)
        with open(filename, 'a') as yaml_file:
            yaml_file.write(yaml)

    def execute(self, context):
        self.unique_job_name = '%s-%s' % (self.job_name, uuid.uuid4())
        self.filename = '%s.yaml' % self.unique_job_name

        logging.info('writing file %s' % self.filename)
        self.write_yaml(self.unique_job_name, self.filename)

        logging.info('running kubectl apply -f %s' % self.filename)
        self.run_subprocess(args=['kubectl', 'apply', '-f', '%s' % self.filename])

        logging.info('polling for completion of job %s' % self.unique_job_name)
        self.poll_job_completion(self.unique_job_name)

        logging.info('deleting job %s' % self.unique_job_name)
        self.run_subprocess(args=['kubectl', 'delete', 'job', '%s' % self.unique_job_name])

        logging.info('removing yaml from worker: %s' % self.filename)
        self.run_subprocess(args=['rm', '%s' % self.filename])
