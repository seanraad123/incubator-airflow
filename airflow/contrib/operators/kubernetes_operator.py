from airflow.models import BaseOperator
import jinja2
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

# Amount of time to sleep between polling for the job status
SLEEP_TIME_BETWEEN_POLLING = 60


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
        result = subprocess.check_output(args=args, stdin=stdin)
        logging.info(result)

    def clean_up(self):
        """
        Deleting the job removes the job and all related pods.
        Cleans old yamls off Airflow worker.
        """
        self.run_subprocess(args=['kubectl', 'delete', 'job', self.unique_job_name])
        # TODO: unlink was throwing errors, will investigate
        self.run_subprocess(args=['rm', self.filename])

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
        running_job_count = 1
        while running_job_count > 0:
            job_description = subprocess.check_output(args=['kubectl', 'describe', 'job', self.unique_job_name])
            matched = re.search(r'(\d+) Running / \d+ Succeeded / (\d+) Failed', job_description)
            logging.info('Current status is: %s' % matched.group(0))

            running_job_count = int(matched.group(1))
            failed_job_count = int(matched.group(2))
            # If any Jobs fail, fail KubernetesJobOperator task
            if failed_job_count != 0:
                self.clean_up()
                raise Exception('%s has failed pods, failing task.' % self.unique_job_name)

            time.sleep(SLEEP_TIME_BETWEEN_POLLING)

    def write_yaml(self):
        """
        Write Kubernetes job yaml to the Airflow worker.
        """
        logging.info('Writing yaml file to worker: %s' % self.filename)
        template = jinja2.Template(YAML_TEMPLATE)
        yaml = template.render(job_name=self.unique_job_name, containers=self.kubernetes_container_information_list)
        with open(self.filename, 'w') as yaml_file:
            yaml_file.write(yaml)

    def execute(self, context):
        self.unique_job_name = '%s-%s' % (self.job_name, uuid.uuid4())
        self.filename = '%s.yaml' % self.unique_job_name

        self.write_yaml()

        self.run_subprocess(args=['kubectl', 'apply', '-f', '%s' % self.filename])

        self.poll_job_completion()

        self.clean_up()
