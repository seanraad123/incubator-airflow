from airflow import configuration
from airflow.models import BaseOperator
from airflow.version import version as airflow_version
from airflow.contrib.utils.kubernetes_utils import dict_to_env, uniquify_job_name, deuniquify_job_name
from airflow.contrib.utils.parameters import enumerate_parameters
import logging
import re
import subprocess
import tempfile
import time
import yaml


class KubernetesJobOperator(BaseOperator):
    template_fields = ('service_account_secret_name', 'env')

    def __init__(self,
                 job_name,
                 service_account_secret_name,
                 container_specs,
                 env=None,
                 sleep_seconds_between_polling=60,
                 clean_up_successful_jobs=True,
                 do_xcom_push=False,  # TODO: [2018-05-09 dangermike] remove this once next_best is no longer using it
                 *args,
                 **kwargs):

        """
        KubernetesJobOperator will:
        1. Create a job given a Kubernetes job yaml
        2. Poll for the job's success/failure
        3. a. If job succeeds, delete job (and related pods)
           b. If pod fails, raise Exception and do not delete.
              This will allow for easier debugging.
              A separate process should be run to clean old dead jobs.

        :param job_name: Name of the Kubernetes job. Will be suffixed at runtime
        :type job_name: string
        :param service_account_secret_name: Secret to use
        :type service_account_secret_name: string
        :param container_specs: Specification for the containers to launch. Environment variables will be
            added automatically, as well as the volume of the service_account_secret
        :type container_specs: dictionary
        :param env: Optional additional environment variables to provide to each container
        :type env: dictionary
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
        super(KubernetesJobOperator, self).__init__(*args, **kwargs)
        self.job_name = job_name
        self.instance_names = []
        self.service_account_secret_name = service_account_secret_name
        self.container_specs = []
        for cs in container_specs:
            if hasattr(cs, 'to_dict'):
                self.container_specs.append(cs.to_dict())
            else:
                self.container_specs.append(cs)
        self.env = env or {}
        self.env['GOOGLE_APPLICATION_CREDENTIALS'] = '/%s/key.json' % service_account_secret_name
        self.env['AIRFLOW_VERSION'] = airflow_version

        # note: support for additional volumes will require an additional parameter

        self.sleep_seconds_between_polling = sleep_seconds_between_polling
        self.clean_up_successful_jobs = clean_up_successful_jobs
        self.do_xcom_push = do_xcom_push

    @staticmethod
    def from_job_yaml(job_yaml_string,
                      service_account_secret_name=None,
                      sleep_seconds_between_polling=60,
                      clean_up_successful_jobs=True,
                      *args,
                      **kwargs
                      ):
        """
        Make a Kubernetes job operator from YAML

        :param job_yaml_string: Kubernetes job yaml as a formatted string
        :type job_yaml_string: string
        :param service_account_secret_name: Secret to use. If not provided the function
            will look at the secret assigned to the first volume.
        :type service_account_secret_name: string
        :param sleep_seconds_between_polling: number of seconds to sleep between polling
            for job completion, defaults to 60
        :type sleep_seconds_between_polling: int
        :param clean_up_successful_jobs: Flag to indicate whether or not successful jobs
            and related pods should be deleted after completion. (Failed jobs and pods
            are currently never deleted, they will have to be deleted manually.)
        :type clean_up_successful_jobs: boolean
        :return: KubernetesJobOperator instance
        """
        job_data = yaml.safe_load(job_yaml_string)

        # clean up the name if this is a round-trip/reuse kind of scenario
        job_name = deuniquify_job_name(job_data['metadata']['name'])

        # this is potentially Bluecore specific: secrets are optional
        service_account_secret_name = service_account_secret_name or \
            job_data['spec']['template']['spec']['volumes'][0]['secret']['secretName']

        # this is gross and horrible, but we have to convert the list-of-name/value-dicts that
        # are environment variables in a container spec into a dictionary.
        container_specs = []
        for cs in job_data['spec']['template']['spec']['containers']:
            env = {}
            for e in cs['env']:
                env[e['name']] = e['value']
            cs['env'] = env
            container_specs.append(cs)

        return KubernetesJobOperator(
            job_name,
            service_account_secret_name=service_account_secret_name,
            container_specs=container_specs,
            sleep_seconds_between_polling=sleep_seconds_between_polling,
            clean_up_successful_jobs=clean_up_successful_jobs,
            *args,
            **kwargs
        )

    def clean_up(self, job_name):
        """
        Deletes the job. Deleting the job deletes are related pods.
        """
        result = subprocess.check_output(args=['kubectl', 'delete', 'job', job_name])
        logging.info(result)

    def on_kill(self):
        """
        Run clean up. Fail the task.
        """
        if 0 < len(self.instance_names):
            # should be exactly one, but let's not mess around
            for name in self.instance_names:
                self.clean_up(name)
            raise Exception('Job %s was killed.' % ','.join(self.instance_names))
        else:
            raise Exception('Job was killed')

    def poll_job_completion(self, job_name):
        """
        Polls for completion of the created job.
        Sleeps for sleep_seconds_between_polling between polling.
        Any failed pods will raise an error and fail the KubernetesJobOperator task.
        """
        logging.info('Polling for completion of job: %s' % job_name)
        running_job_count = 1
        while running_job_count > 0:
            time.sleep(self.sleep_seconds_between_polling)

            job_description = subprocess.check_output(args=['kubectl', 'describe', 'job', job_name])
            matched = re.search(r'(\d+) Running / \d+ Succeeded / (\d+) Failed', job_description)
            logging.info('Current status is: %s' % matched.group(0))

            running_job_count = int(matched.group(1))
            failed_job_count = int(matched.group(2))
            if failed_job_count != 0:
                raise Exception('%s has failed pods, failing task.' % job_name)

    def create_job_yaml(self, context):
        """
        create job_yaml_string from the operator's parameters

        :param context: Anything that implements xcom_pull
        :return: A tuple of the job's unique name and a string of YAML for the job
        """
        #
        unique_job_name = uniquify_job_name(self, context)

        # Copy the environment variables from the task and evaluate any XComs
        # Add in the AIRFLOW_xxx vars we need to support XComs from within the container
        instance_env = self.env.copy()
        instance_env['AIRFLOW_DAG_ID'] = self.dag_id
        instance_env['AIRFLOW_TASK_ID'] = self.task_id
        instance_env['AIRFLOW_EXECUTION_DATE'] = context['execution_date'].isoformat()
        instance_env['AIRFLOW_ENABLE_XCOM_PICKLING'] = configuration.getboolean('core', 'enable_xcom_pickling')
        instance_env['KUBERNETES_JOB_NAME'] = unique_job_name

        # Make a copy of all the containers.
        # Expand collections and apply XComs in args and/or command
        # Apply the instance environment variables
        # Add in the secrets volume
        instance_containers = [cs.copy() for cs in self.container_specs]
        for cs in instance_containers:
            if 'args' in cs:
                cs['args'] = list(enumerate_parameters(cs['args'], self))
            if 'command' in cs:
                cs['command'] = list(enumerate_parameters(cs['command'], self))
            # This assumes that env is a dictionary, which is possibly false
            cs['env'] = cs.get('env', {})
            cs['env'].update(instance_env)
            cs['env'] = dict_to_env(cs['env'], self)

            cs['volumeMounts'] = cs.get('volumeMounts', [])
            # do we already have the secret mount? if not, we should add it
            cs['volumeMounts'] = cs.get('volumeMounts', [])
            if 0 == len([x for x in cs['volumeMounts'] if self.service_account_secret_name == x.get('name')]):
                cs['volumeMounts'].append({
                    'name': self.service_account_secret_name,
                    'mountPath': "/%s" % self.service_account_secret_name,
                    'readOnly': True,
                })

        kub_job_dict = {
            'apiVersion': 'batch/v1',
            'kind': 'Job',
            'metadata': {'name': unique_job_name},
            'spec': {
                'template': {
                    'spec': {
                        'containers': instance_containers,
                        'volumes': [{
                            'name': self.service_account_secret_name,
                            'secret': {
                                'secretName': self.service_account_secret_name,
                            },
                        }],
                        'restartPolicy': 'Never'
                    }
                },
                'backoffLimit': 0,
            },
        }

        return unique_job_name, yaml.safe_dump(kub_job_dict)

    def execute(self, context):
        job_name, job_yaml_string = self.create_job_yaml(context)
        logging.info(job_yaml_string)
        self.instance_names.append(job_name)  # should happen once, but safety first!

        with tempfile.NamedTemporaryFile(suffix='.yaml') as f:
            f.write(job_yaml_string)
            f.flush()
            result = subprocess.check_output(args=['kubectl', 'apply', '-f', f.name])
            logging.info(result)

        try:
            self.poll_job_completion(job_name)

            job_description = subprocess.check_output(args=['kubectl', 'describe', 'job', job_name])
            matched = re.search(r'Created pod: (.+?)\n', job_description)
            pod = matched.group(1)
            output = subprocess.check_output(args=['kubectl', 'logs', pod])

            # log output
            logging.info('\n\n\nLOGGING OUTPUT FROM JOB: \n')
            logging.info(output)

            if self.clean_up_successful_jobs:
                self.clean_up(job_name)

            # returning output if do_xcom_push is set
            # TODO: [2018-05-09 dangermike] remove this once next_best is no longer using it
            if self.do_xcom_push:
                return output
        except Exception as e:
            raise e
