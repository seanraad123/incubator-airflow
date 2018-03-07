from airflow.models import BaseOperator
import logging
import subprocess

"""
How is it going to work?

1. People create Docker image with code they want to run, store in registry
2. People create deployment .yaml
3. People create DAG with Kubernetes Operator (docker image, deployment yaml, command(s)) and schedule
    a. Create kubernetes deployment from yaml (must have unique name)
    b. Run command(s) on K8 deployment
    c. Tear down deployment
"""


class KubernetesOperator(BaseOperator):
    """
    1. Create Kubernetes pod
    2. Execute docker image code
    3. Tear down and clean up Kubernetes pod
    """
    def __init__(self,
                 deployment_yaml):
        """
        Will need to assert valid yaml.
        """
        assert deployment_yaml
        self.deployment_yaml = deployment_yaml
        # assert docker_image
        # self.docker_image = docker_image
        # assert commands
        # self.commands = commands

    def execute(self):
        # step one, create deploymen
        call_template = 'kubectl apply -f {}'
        parameterized_call = call_template % self.deployment_yaml
        try:
            subprocess.check_call(parameterized_call)
        except Exception as e:
            logging.error('Exception: %s. Failed to generate deployment from %s' % (e, self.deployment_yaml))
