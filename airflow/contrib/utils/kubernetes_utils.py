import jinja2
import uuid

DEFAULT_YAML_TEMPLATE = """
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
        volumeMounts:
        - name: {{ service_account_secret_name }}
          mountPath: /{{ service_account_secret_name }}
          readOnly: true
        env:
        - name: GOOGLE_APPLICATION_CREDENTIALS
          value: /{{ service_account_secret_name }}/key.json
      volumes:
      - name: {{ service_account_secret_name }}
        secret:
          secretName: {{ service_account_secret_name }}
      restartPolicy: Never
  backoffLimit: 0
"""


def generate_yaml(kubernetes_job_yaml_dictionary):
    """
    Generate YAML string from a Kubernetes Job yaml template
    and provided values.

    """
    template = jinja2.Template(DEFAULT_YAML_TEMPLATE)
    return template.render(kubernetes_job_yaml_dictionary)


def generate_kubernetes_job_yaml(job_name,
                                 container_information_list,
                                 service_account_secret_name):
    """
    Creates a Kubernetes Job yaml from a Jinja template.
    Will ensure that the job name is unique,
    avoiding jobs overwriting each other.
    Kubernetes secret being used must have the service account
    keyfile json stored as key.json.
    """
    unique_job_name = '%s-%s' % (job_name, uuid.uuid4())
    job_yaml_dictionary = {
        'job_name': unique_job_name,
        'containers': container_information_list,
        'service_account_secret_name': service_account_secret_name
    }
    return generate_yaml(job_yaml_dictionary)


class KubernetesContainerInformation():
    """
    Information for an individual container,
    used to generate Kubernetes Job yamls.
    """
    def __init__(self,
                 name,
                 image,
                 command):
        self.name = name
        self.image = image
        self.command = command
