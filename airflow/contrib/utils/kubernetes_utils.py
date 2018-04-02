import jinja2


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


def generate_yaml(kubernetes_job_yaml_dictionary, kubernetes_job_yaml_template=None):
    """
    Generate YAML string from a Kubernetes Job yaml template
    and provided values.

    If kubernetes_job_yaml is None, use a default yaml template.
    """
    if not kubernetes_job_yaml_template:
        kubernetes_job_yaml_template = YAML_TEMPLATE

    template = jinja2.Template(kubernetes_job_yaml_template)
    return template.render(kubernetes_job_yaml_dictionary)


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
