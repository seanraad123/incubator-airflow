from airflow.models import DAG, TaskInstance
from datetime import datetime, timedelta
from airflow.utils.kubernetes_utils import KubernetesContainerInformation
import airflow.contrib.operators.kubernetes_operator as kubernetes_operator
import mock
import unittest


class KubernetesJobOperatorTest(unittest.TestCase):

    ARGS = {
        'owner': 'jessica'
    }
    DAG = DAG(
        dag_id='DAG_ID',
        default_args=ARGS,
        schedule_interval="17 1 * * *",
        start_date=datetime(2019, 10, 1),
        catchup=False
    )
    TASK_ID = 'test-task-id'
    YAML = """
        apiVersion: batch/v1
        kind: Job
        metadata:
          name: {{ job_name }}
        spec:
          template:
            spec:
              containers:
              - name: test-container-name
                image: test-container-image-information
                command: ['test', 'command']
              restartPolicy: Never
          backoffLimit: 0
    """
    CONTAINER = KubernetesContainerInformation(name='test-container-name',
                                               image='test-container-image-information',
                                               command=['test', 'command'])

    SUCCESS_SIDE_EFFECTS = ['applied',
                            '1 Running / 0 Succeeded / 0 Failed',
                            '0 Running / 1 Succeeded / 0 Failed',
                            'cleaned up']

    FAILURE_SIDE_EFFECTS = ['applied',
                            '1 Running / 0 Succeeded / 0 Failed',
                            '0 Running / 0 Succeeded / 1 Failed',
                            'cleaned up']

    def setUp(self):
        kubernetes_operator.SLEEP_TIME_BETWEEN_POLLING = 1

    @mock.patch('subprocess.check_output')
    def kubernetes_job_operator_execution(self,
                                          success,
                                          kubernetes_job_operator_task,
                                          mock_check_output):
        if success:
            mock_check_output.side_effect = self.SUCCESS_SIDE_EFFECTS
        else:
            mock_check_output.side_effect = self.FAILURE_SIDE_EFFECTS

        task_instance = TaskInstance(task=kubernetes_job_operator_task,
                                     execution_date=datetime.utcnow() - timedelta(days=1))

        if success:
            task_instance.run()
            self.assertEquals('success', task_instance.current_state())
        else:
            # Failed pod will cause task_instance to fail
            with self.assertRaises(Exception):
                task_instance.run()
            self.assertEquals('failed', task_instance.current_state())

    def test_basic_execution_failure(self):
        task = kubernetes_operator.KubernetesJobOperator(kubernetes_job_name='test-basic',
                                                         kubernetes_job_yaml_dictionary={
                                                            'containers': [self.CONTAINER]
                                                         },
                                                         task_id=self.TASK_ID,
                                                         dag=self.DAG)
        self.kubernetes_job_operator_execution(success=False,
                                               kubernetes_job_operator_task=task)

    @mock.patch('subprocess.check_output')
    def test_basic_execution_success(self, mock_check_output):
        task = kubernetes_operator.KubernetesJobOperator(kubernetes_job_name='test-basic',
                                                         kubernetes_job_yaml_dictionary={
                                                            'containers': [self.CONTAINER]
                                                         },
                                                         task_id=self.TASK_ID,
                                                         dag=self.DAG)
        self.kubernetes_job_operator_execution(success=True,
                                               kubernetes_job_operator_task=task)

    @mock.patch('subprocess.check_output')
    def test_user_provided_yaml_execution_failure(self, mock_check_output):
        task = kubernetes_operator.KubernetesJobOperator(kubernetes_job_name='test-basic',
                                                         kubernetes_job_yaml_template=self.YAML,
                                                         task_id=self.TASK_ID,
                                                         dag=self.DAG)
        self.kubernetes_job_operator_execution(success=False,
                                               kubernetes_job_operator_task=task)

    @mock.patch('subprocess.check_output')
    def test_user_provided_yaml_execution_success(self, mock_check_output):
        task = kubernetes_operator.KubernetesJobOperator(kubernetes_job_name='test-basic',
                                                         kubernetes_job_yaml_template=self.YAML,
                                                         task_id=self.TASK_ID,
                                                         dag=self.DAG)
        self.kubernetes_job_operator_execution(success=True,
                                               kubernetes_job_operator_task=task)

if __name__ == '__main__':
    unittest.main()
