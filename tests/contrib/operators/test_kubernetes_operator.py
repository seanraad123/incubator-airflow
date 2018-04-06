from airflow.models import DAG, TaskInstance
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_operator import KubernetesJobOperator
import mock
import unittest


class KubernetesJobOperatorTest(unittest.TestCase):

    ARGS = {
        'owner': 'dag-owner'
    }
    DAG = DAG(
        dag_id='dag-id',
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
              name: test-job-name
            spec:
              template:
                spec:
                  containers:
                  - name: test-container-name
                    image: test-container-image
                    command: test-container-command
                    volumeMounts:
                    - name: test-service-account-secret-name
                      mountPath: /test-service-account-secret-name
                      readOnly: true
                    env:
                    - name: GOOGLE_APPLICATION_CREDENTIALS
                      value: /test-service-account-secret-name/key.json
                  volumes:
                  - name: test-service-account-secret-name
                    secret:
                      secretName: test-service-account-secret-name
                  restartPolicy: Never
              backoffLimit: 0
    """

    def get_kubernetes_job_operator_task_instance(self):
        task = KubernetesJobOperator(job_yaml_string=self.YAML,
                                     sleep_seconds_between_polling=0,
                                     task_id=self.TASK_ID,
                                     dag=self.DAG)

        task_instance = TaskInstance(task=task,
                                     execution_date=datetime.utcnow() - timedelta(days=1))

        return task_instance

    @mock.patch('subprocess.check_output')
    def test_task_failure(self, mock_check_output):
        mock_check_output.side_effect = ['applied',
                                         '1 Running / 0 Succeeded / 0 Failed',
                                         '0 Running / 1 Succeeded / 0 Failed',
                                         'cleaned up']

        task_instance = self.get_kubernetes_job_operator_task_instance()
        task_instance.run()
        self.assertEquals('success', task_instance.current_state())

    @mock.patch('subprocess.check_output')
    def test_task_success(self, mock_check_output):
        mock_check_output.side_effect = ['applied',
                                         '1 Running / 0 Succeeded / 0 Failed',
                                         '0 Running / 0 Succeeded / 1 Failed',
                                         'cleaned up']

        task_instance = self.get_kubernetes_job_operator_task_instance()
        with self.assertRaises(Exception):
            task_instance.run()
        self.assertEquals('failed', task_instance.current_state())

if __name__ == '__main__':
    unittest.main()
