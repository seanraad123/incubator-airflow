from airflow.contrib.utils.kubernetes_utils import KubernetesContainerInformation
from airflow.models import DAG, TaskInstance
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_operator import KubernetesJobOperator
import mock
import unittest
import yaml


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
        task = KubernetesJobOperator.from_job_yaml(
            job_yaml_string=self.YAML,
            sleep_seconds_between_polling=0,
            task_id=self.TASK_ID,
            dag=self.DAG)

        task_instance = TaskInstance(task=task,
                                     execution_date=datetime.utcnow() - timedelta(days=1))

        return task_instance

    def test_create_from_yaml(self):
        task = KubernetesJobOperator.from_job_yaml(
            job_yaml_string=self.YAML,
            sleep_seconds_between_polling=0,
            task_id=self.TASK_ID,
            dag=self.DAG)

        job_name, job_yaml = task.create_job_yaml(dict(execution_date=datetime.utcnow()))
        self.assertTrue(job_name.startswith('test-job-name-'))
        parsed = yaml.safe_load(job_yaml)
        self.assertEqual(1, len(parsed['spec']['template']['spec']['containers']))

        c_zero = parsed['spec']['template']['spec']['containers'][0]
        self.assertEqual(c_zero['command'][0], 'test-container-command')
        self.assertNotIn('args', c_zero)
        self.assertEqual(c_zero['image'], 'test-container-image')
        self.assertEqual(c_zero['name'], 'test-container-name')

        env_dict = dict([(e['name'], e['value']) for e in c_zero['env']])
        self.assertEqual('dag-id', env_dict['AIRFLOW_DAG_ID'])
        self.assertEqual('/test-service-account-secret-name/key.json', env_dict['GOOGLE_APPLICATION_CREDENTIALS'])
        self.assertIsNotNone(env_dict['AIRFLOW_VERSION'])
        self.assertEqual(job_name, env_dict['KUBERNETES_JOB_NAME'])
        self.assertTrue(env_dict['AIRFLOW_ENABLE_XCOM_PICKLING'])
        self.assertIn('AIRFLOW_EXECUTION_DATE', env_dict)
        self.assertEqual('test-task-id', env_dict['AIRFLOW_TASK_ID'])

        self.assertEqual(1, len(c_zero['volumeMounts']))

    def test_init(self):
        task = KubernetesJobOperator(
            job_name='test-job-name',
            service_account_secret_name='test-service-account-secret-name',
            container_specs=[KubernetesContainerInformation(
                name='test-container-name',
                image='test-container-image',
                command='test-container-command',
            )],
            sleep_seconds_between_polling=0,
            task_id=self.TASK_ID,
            dag=self.DAG)

        job_name, job_yaml = task.create_job_yaml(dict(execution_date=datetime.utcnow()))
        self.assertTrue(job_name.startswith('test-job-name-'))
        parsed = yaml.safe_load(job_yaml)
        self.assertEqual(1, len(parsed['spec']['template']['spec']['containers']))

        c_zero = parsed['spec']['template']['spec']['containers'][0]
        self.assertEqual(c_zero['command'][0], 'test-container-command')
        self.assertNotIn('args', c_zero)
        self.assertEqual(c_zero['image'], 'test-container-image')
        self.assertEqual(c_zero['name'], 'test-container-name')

        env_dict = dict([(e['name'], e['value']) for e in c_zero['env']])
        self.assertEqual('dag-id', env_dict['AIRFLOW_DAG_ID'])
        self.assertEqual('/test-service-account-secret-name/key.json', env_dict['GOOGLE_APPLICATION_CREDENTIALS'])
        self.assertIsNotNone(env_dict['AIRFLOW_VERSION'])
        self.assertEqual(job_name, env_dict['KUBERNETES_JOB_NAME'])
        self.assertTrue(env_dict['AIRFLOW_ENABLE_XCOM_PICKLING'])
        self.assertIn('AIRFLOW_EXECUTION_DATE', env_dict)
        self.assertEqual('test-task-id', env_dict['AIRFLOW_TASK_ID'])

        self.assertEqual(1, len(c_zero['volumeMounts']))

    def test_init_with_container_dict(self):
        task = KubernetesJobOperator(
            job_name='test-job-name',
            service_account_secret_name='test-service-account-secret-name',
            container_specs=[dict(
                command='test-container-command',
                image='test-container-image',
                name='test-container-name',
            )],
            sleep_seconds_between_polling=0,
            task_id=self.TASK_ID,
            dag=self.DAG)

        job_name, job_yaml = task.create_job_yaml(dict(execution_date=datetime.utcnow()))
        self.assertTrue(job_name.startswith('test-job-name-'))
        parsed = yaml.safe_load(job_yaml)
        self.assertEqual(1, len(parsed['spec']['template']['spec']['containers']))

        c_zero = parsed['spec']['template']['spec']['containers'][0]
        self.assertEqual(c_zero['command'][0], 'test-container-command')
        self.assertNotIn('args', c_zero)
        self.assertEqual(c_zero['image'], 'test-container-image')
        self.assertEqual(c_zero['name'], 'test-container-name')

        env_dict = dict([(e['name'], e['value']) for e in c_zero['env']])
        self.assertEqual('dag-id', env_dict['AIRFLOW_DAG_ID'])
        self.assertEqual('/test-service-account-secret-name/key.json', env_dict['GOOGLE_APPLICATION_CREDENTIALS'])
        self.assertIsNotNone(env_dict['AIRFLOW_VERSION'])
        self.assertEqual(job_name, env_dict['KUBERNETES_JOB_NAME'])
        self.assertTrue(env_dict['AIRFLOW_ENABLE_XCOM_PICKLING'])
        self.assertIn('AIRFLOW_EXECUTION_DATE', env_dict)
        self.assertEqual('test-task-id', env_dict['AIRFLOW_TASK_ID'])

        self.assertEqual(1, len(c_zero['volumeMounts']))

    def test_init_with_container_dict_custom(self):
        task = KubernetesJobOperator(
            job_name='test-job-name',
            service_account_secret_name='test-service-account-secret-name',
            container_specs=[
                dict(
                    command='test-container-command',
                    image='test-container-image',
                    name='test-container-name',
                    env={'SPECIALS_VALUE': 'rudeboy'},
                    volumeMounts=[
                        dict(
                            name='megamount',
                            mountPath='/opt/mega',
                            readOnly=True,
                        )
                    ]
                )
            ],
            sleep_seconds_between_polling=0,
            task_id=self.TASK_ID,
            dag=self.DAG)

        job_name, job_yaml = task.create_job_yaml(dict(execution_date=datetime.utcnow()))
        self.assertTrue(job_name.startswith('test-job-name-'))
        parsed = yaml.safe_load(job_yaml)
        self.assertEqual(1, len(parsed['spec']['template']['spec']['containers']))

        c_zero = parsed['spec']['template']['spec']['containers'][0]
        self.assertEqual(c_zero['command'][0], 'test-container-command')
        self.assertNotIn('args', c_zero)
        self.assertEqual(c_zero['image'], 'test-container-image')
        self.assertEqual(c_zero['name'], 'test-container-name')

        env_dict = dict([(e['name'], e['value']) for e in c_zero['env']])
        self.assertEqual('dag-id', env_dict['AIRFLOW_DAG_ID'])
        self.assertEqual('/test-service-account-secret-name/key.json', env_dict['GOOGLE_APPLICATION_CREDENTIALS'])
        self.assertIsNotNone(env_dict['AIRFLOW_VERSION'])
        self.assertEqual(job_name, env_dict['KUBERNETES_JOB_NAME'])
        self.assertTrue(env_dict['AIRFLOW_ENABLE_XCOM_PICKLING'])
        self.assertIn('AIRFLOW_EXECUTION_DATE', env_dict)
        self.assertEqual('test-task-id', env_dict['AIRFLOW_TASK_ID'])
        self.assertEqual('rudeboy', env_dict['SPECIALS_VALUE'])

        self.assertEqual(2, len(c_zero['volumeMounts']))
        vol_dict = dict([(v['name'], v) for v in c_zero['volumeMounts']])
        self.assertIn('megamount', vol_dict)
        self.assertEqual('/opt/mega', vol_dict['megamount']['mountPath'])

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
