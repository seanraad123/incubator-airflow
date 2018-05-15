from __future__ import absolute_import

from collections import namedtuple
from datetime import datetime
import unittest

from airflow.contrib.utils.kubernetes_utils import uniquify_job_name, deuniquify_job_name


class UniqueNameTest(unittest.TestCase):
    FakeTask = namedtuple('FakeTask', ['job_name', 'dag_id', 'task_id'])
    fake_task = FakeTask('fakejob', 'fakedag', 'faketask')
    fake_context = dict(execution_date=datetime(2018, 5, 14, 19, 39, 5, 841643))
    fake_run_timestamp = datetime(2018, 5, 14, 19, 41, 32, 398224)

    def test_uniquify(self):
        unique_job_name = uniquify_job_name(
            UniqueNameTest.fake_task,
            UniqueNameTest.fake_context,
            UniqueNameTest.fake_run_timestamp,
        )
        self.assertEqual('fakejob-f8bf14269d259a12-1526326892398', unique_job_name)

        unique_job_name2 = uniquify_job_name(
            UniqueNameTest.fake_task,
            UniqueNameTest.fake_context,
        )

        self.assertNotEqual(unique_job_name, unique_job_name2)

    def test_deuniquify(self):
        self.assertEqual('fakejob', deuniquify_job_name('fakejob-f8bf14269d259a12-1526326892398'))
        self.assertEqual('fake_job', deuniquify_job_name('fake_job-f8bf14269d259a12-1526326892398'))
        self.assertEqual('fake-job', deuniquify_job_name('fake-job-f8bf14269d259a12-1526326892398'))
        self.assertEqual('unperturbed', deuniquify_job_name('unperturbed'))
        self.assertEqual('unperturbed-with-dashes', deuniquify_job_name('unperturbed-with-dashes'))

    def test_roundtrip(self):
        for dag_id in ['dag', 'diggity-dag', 'dang ol\' dag']:
            for task_id in [
                'NemeanLion',
                'Lernaean-Hydra',
                'Ceryneian_Hind',
                'Erymanthian Boar',
                'Augean^stables',
                'Stymphalian\nbirds',
                'Cretan\tBull',
                'Mares.of.Diomedes',
                'Belt/of/Hippolyta',
                'Cattle\\of\\Geryon',
                'Golden$Apples$$of$$$the$$$$Hesperides',
            ]:
                for job_name in ['job', 'j.o.b.', 'j-OH-bee', 'janky job']:
                    task = UniqueNameTest.FakeTask(job_name, dag_id, task_id)
                    self.assertEqual(
                        job_name,
                        deuniquify_job_name(
                            uniquify_job_name(
                                task,
                                dict(execution_date=datetime.utcnow()),
                            )
                        )
                    )
