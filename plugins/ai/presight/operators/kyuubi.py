from airflow.providers.apache.hive.operators.hive import HiveOperator
from ai.presight.hooks.kyuubi import KyuubiCliHook
import os
from functools import cached_property
from collections.abc import Sequence

class KyuubiOperator(HiveOperator):
    """
    Enhanced version of HiveOperator that adds Python language configuration
    when executing .py files.
    """
    template_ext: Sequence[str] = (
        ".hql",
        ".sql",
        ".pys",
    )
    def __init__(
            self,
            **kwargs):
        super().__init__(**kwargs)
        hql = kwargs['hql']
        self.is_py_hql = isinstance(hql, str) and hql.endswith('.pys')

    @cached_property
    def hook(self) -> KyuubiCliHook:
        """Get Hive cli hook."""
        return KyuubiCliHook(
            hive_cli_conn_id=self.hive_cli_conn_id,
            mapred_queue=self.mapred_queue,
            mapred_queue_priority=self.mapred_queue_priority,
            mapred_job_name=self.mapred_job_name,
            hive_cli_params=self.hive_cli_params,
            auth=self.auth,
            proxy_user=self.proxy_user,
        )
    
    def execute(self, context: dict) -> None:
        if self.is_py_hql:
            self.log.info("Prepending SET kyuubi.operation.language=python; to the HQL content.")
            self.hql = "SET kyuubi.operation.language=python;\n" + self.hql
        super().execute(context)
