from airflow.providers.apache.hive.hooks.hive import HiveCliHook
from typing import List, Optional


class KyuubiCliHook(HiveCliHook):
    """
    Enhanced version of HiveCliHook that uses kyuubi-beeline instead of standard beeline.
    """
    def _prepare_cli_cmd(self) -> List[str]:
        """
        Override _prepare_cli_cmd to use kyuubi-beeline instead of beeline
        """
        conn = self.conn
        if conn.port:
            hive_cmd = ["kyuubi-beeline", "-u", f"jdbc:kyuubi://{conn.host}:{conn.port}/{conn.schema}"]
        else:
            hive_cmd = ["kyuubi-beeline", "-u", f"jdbc:kyuubi://{conn.host}/{conn.schema}"]

        if conn.login:
            hive_cmd.extend(["-n", conn.login])
        if conn.password:
            hive_cmd.extend(["-p", conn.password])

        return hive_cmd
