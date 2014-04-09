"""DataWarehouseProducerFunction"""
from perrylib.common.environ_helper import git_sha
from datetime import datetime
import socket

DATETIME_FORMAT_ENCODE = 'datetime(%Y-%m-%d %H:%M:%S UTC)'
MOCK_QUEUE_TYPE_DATA_WAREHOUSE = 'warehouse_queue'


def message_builder(app_id, app_version, producer):
    """Returns a function to build a payload for data_warehouse producers.
    Args:
      app_id: Id of the application
      app_version: version of the application
    Returns:
      dict -> {timestamp, app_id, app_version, host, datawarehouse_lib_version, data}
    """
    sha = git_sha()

    def message_payload(payload):
        """Generate message payload"""
        producer.publish({
            'timestamp': datetime.utcnow().strftime(DATETIME_FORMAT_ENCODE),
            'app_id': app_id,
            'app_version': app_version,
            'host': socket.gethostname(),
            'datawarehouse_lib_version': sha,
            'data': payload})
    return message_payload
