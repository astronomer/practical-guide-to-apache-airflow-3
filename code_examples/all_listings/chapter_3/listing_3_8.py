from airflow.sdk import Asset, AssetWatcher, dag
from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger

trigger = MessageQueueTrigger(
    queue="<your-queue-url>", aws_conn_id="my_aws_conn", waiter_delay=20
)
my_asset = Asset(
    "my_queue_asset", watchers=[AssetWatcher(name="my_queue_watcher", trigger=trigger)]
)


@dag(schedule=[my_asset])
def my_dag():
    pass
