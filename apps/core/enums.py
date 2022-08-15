from enum import Enum
from sre_constants import SUCCESS


# A pythonic way to get all values of Enum
class ExtendedEnum(Enum):

    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))


class WorkTypeEnum(str, Enum):
    STREAM = "STREAM"
    BATCH = "BATCH"


class CategoryEnum(str, Enum):
    DAG = "DAG"
    SQL = "SQL"
    SPARK_JAR = "SPARK_JAR"


class WorkStatusEnum(str, Enum):
    WAITTING = "WAITTING"
    EXECUTING = "EXECUTING"
    FINISHED = "FINISHED"


class WorkDependEnum(str, Enum):
    ON_NOTHING = "ON_NOTHING"
    ON_PRE_SUCCEEDED = "ON_PRE_SUCCEEDED"
    ON_PRE_FINISHED = "ON_PRE_FINISHED"


class TaskStatusEnum(str, Enum):
    WAITTING = "WAITTING"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TERMINATING = "TERMINATING"
    MANUAL_TERMINATED = "MANUAL_TERMINATED"
    SYSTEM_TERMINATED = "SYSTEM_TERMINATED"


class TaskNodeStatusEnum(str, Enum):
    SUCCESS = "SUCCESS"
    RUNNING = "RUNNING"
    FAILED = "FAILED"
    UPSTREAM_FAILED = "UPSTREAM_FAILED"
    SKIPPED = "SKIPPED"
    UP_FOR_RETRY = "UP_FOR_RETRY"
    UP_FOR_RESCHEDULE = "UP_FOR_RESCHEDULE"
    QUEUED = "QUEUED"
    NONE = "NONE"
    SCHEDULED = "SCHEDULED"
    DEFERRED = "DEFERRED"
    SENSING = "SENSING"
    REMOVED = "REMOVED"


class JavaVersionEnum(str, Enum):
    JAVA_SE_8 = "JAVA_SE_8"
    JAVA_SE_11 = "JAVA_SE_11"


class FrequencyEnum(str, Enum):
    MINUTELY = "MINUTELY"
    ONCE = "ONCE"
    HOURLY = "HOURLY"
    DAILY = "DAILY"
    WEEKLY = "WEEKLY"
    MONTHLY = "MONTHLY"
    YEARLY = "YEARLY"
    ALWAYS = "ALWAYS"


class DataSourceEnum(str, Enum):
    KAFKA = "KAFKA"
    POSTGRESQL = "POSTGRESQL"
    ASSET = "ASSET"
    FTP = "FTP"
    OSS = "OSS"


class ReadTypeEnum(str, Enum):
    FULL = "FULL"
    DELTA = "DELTA"


class WriteTypeEnum(str, Enum):
    OVERWRITE = "OVERWRITE"
    APPEND = "APPEND"


class SyncTypeEnum(str, Enum):
    ONLINE = "ONLINE"
    OFFLINE = "OFFLINE"


class ActionEnum(str, Enum):
    START_UP = "START_UP"
    SHUT_DOWN = "SHUT_DOWN"
    SHUT_DOWN_ALL = "SHUT_DOWN_ALL"
    SUBMIT = "SUBMIT"


class TaskTypeEnum(str, Enum):
    COMMON = "COMMON"
    IMPORT = "IMPORT"
    SQL = "SQL"
    DAG = "DAG"


class ConnectionStatusEnum(str, Enum):
    SUCCEED = "SUCCEED"
    FAILED = "FAILED"
    NONE = "NONE"


class ConnectionTypeEnum(str, Enum):
    KAFKA = "KAFKA"
    CHUHEDB = "CHUHEDB"
    POSTGRESQL = "POSTGRESQL"
    DRUID = "DRUID"


class OffsetTypeEnum(str, Enum):
    EARLIEST = "EARLIEST"
    LATEST = "LATEST"
    NONE = "NONE"


class TimeoutStrategyEnum(str, Enum):
    MARKED_AS_FAILED = "MARKED_AS_FAILED"
    SEND_MESSAGE = "SEND_MESSAGE"
    NONE = "NONE"


class NoticeStrategyEnum(str, Enum):
    ALL = "ALL"
    SUCCEED = "SUCCEED"
    FAIL = "FAIL"
    NONE = "NONE"


class SqlStateEnum(str, Enum):
    SUCCEED = "SUCCEED"
    FAIL = "FAIL"
    RUNNING = "RUNNING"


class SqlEnum(str, Enum):

    START_UP_SQL = "START_UP_SQL"
    # SHUT_DOWN_SQL = "SHUT_DOWN_SQL"
    SUBMIT_SQL = "SUBMIT_SQL"


class AnalysisState(str, Enum):

    TO_START = "TO_START"
    RUNNING = "STARTING"
    END = "END"


class SqlRunState(str, Enum):

    START = "START"
    FAIL = "FAIL"
    SUCCEED = "SUCCEED"


class ScheduleEngineType(str, Enum):

    AIRFLOW = "AIRFLOW"

