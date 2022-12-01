from enum import Enum


class SiteStatus(Enum):
    UP = 1
    DOWN = 2


class TransactionType(Enum):
    RO = 1
    RW = 2


class TransactionStatus(Enum):
    ACTIVE = 1
    BLOCKED = 2
    COMMITED = 3
    ABORTED = 4


class LockType(Enum):
    READ = 1
    WRITE = 2


class NumType(Enum):
    EVEN = 0
    ODD = 1


class DataType(Enum):
    REPLICATED = 0
    NONREPLICATED = 1


class AbortType(Enum):
    DEADLOCK = 1
    SITE_FAILURE = 2
    NO_DATA_FOR_READ_ONLY = 3


class OperationType(Enum):
    BEGIN = 1
    BEGINRO = 2
    WRITE = 3
    READ = 4
    FAIL = 5
    RECOVER = 6
    DUMP = 7
    END = 8

class CommitValue:
    """Represents a committed value of a variable."""

    def __init__(self, value, ts):
        """
        Initialize a CommitValue instance.
        :param value: the committed value
        :param commit_ts: the timestamp of the commit
        """
        self.value = value
        self.cm_ts = ts

class Result:
    """Helper class that stores the result of a read or write action."""

    def __init__(self, success, value=None):
        """
        Initialize a Result instance.
        :param success: indicate if the result is successful or not
        :param value: result's value (optional)
        """
        self.success = success
        self.value = value

class TempValue:
    """Saves the temporary written value before the transaction commits."""

    def __init__(self, value, t_id):
        """
        Initialize a TempValue instance.
        :param value: temporary value written by a transaction holding W-lock
        :param transaction_id: the id of the transaction holding W-lock
        """
        self.value = value
        self.t_id = t_id

class ReadLock:
    """Represents a current Read lock."""

    def __init__(self, variable_id, transaction_id):
        """
        Initialize a ReadLock instance.
        :param variable_id: variable's id for the R-lock
        :param transaction_id: transaction's id for the R-lock
        """
        self.variable_id = variable_id
        # multiple transactions could share a R-lock
        self.transaction_id_set = {transaction_id}
        self.lock_type = LockType.R

    def __repr__(self):
        """Custom print for debugging purpose."""
        return "({}, {}, {})".format(
            self.transaction_id_set, self.variable_id, self.lock_type)


class WriteLock:
    """Represents a current Write lock."""

    def __init__(self, variable_id, transaction_id):
        """
        Initialize a WriteLock instance.
        :param variable_id: variable's id for the W-lock
        :param transaction_id: transaction's id for the W-lock
        """
        self.variable_id = variable_id
        self.transaction_id = transaction_id
        self.lock_type = LockType.W

    def __repr__(self):
        """Custom print for debugging purpose."""
        return "({}, {}, {})".format(
            self.transaction_id, self.variable_id, self.lock_type)


class QueuedLock:
    """Represents a lock in queue."""

    def __init__(self, variable_id, transaction_id, lock_type: LockType):
        """
        Initialize a QueuedLock instance.
        :param variable_id: variable's id for the queued lock
        :param transaction_id: transaction's id for the queued lock
        :param lock_type: either R or W type
        """
        self.variable_id = variable_id
        self.transaction_id = transaction_id
        self.lock_type = lock_type  # Q-lock could be either read or write

    def __repr__(self):
        """Custom print for debugging purpose."""
        return "({}, {}, {})".format(
            self.transaction_id, self.variable_id, self.lock_type)

class Lock:
    def __init__(self, tid: str, vid: str, lock_type: LockType) -> None:
        self.tid = tid  # transaction id
        self.vid = vid  # variable id
        self.lock_type = lock_type  # either R or W