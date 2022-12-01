from enum import Enum

class Transaction:
    """
    Transaction object that stores the transaction id, timestamp, and a flag to indicate read-only.

    Args:
        id (str): Transaction ID
        ts (int): Timestamp of when the transaction began
        is_ro (bool): Whether the transaction is read-only
    """
    def __init__(self, id: str, ts: int, is_ro: bool):
        """Constructor to initialize a transaction object."""
        self.id = id                # Transaction ID
        self.ts = ts                # Timestamp of when the transaction began
        self.is_ro = is_ro          # Flag to indicate whether the transaction is read-only
        self.is_aborted = False     # will abort if deadlock
        self.visited_sites = []     # sites visited by this transaction
        
class Operation(object):
    """
    Operation object that stores the operation type, transaction id, variable id, and value.

    Args:
        op (str): Operation type (R/W)
        t_id (str): Transaction ID
        v_id (int): Variable ID
        val (int): Value to write (if write operation)
    """
    def __init__(self, op: str, t_id: str, v_id: int, val: int=None):
        """Constructor to initialize an operation object."""
        self.op = op
        self.t_id = t_id
        self.v_id = v_id
        self.val = val

class TransactionType(Enum):
    """Enum for transaction type."""
    RO = 1
    RW = 2

class TransactionStatus(Enum):
    """Enum for transaction status."""
    ACTIVE = 1
    BLOCKED = 2
    COMMITED = 3
    ABORTED = 4

class LockType(Enum):
    """Enum for lock type."""
    READ = 1
    WRITE = 2

class DataType(Enum):
    """Enum for replicated data type."""
    REPLICATED = 0
    NONREPLICATED = 1

class AbortType(Enum):
    """Enum for abort type."""
    DEADLOCK = 1
    SITE_FAILURE = 2
    NO_DATA_FOR_READ_ONLY = 3

class OperationType(Enum):
    """Enum for operation type."""
    BEGIN = 1
    BEGINRO = 2
    WRITE = 3
    READ = 4
    FAIL = 5
    RECOVER = 6
    DUMP = 7
    END = 8

class Commit(object):
    """
    Commit object that stores the value and timestamp of the commit operation.
    
    Args:
        val (int): Value to commit
        ts (int): Timestamp of when the commit operation was executed
    """
    def __init__(self, val, ts):
        """Constructor to initialize a commit object."""
        self.val = val
        self.ts = ts

class Output(object):
    """
    Output object that stores the success flag and value of the operation.
    
    Args:
        success (bool): Flag to indicate whether the operation was successful
        val (int): Value of the operation (if write operation)
    """
    def __init__(self, success, val=None):
        """Constructor to initialize an output object."""
        self.success = success
        self.val = val

class Temp(object):
    """
    Temp object that stores the temporary value and transaction id of the write operation.
    
    Args:
        val (int): Temporary value written by a transaction holding the write lock
        t_id (str): Transaction ID of the transaction holding the write lock
    """
    def __init__(self, val, t_id):
        """Constructor to initialize a temp object."""
        self.val = val
        self.t_id = t_id

class ReadLock:
    """Represents a current Read lock."""

    def __init__(self, v_id: int, t_id: int):
        """
        Initialize a ReadLock instance.
        :param variable_id: variable's id for the R-lock
        :param transaction_id: transaction's id for the R-lock
        """
        self.v_id = v_id
        # multiple transactions could share a R-lock
        self.transaction_id_set = {t_id}
        self.lock_type = LockType.READ

    def __repr__(self):
        """Custom print for debugging purpose."""
        return "({}, {}, {})".format(
            self.transaction_id_set, self.v_id, self.lock_type)


class WriteLock:
    """Represents a current Write lock."""

    def __init__(self, v_id: int, t_id: int):
        """
        Initialize a WriteLock instance.
        :param variable_id: variable's id for the W-lock
        :param transaction_id: transaction's id for the W-lock
        """
        self.v_id = v_id
        self.t_id = t_id
        self.lock_type = LockType.WRITE

    # def __repr__(self):
    #     """Custom print for debugging purpose."""
    #     return "({}, {}, {})".format(self.t_id, self.v_id, self.lock_type)


class QueuedLock:
    """Represents a lock in queue."""

    def __init__(self, v_id: int, t_id: int, lock_type: LockType):
        """
        Initialize a QueuedLock instance.
        :param variable_id: variable's id for the queued lock
        :param transaction_id: transaction's id for the queued lock
        :param lock_type: either R or W type
        """
        self.v_id = v_id
        self.t_id = t_id
        self.lock_type = lock_type  # Q-lock could be either read or write

    def __repr__(self):
        """Custom print for debugging purpose."""
        return "({}, {}, {})".format(self.t_id, self.v_id, self.lock_type)

class Lock:
    def __init__(self, tid: str, vid: str, lock_type: LockType) -> None:
        self.tid = tid  # transaction id
        self.vid = vid  # variable id
        self.lock_type = lock_type  # either R or W
        
class Variable(object):
    def __init__(self, v_id: int, init_val, replicated: bool):
        self.v_id = v_id                # Variable ID
        self.val_list = [init_val]      # List of stored committed values
        self.readable = True            # Flag to indicate whether the variable is readable
        self.replicated = replicated    # Flag to indicate whether the variable is replicated 
        self.fail = False               # Flag to indicate whether the variable is failed
        self.temp_value = None          # Temporary value written by a transaction holding W-lock

    # def get_last_committed_value(self):
    #     """
    #     :return: the latest committed value
    #     """
    #     return self.commits[0].val

    # def get_temp_value(self):
    #     """
    #     :return: the temporary value written by a transaction holding a W-lock
    #     """
    #     if not self.temp_value:
    #         raise RuntimeError("No temp value!")
    #     return self.temp_value.val

    def add_commit_value(self, commit_value):
        """
        Insert a CommitValue object to the front of the committed value list.
        :param commit_value: a CommitValue object
        """
        self.val_list.insert(0, commit_value)