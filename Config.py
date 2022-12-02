"""
Created on Friday, 2022-12-02

Author: Wonkwon Lee, Young Il Kim

"""

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
    Output object that stores flag to indicate operation succeed and value of the operation.
    
    Args:
        succeed (bool): Flag to indicate whether the operation was successful
        val (int): Value of the operation (if write operation)
    """
    def __init__(self, succeed, val=None):
        """Constructor to initialize an output object."""
        self.succeed = succeed
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

class RLock(object):
    """
    Read lock is a shared lock, so multiple transactions can share the same read lock.
    
    Args:
        t_id (int): Transaction ID
        v_id (int): Variable ID
    """
    def __init__(self, t_id: int, v_id: int):
        """Constructor to initialize a read lock object."""
        self.transaction_id_set = {t_id}
        self.v_id = v_id
        self.lock_type = LockType.READ

class WLock:
    """
    Write lock is an exclusive lock, so only one transaction can hold the write lock.
    
    Args:
        t_id (int): Transaction ID
        v_id (int): Variable ID
    """

    def __init__(self, t_id: int, v_id: int):
        """Constructor to initialize a write lock object."""
        self.t_id = t_id
        self.v_id = v_id
        self.lock_type = LockType.WRITE

class QLock:
    """
    Waiting lock is a lock that is waiting to be granted.
    
    Args:
        t_id (int): Transaction ID
        v_id (int): Variable ID
        lock (LockType): Lock type (READ/WRITE)
    """
    def __init__(self, t_id: int, v_id: int, lock_type: LockType):
        """Constructor to initialize a waiting lock object."""
        self.t_id = t_id
        self.v_id = v_id
        self.lock_type = lock_type 
        
        
class Variable(object):
    """
    Variable object that stores the variable id, list of committed values, and flags to indicate readable and replicated
    
    Args:
        v_id (int): Variable ID
        val (int): Initial value of the variable
        replicated (bool): Flag to indicate whether the variable is replicated
    """
    def __init__(self, v_id: int, val: int, replicated: bool):
        """Constructor to initialize a variable object."""
        self.v_id = v_id                # Variable ID
        self.val_list = [val]           # List of stored committed values
        self.readable = True            # Flag to indicate whether the variable is readable
        self.replicated = replicated    # Flag to indicate whether the variable is replicated 
        self.fail = False               # Flag to indicate whether the variable is failed
        self.temp_value = None          # Temporary value written by a transaction holding W-lock
        
    def update(self, val):
        """
        Update the variable with a new committed value.
        
        Args:
            val (int): New committed value
        """
        self.val_list.insert(0, val)