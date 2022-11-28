class Transaction(object):
    def __init__(self, id: str, ts: int, is_ro: bool):
        """
        Initialize a transaction object.

        Args:
            id (str): Transaction ID
            ts (int): Timestamp of when the transaction began
            is_ro (bool): Whether the transaction is read-only
        """
        self.id = id
        self.ts = ts
        self.is_ro = is_ro
        self.is_aborted = False     # will abort if deadlock
        self.visited_sites = []     # sites visited by this transaction
        
        
class ReadOnlyTransaction(Transaction):
    def __init__(self, id: str, ts: int):
        """
        Initialize a read-only transaction object.

        Args:
            id (str): Transaction ID
            ts (int): Timestamp of when the transaction began
        """
        super().__init__(id, ts, is_ro=True)
        self.read_values = {}       # variable ID: read value
        self.read_ts = {}           # variable ID: read timestamp
        self.snapshot = {}          # variable ID: snapshot value


class ReadWriteTransaction(Transaction):
    def __init__(self, id: str, ts: int):
        """
        Initialize a read-write transaction object.

        Args:
            id (str): Transaction ID
            ts (int): Timestamp of when the transaction began
        """
        super().__init__(id, ts, is_ro=False)
        self.locks = []             # list of Lock objects
        self.write_values = {}      # variable ID: write value
        self.write_ts = {}          # variable ID: write timestamp


class Operation(object):
    def __init__(self, op: str, t_id: str, v_id: int, val: int=None):
        """
        Initialize an operation object.

        Args:
            op (str): Operation type (read/write)
            t_id (str): Transaction ID
            v_id (int): Variable ID
            val (int): Value to write (if write operation)
        """
        self.op = op
        self.t_id = t_id
        self.v_id = v_id
        self.val = val
        
