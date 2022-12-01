# from DataManager import DataManager 
import DataManager
from Config import *

class LockManager:
    def __init__(self, v_id):
        """
        Initialize a LockManager instance.
        :param v_id: variable's id for a lock manager
        """
        self.v_id = v_id
        self.current_lock = None
        self.lock_list = []  
        self.sharedReadLock = []

    def clear_lock(self):
        """
        Clear the current lock.
        """
        self.current_lock = None
        self.lock_list = []

    def set_lock(self, type: str):
        """
        Set a lock.
        :param ro: whether the lock is read-only
        """
        self.current_lock = type
        # self.lock_list.append(self.current_lock)

    def initialize_block_graph(sites):
        pass

    def releaseCurrentLock(self, t_id):
        pass

    def share_read_lock(self, t_id):
        """
        Share the R-lock with another transaction.
        :param transaction_id: the id of the transaction acquiring the R-lock
        """
        # if not self.current_lock.lock_type == LockType.R:
        #     raise RuntimeError("Attempt to share W-lock!")
        # self.current_lock.t_id_set.add(t_id)
        pass

    def has_write_lock(self, t_id):
        pass

    def share_lock(self, t_id):
        pass

    def add_queue(self, t_id):
        pass

