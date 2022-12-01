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