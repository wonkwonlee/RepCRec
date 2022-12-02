"""
Due on Saturday, 12/03/2022

Author: Wonkwon Lee, Young Il Kim

"""
from Config import *

class LockManager(object):
    """
    Lock manager is responsible for managing read lock, write lock, and lock queue.
    A lock manager stores variable id, current lock, and a list of lock in queue.
    
    Args:
        v_id (int): Variable ID
    """
    def __init__(self, v_id: int):
        """
        Constructor for initializing a lock manager.
        """
        self.v_id = v_id        # Variable ID
        self.lock = None        # Current lock
        self.lock_queue = []    # Lock queue

    def process_lock(self, wlock):
        """
        Process a write lock request based on the current lock and the lock queue.

        Args:
            wlock (lock): Write lock to be processed
        """
        if not self.lock:
            print("No lock exists")
            return
        elif not self.lock.type == LockType.READ:
            print("Current lock is not read lock.")
            return
        elif len(self.lock.t_table) != 1:
            print("There are multiple transactions holding the read lock.")
            return
        elif wlock.t_id not in self.lock.t_table:
            print("The transaction holding the read lock is not the same as the transaction holding the write lock.")
            return 
        self.lock = wlock
        # self.set_lock(wlock)

    def share_lock(self, t_id: int):
        """
        Share the current lock with a transaction.

        Args:
            t_id (int): Transaction ID
        """
        if not self.lock.type == LockType.READ:
            print("Current lock is not read lock.")
            return
        self.lock.t_table.add(t_id)

    def add_queue(self, queue):
        """
        Add a current lock to the lock queue.

        Args:
            queue (QLock): Lock to be added to the queue
        """
        for queued_lock in self.lock_queue:
            if queued_lock.t_id == queue.t_id:
                if queued_lock.type == queue.type or queue.type == LockType.READ:
                    return
        self.lock_queue.append(queue)

    def check_wlock(self, t_id=None):
        """
        Check if there is any write lock in the queue. 
        If transaction ID is provided, write lock from the same transaction will be ignored.

        Args:
            t_id (int, optional): Transaction ID. Defaults to None.

        Returns:
            bool: True if there is a write lock in the queue, False otherwise.
        """
        for l in self.lock_queue:
            if l.type == LockType.WRITE:
                if t_id and l.t_id == t_id:
                    continue
                return True

    def release_lock(self, t_id: int):
        """
        Release a lock from the lock queue. 

        Args:
            t_id (int): Transaction ID
        """
        if self.lock:
            if self.lock.type == LockType.WRITE:
                if self.lock.t_id == t_id:
                    self.lock = None       
            else:
                if t_id in self.lock.t_table:
                    self.lock.t_table.remove(t_id)
                if not len(self.lock.t_table):
                    self.lock = None