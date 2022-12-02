"""
Created on Friday, 2022-12-02

Author: Wonkwon Lee, Young Il Kim

"""
from Config import *

class LockManager(object):
    """
    Lock manager is responsible for managing read lock, write lock, and lock queue.
    A lock manager stores variable id, current lock, and a list of lock in queue.
    
    Args:
        v_id (int): variable's id for a lock manager
    """
    def __init__(self, v_id: int):
        """
        Constructor for initializing a lock manager.
        """
        self.v_id = v_id
        self.lock = None
        self.lock_queue = []

    def clear(self):
        """
        Empty the current lock and the lock queue.
        """
        self.lock = None
        self.lock_queue = []

    def set_lock(self, lock):
        """
        Set the current lock.
        """
        self.lock = lock

    def process_lock(self, wlock):
        """
        Process a write lock request based on the current lock and the lock queue.

        Args:
            wlock (lock): write lock to be processed
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
        self.set_lock(wlock)

    def share_lock(self, t_id: int):
        """
        Share the current lock with a transaction.

        Args:
            t_id (int): Transaction id
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
        
        
        
###################################################################################################
######################################## TODO #####################################################
###################################################################################################
    def has_other_queued_write_lock(self, t_id=None):
        """
        Check if there's any other W-lock waiting in the queue.
        :param transaction_id: if provided, W-lock in the queue that is from
         this transaction will be ignored.
        :return: boolean value to indicate if existing queued W-lock
        """
        for queued_lock in self.lock_queue:
            if queued_lock.type == LockType.WRITE:
                if t_id and queued_lock.t_id == t_id:
                    continue
                return True
        return False

    def release_current_lock_by_transaction(self, t_id):
        """
        Release the current lock held by a transaction.
        :param transaction_id: the id of the transaction
        """
        if self.lock:
            # print("=========================== LM :: RCLBT ===========================")
            if self.lock.type == LockType.READ:
                # print("if self.lock.type == LockType.READ:")
                if t_id in self.lock.t_table:
                    # print("if t_id in self.lock.t_table:")
                    self.lock.t_table.remove(t_id)
                    # print("self.lock.t_table.remove(t_id)")
                # print(len(self.lock.t_table))
                if not len(self.lock.t_table):
                    # print("if not len(self.lock.t_table):")
                    self.lock = None
            else:
                # print("if self.lock.type == LockType.WRITE:")
                if self.lock.t_id == t_id:
                    self.lock = None