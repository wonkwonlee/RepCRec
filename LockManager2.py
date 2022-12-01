from Config import *
class LockManager2:
    """Manages both current lock and queued locks of a certain variable."""

    def __init__(self, variable_id):
        """
        Initialize a LockManager instance.
        :param variable_id: variable's id for a lock manager
        """
        self.variable_id = variable_id
        self.current_lock = None
        self.queue = []  # list of QueuedLock

    def clear(self):
        """Clean up both current lock and lock queue."""
        self.current_lock = None
        self.queue = []

    def set_current_lock(self, lock):
        """
        Set a new lock as the current lock.
        :param lock: a ReadLock object or a WriteLock object
        """
        self.current_lock = lock

    def promote_current_lock(self, write_lock):
        """
        Promote the current lock from R-lock to W-lock for the same transaction.
        :param write_lock: the new WriteLock
        """
        if not self.current_lock:
            raise RuntimeError("No current lock!")
        if not self.current_lock.lock_type == LockType.READ:
            raise RuntimeError("Current lock is not R-lock!")
        if len(self.current_lock.transaction_id_set) != 1:
            raise RuntimeError("Other transaction sharing R-lock!")
        if write_lock.transaction_id not in \
                self.current_lock.transaction_id_set:
            raise RuntimeError("{} is not holding current R-lock!".format(
                write_lock.transaction_id))
        self.set_current_lock(write_lock)

    def share_read_lock(self, transaction_id):
        """
        Share the R-lock with another transaction.
        :param transaction_id: the id of the transaction acquiring the R-lock
        """
        if not self.current_lock.lock_type == LockType.READ:
            raise RuntimeError("Attempt to share W-lock!")
        self.current_lock.transaction_id_set.add(transaction_id)

    def add_queue(self, new_lock: QueuedLock):
        """
        Add a new QueuedLock into lock queue.
        :param new_lock: the new QueuedLock
        """
        for queued_lock in self.queue:
            if queued_lock.t_id == new_lock.t_id:
                # transaction holds the same type of lock or the new lock is
                # a R-lock when already had locks in queue
                if queued_lock.lock_type == new_lock.lock_type or \
                        new_lock.lock_type == LockType.READ:
                    return
        self.queue.append(new_lock)

    def has_other_queued_write_lock(self, t_id=None):
        """
        Check if there's any other W-lock waiting in the queue.
        :param transaction_id: if provided, W-lock in the queue that is from
         this transaction will be ignored.
        :return: boolean value to indicate if existing queued W-lock
        """
        for queued_lock in self.queue:
            if queued_lock.lock_type == LockType.WRITE:
                if t_id and queued_lock.t_id == t_id:
                    continue
                return True
        return False

    def release_current_lock_by_transaction(self, transaction_id):
        """
        Release the current lock held by a transaction.
        :param transaction_id: the id of the transaction
        """
        if self.current_lock:
            # print("=========================== LM :: RCLBT ===========================")
            if self.current_lock.lock_type == LockType.READ:
                # print("if self.current_lock.lock_type == LockType.READ:")
                # current lock is R-lock
                if transaction_id in self.current_lock.transaction_id_set:
                    # print("if transaction_id in self.current_lock.transaction_id_set:")
                    self.current_lock.transaction_id_set.remove(transaction_id)
                    # print("self.current_lock.transaction_id_set.remove(transaction_id)")
                # print(len(self.current_lock.transaction_id_set))
                if not len(self.current_lock.transaction_id_set):
                    # print("if not len(self.current_lock.transaction_id_set):")
                    # release when no other transaction holding R-lock
                    self.current_lock = None
            else:
                # print("if self.current_lock.lock_type == LockType.WRITE:")
                # current lock is W-lock
                if self.current_lock.t_id == transaction_id:
                    self.current_lock = None