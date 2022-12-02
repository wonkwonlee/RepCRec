"""
Created on Friday, 2022-12-02

Author: Wonkwon Lee, Young Il Kim

"""

from LockManager2 import LockManager2
from Config import *
from collections import defaultdict

class DataManager:
    """
    Initialize a data manager which manages all of the variables at a site.
    
    Args:
        site_id (int): Site ID
        
    Returns:
        DataManager: A data manager object each site has.
    """
    def __init__(self, site_id: int):
        self.site_id = site_id      # Site ID
        self.is_running = True      # Flag to indicate if the site is running
        self.data_table = {}        # Dictionary of variables stored at this site
        self.lock_table = {}        # Dictionary of lock managers for each variable
        self.fail_ts = []           # List of timestamps when the site failed
        self.recover_ts = []        # List of timestamps when the site recovered
        self.readable = set()       # Set of variables that are readable at this site

        # Initialize data variables
        for i in range(1, 21):
            v_id = "x" + str(i)    # Variable ID: x1, x2, ..., x20
            if i % 2 == 0:                      # Even variables are replicated
                self.data_table[v_id] = Variable(v_id, Commit(i*10,0), True)
                self.lock_table[v_id] = LockManager2(v_id)
            elif i % 10 + 1 == self.site_id:    # Odd variables are not replicated
                self.data_table[v_id] = Variable(v_id, Commit(i*10,0), False)
                self.lock_table[v_id] = LockManager2(v_id)
                                
    def read_snapshot(self, v_id: int, ts:int):
        """
        Read a variable from a snapshot during read-only transactions. 

        Args:
            v_id (int): Variable ID
            ts (int): Timestamp of the snapshot
            
        Returns:
            Output: An output object containing the result of the read_snapshot operation.
        """
        # var = self.data_table[v_id]
        
        # if not var.readable:
        #     return Output(False)
        # else:
        #     for cv in var.val_list:
        #         if cv.ts <= ts:
        #             if var.replicated:
        #                 for f in self.fail_ts:
        #                     if cv.ts < f and f <= ts:
        #                         return Output(False)
        #             return Output(True, cv.val)
                
        #     def read_snapshot(self, v_id, ts:int):

        var : Variable = self.data_table[v_id]
        if var.readable :
            # print("================ DM :: READ_SNAPSHOT :: Var.Readable :) ================")
            for commit in var.val_list :
                # print("commit :: {}".format(commit))
                # print("ts :: {}".format(ts))
                if commit.ts <= ts : 
                    # print("commit <= ts")
                    if var.replicated:
                        # print("var.replicated")
                        for fail in self.fail_ts:
                            # print("fail :: {}".format(fail))
                            if commit.ts < fail and fail <= ts :
                                # print("if commit < fail and fail <= ts")
                                return Output(False)
                    return Output(True, commit.val)
        return Output(False)
           
        
    def read(self, t_id: int, v_id: int):
        """
        Read a value from a variable during read-write transactions.

        Args:
            t_id (int): Variable ID
            v_id (int): Variable ID
            
        Returns:
            Output: An output object containing the result of the read operation.
        """
        var = self.data_table[v_id]
        if var.readable:
            lm = self.lock_table[v_id]
            current_lock = lm.current_lock
            
            if current_lock:
                if current_lock.lock_type == LockType.READ:
                    if t_id in current_lock.transaction_id_set:
                        return Output(True, var.val_list[0].val)
                    if not lm.has_other_queued_write_lock():
                        lm.share_read_lock(t_id)
                        return Output(True, var.val_list[0].val)
                    lm.add_queue(QLock(t_id, v_id, LockType.READ))
                    return Output(False, None)
                
                elif current_lock.lock_type == LockType.WRITE:
                    if t_id == current_lock.t_id:
                        return Output(True, var.tempVal)
                    lm.add_queue(QLock(t_id, v_id, LockType.READ))
                    return Output(False, None)
            

            lm.set_current_lock(RLock(t_id, v_id))
            return Output(True, var.val_list[0].val)
        return Output(False, None)
        
        
    def write(self, t_id: int, v_id: int, val: int):
        """
        Write a value to a variable during read-write transactions.

        Args:
            t_id (int): Transaction ID
            v_id (int): Variable ID
            val (int): Value to write
        """
        var = self.data_table[v_id]
        lm = self.lock_table[v_id]
        
        assert lm is not None and var is not None
        current_lock = lm.current_lock
        # print("================ DM :: def write() ================")
        # print("lm :: {}".format(lm))
        # print("var :: {}".format(var))
        # print("current_lock :: {}".format(current_lock))
        if current_lock:
            if current_lock.lock_type == LockType.READ:
                if len(current_lock.transaction_id_set) != 1:
                    print("Write lock cannot be acquired. Need to wait.")
                    return
                if t_id in current_lock.transaction_id_set:
                    if lm.has_other_queued_write_lock(t_id):
                        print("Write lock cannot be acquired. Need to wait.")
                        return
                    lm.promote_current_lock(
                        WLock(t_id, v_id))
                    var.temp_value = Temp(val, t_id)
                    return
                print("Write lock cannot be acquired. Need to wait.")
                return
            if t_id == current_lock.t_id:
                var.temp_value = Temp(val, t_id)
                return
            print("Write lock cannot be acquired. Need to wait.")
            return
        lm.set_current_lock(WLock(t_id, v_id))
        var.temp_value = Temp(val, t_id)

    def dump(self):
        """
        Dump all variables at this site and print them out.
        """
        status = "running" if self.is_running else "failed"
        output = f"Site {self.site_id} - {status}"

        for k, v in self.data_table.items():
            output += " {} : {}".format(k, v.val_list[0].val)
        print(output)

           
    def commit(self, t_id: int, ts: int):
        """
        Commit all variables that are written by the transaction.

        Args:
            t_id (int): Transaction ID
            ts (int): Timestamp of the commit
        """
        for k, v in self.lock_table.items():
            v.release_current_lock_by_transaction(t_id)
            for ql in list(v.queue):
                # print("ql.t_id {}".format(ql.t_id))
                # print("t_id {}".format(t_id))
                if ql.t_id == t_id:
                    continue
        #print("((((((((((((((((((((((((((((((((((((((((")
        for k, v in self.data_table.items():
            if v.temp_value and v.temp_value.t_id == t_id:
                v.update(Commit(v.temp_value.val, ts))
                v.readable = True
                # print("v.temp_value {}".format(v.temp_value.value))
                # print("v's commits {}".format(v.commits[0].val))
        self.resolve_lock_table()
        
    def abort(self, t_id):
        """
        Abort the transaction and release its locks.
        :param transaction_id: transaction's id
        """
        for k, v in self.lock_table.items():
            # release current lock held by this transaction
            v.release_current_lock_by_transaction(t_id)
            # remove queued locks of this transaction
            for ql in list(v.queue):
                if ql.t_id == t_id:
                    v.queue.remove(ql)
        self.resolve_lock_table()
        
    def get_write_lock(self, t_id, v_id):
        """
        Try to let a transaction get current W-lock on a variable.
        If it cannot get a current W-lock, add to lock queue.
        :param transaction_id: transaction's id
        :param variable_id: variable's id
        :return: boolean value to indicate if current W-lock can be acquired
        """
        lm: LockManager2 = self.lock_table[v_id]
        current_lock = lm.current_lock
        if current_lock:
            if current_lock.lock_type == LockType.READ:
                if len(current_lock.transaction_id_set) != 1:
                    # Multiple transactions holding R-lock on the same variable
                    lm.add_queue(
                        QLock(t_id, v_id, LockType.WRITE))
                    return False
                # Only one transaction holding an R-lock
                # Which one?
                # print("current_lock.transaction_id_set :: {}".format(current_lock.transaction_id_set))
                if t_id in current_lock.transaction_id_set:
                    # Only this transaction holds the R-lock
                    # Can it be promoted to W-lock?
                    if lm.has_other_queued_write_lock(t_id):
                        lm.add_queue(QLock(t_id, v_id, LockType.WRITE))
                        return False
                    return True
                # One other transaction is holding the R-lock
                lm.add_queue(
                    QLock(t_id, v_id, LockType.WRITE))
                return False
            # current lock is W-lock
            if t_id == current_lock.t_id:
                # This transaction already holds a W-lock
                return True
            # Another transaction is holding W-lock
            lm.add_queue(
                QLock(t_id, v_id,LockType.WRITE))
            return False
        # No existing lock on the variable
        return True

    def has_variable(self, v_id):
        """
        Check if a variable is stored at this site.
        """
        return self.data_table.get(v_id)

    def fail(self, ts):
        """
        Set site status to down and clear the lock table.
        :param ts: record the failure time
        """
        self.is_up = False
        self.fail_ts.append(ts)
        for k, v in self.lock_table.items():
            v.clear()

    def recover(self, ts):
        """
        Set site status to up.
        Replicated variables do not respond to Read until a committed write
        takes place.
        :param ts: record the recovery time
        """
        self.is_up = True
        self.recover_ts.append(ts)
        for v in self.data_table.values():
            if v.replicated:
                v.readable = False  # only for replicated variables

    def generate_blocking_graph(self):
        """
        Generate the blocking graph for this site
        :return: blocking graph
        """

        def current_blocks_queued(current_lock, queued_lock):
            """
            Check if the current lock is blocking a queued lock.
            :param current_lock: current lock
            :param queued_lock: a queued Lock
            :return: boolean value to indicate if current blocks queued
            """
            if current_lock.lock_type == LockType.READ:
                if queued_lock.lock_type == LockType.READ or \
                        (len(current_lock.transaction_id_set) == 1 and queued_lock.t_id in current_lock.transaction_id_set):
                    return False
                return True
            # current lock is W-lock
            return not current_lock.t_id == queued_lock.t_id

        def queued_blocks_queued(queued_lock_left, queued_lock_right):
            """
            Check if a queued lock is blocking another queued lock behind it.
            :param queued_lock_left: a queued lock
            :param queued_lock_right: another queued lock behind the first one
            :return: boolean value to indicate if queued blocks queued
            """
            if queued_lock_left.lock_type == LockType.READ and \
                    queued_lock_right.lock_type == LockType.READ:
                return False
            # at least one lock is W-lock
            return not queued_lock_left.t_id == queued_lock_right.t_id

        graph = defaultdict(set)
        for k, v in self.lock_table.items():
            if not v.current_lock or not v.queue:
                continue
            # print("current_lock: {}".format(lm.current_lock))
            # print("queue: {}".format(lm.queue))
            for ql in v.queue:
                if current_blocks_queued(v.current_lock, ql):
                    if v.current_lock.lock_type == LockType.READ:
                        for t_id in v.current_lock.transaction_id_set:
                            if t_id != ql.t_id:
                                graph[ql.t_id].add(t_id)
                    else:
                        if v.current_lock.t_id != ql.t_id:
                            graph[ql.t_id].add(
                                v.current_lock.t_id)
            for i in range(len(v.queue)):
                for j in range(i):
                    if queued_blocks_queued(v.queue[j], v.queue[i]):
                        graph[v.queue[i].t_id].add(v.queue[j].t_id)
        return graph

    def resolve_lock_table(self):
        """
        Check the lock table and move queued locks ahead if necessary.
        """
        for k, v in self.lock_table.items():
            if v.queue:
                if not v.current_lock:
                    # current lock is None
                    # pop the first queued lock and add to
                    first_ql = v.queue.pop(0)
                    if first_ql.lock_type == LockType.READ:
                        v.set_current_lock(RLock(first_ql.t_id, first_ql.v_id))
                    else:
                        v.set_current_lock(WLock(first_ql.t_id, first_ql.v_id))
                if v.current_lock.lock_type == LockType.READ:
                    # current lock is R-lock
                    # share R-lock with leading R-queued-locks
                    for ql in list(v.queue):
                        if ql.lock_type == LockType.WRITE:
                            if len(v.current_lock.transaction_id_set) == 1 \
                                    and ql.t_id in \
                                    v.current_lock.transaction_id_set:
                                v.promote_current_lock(WLock(ql.t_id, ql.v_id))
                                v.queue.remove(ql)
                            break
                        v.share_read_lock(ql.t_id)
                        v.queue.remove(ql)













                
    # def dump(self):
    #     status = "running" if self.is_running else "failed"
    #     output = f"Site {self.site_id} - {status}"

    #     for k, v in self.data_table.items():
    #         # output += f" {k}: {v.get_last_commit_value()}"
    #         output += " {} : {}".format(k, v.get_last_committed_value())
    #     print(output)
        
                
    # def read_snapshot(self, v_id, ts:int):
    #     # print("================ DM :: READ_SNAPSHOT ================")
    #     # print("v_id :: {}".format(v_id))
    #     var : Variable = self.data_table[v_id]
    #     if var.readable :
    #         # print("================ DM :: READ_SNAPSHOT :: Var.Readable :) ================")
    #         for commit in var.val_list :
    #             # print("commit :: {}".format(commit))
    #             # print("ts :: {}".format(ts))
    #             if commit.cm_ts <= ts : 
    #                 # print("commit <= ts")
    #                 if var.replicated:
    #                     # print("var.replicated")
    #                     for fail in self.fail_ts:
    #                         # print("fail :: {}".format(fail))
    #                         if commit.cm_ts < fail and fail <= ts :
    #                             # print("if commit < fail and fail <= ts")
    #                             return Result(False)
    #                 return Result(True, commit.value)
    #     return Result(False)

           
        
    # def read(self, t_id: int, v_id: int):
    #     var: Variable = self.data_table[v_id]
    #     if var.readable:
    #         lm: LockManager2 = self.lock_table[v_id]
    #         current_lock = lm.current_lock
            
    #         if current_lock:
    #             if current_lock.lock_type == LockType.READ:
    #                 if t_id in current_lock.transaction_id_set:
    #                     return Result(True, var.val_list[0].value)
    #                 if not lm.has_other_queued_write_lock():
    #                     lm.share_read_lock(t_id)
    #                     return Result(True, var.val_list[0].value)
    #                 lm.add_queue(QueuedLock(t_id, v_id, LockType.READ))
    #                 return Result(False, None)
                
    #             elif current_lock.lock_type == LockType.WRITE:
    #                 if t_id == current_lock.t_id:
    #                     return Result(True, var.tempVal)
    #                 lm.add_queue(QueuedLock(t_id, v_id, LockType.READ))
    #                 return Result(False, None)
            

    #         lm.set_current_lock(ReadLock(v_id, t_id, ))
    #         return Result(True, var.val_list[0].value)
        
    #     return Result(False, None)
        
        
        
    # def write(self, t_id: int, v_id: int, val: int):
    #     lm: LockManager2 = self.lock_table[v_id]
    #     var: Variable = self.data_table[v_id]

    #     assert lm is not None and var is not None
    #     current_lock = lm.current_lock
    #     # print("================ DM :: def write() ================")
    #     # print("lm :: {}".format(lm))
    #     # print("var :: {}".format(var))
    #     # print("current_lock :: {}".format(current_lock))
    #     if current_lock:
    #         # print("current_lock.lock_type :: {}".format(current_lock.lock_type))
    #         if current_lock.lock_type == LockType.READ:
    #             if len(current_lock.transaction_id_set) != 1:
    #                 raise RuntimeError("Cannot promote to W-Lock: " "other transactions are holding R-lock!")
    #             if t_id in current_lock.transaction_id_set:
    #                 if lm.has_other_queued_write_lock(t_id):
    #                     raise RuntimeError("Cannot promote to W-Lock: " "other W-lock is waiting in queue!")
    #                 lm.promote_current_lock(
    #                     WriteLock(v_id, t_id))
    #                 var.temp_value = TempValue(val, t_id)
    #                 return
    #             raise RuntimeError("Cannot promote to W-Lock: " "R-lock is not held by this transaction!")
    #         # current lock is W-lock
    #         if t_id == current_lock.t_id:
    #             # This transaction already holds a W-lock
    #             var.temp_value = TempValue(val, t_id)
    #             return
    #         # Another transaction is holding W-lock
    #         raise RuntimeError("Cannot get W-Lock: "
    #                            "another transaction is holding W-lock!")
    #     # No existing lock on the variable
    #     lm.set_current_lock(WriteLock(v_id, t_id))
    #     var.temp_value = TempValue(val, t_id)

   
           
    # def commit(self, t_id, commit_ts):
    #     """
    #     Commit a transaction and release its locks.
    #     :param transaction_id: transaction's id
    #     :param commit_ts: the timestamp of the commit
    #     """
    #     for lm in self.lock_table.values():
    #         # release current lock held by this transaction
    #         lm.release_current_lock_by_transaction(t_id)
    #         # there shouldn't be any queued locks of this transaction
    #         # print(lm.queue)

    #         for ql in list(lm.queue):
    #             # print("ql.t_id {}".format(ql.t_id))
    #             # print("t_id {}".format(t_id))
    #             if ql.t_id == t_id:
    #                 continue
    #                 # print("hello")
    #                 # raise RuntimeError("{} cannot commit with unresolved queued locks!".format(t_id))
    #     # commit temp values
    #     #print("((((((((((((((((((((((((((((((((((((((((")
    #     for v in self.data_table.values():
    #         if v.temp_value and v.temp_value.t_id == t_id:
    #             v.add_commit_value(CommitValue(v.temp_value.value, commit_ts))
    #             v.readable = True
    #             # print("v.temp_value {}".format(v.temp_value.value))
    #             # print("v's val_list {}".format(v.val_list[0].value))
    #     self.resolve_lock_table()
        
    
    # def abort(self, t_id):
    #     """
    #     Abort the transaction and release its locks.
    #     :param transaction_id: transaction's id
    #     """
    #     for lm in self.lock_table.values():
    #         # release current lock held by this transaction
    #         lm.release_current_lock_by_transaction(t_id)
    #         # remove queued locks of this transaction
    #         for ql in list(lm.queue):
    #             if ql.t_id == t_id:
    #                 lm.queue.remove(ql)
    #     self.resolve_lock_table()
        
    # def get_write_lock(self, t_id, v_id):
    #     """
    #     Try to let a transaction get current W-lock on a variable.
    #     If it cannot get a current W-lock, add to lock queue.
    #     :param transaction_id: transaction's id
    #     :param variable_id: variable's id
    #     :return: boolean value to indicate if current W-lock can be acquired
    #     """
    #     lm: LockManager2 = self.lock_table[v_id]
    #     current_lock = lm.current_lock
    #     if current_lock:
    #         if current_lock.lock_type == LockType.READ:
    #             if len(current_lock.transaction_id_set) != 1:
    #                 # Multiple transactions holding R-lock on the same variable
    #                 lm.add_queue(
    #                     QueuedLock(v_id, t_id, LockType.WRITE))
    #                 return False
    #             # Only one transaction holding an R-lock
    #             # Which one?
    #             print("current_lock.transaction_id_set :: {}".format(current_lock.transaction_id_set))
    #             if t_id in current_lock.transaction_id_set:
    #                 # Only this transaction holds the R-lock
    #                 # Can it be promoted to W-lock?
    #                 if lm.has_other_queued_write_lock(t_id):
    #                     lm.add_queue(QueuedLock(v_id, t_id, LockType.WRITE))
    #                     return False
    #                 return True
    #             # One other transaction is holding the R-lock
    #             lm.add_queue(
    #                 QueuedLock(v_id, t_id, LockType.WRITE))
    #             return False
    #         # current lock is W-lock
    #         if t_id == current_lock.t_id:
    #             # This transaction already holds a W-lock
    #             return True
    #         # Another transaction is holding W-lock
    #         lm.add_queue(
    #             QueuedLock(v_id, t_id, LockType.WRITE))
    #         return False
    #     # No existing lock on the variable
    #     return True

    # # def has_variable(self, v_id):
    # #     """
    # #     Check if a variable is stored at this site.
    # #     """
    # #     return self.data_table.get(v_id)

    
    # # def resolve_lock_table(self):
    # #     """
    # #     Check the lock table and move queued locks ahead if necessary.
    # #     """
    # #     for v, lm in self.lock_table.items():
    # #         if lm.queue:
    # #             if not lm.current_lock:
    # #                 # current lock is None
    # #                 # pop the first queued lock and add to
    # #                 first_ql = lm.queue.pop(0)
    # #                 if first_ql.lock_type == LockType.READ:
    # #                     lm.set_current_lock(ReadLock(first_ql.v_id, first_ql.t_id))
    # #                 else:
    # #                     lm.set_current_lock(WriteLock(first_ql.v_id, first_ql.t_id))
    # #             if lm.current_lock.lock_type == LockType.READ:
    # #                 # current lock is R-lock
    # #                 # share R-lock with leading R-queued-locks
    # #                 for ql in list(lm.queue):
    # #                     if ql.lock_type == LockType.WRITE:
    # #                         if len(lm.current_lock.transaction_id_set) == 1 \
    # #                                 and ql.t_id in \
    # #                                 lm.current_lock.transaction_id_set:
    # #                             lm.promote_current_lock(WriteLock(ql.v_id, ql.t_id))
    # #                             lm.queue.remove(ql)
    # #                         break
    # #                     lm.share_read_lock(ql.t_id)
    # #                     lm.queue.remove(ql)


