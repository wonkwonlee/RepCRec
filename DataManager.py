from LockManager import LockManager
from Config import *

class Variable(object):
    def __init__(self, v_id: int, init_val, replicated: bool):
        self.v_id = v_id
        self.val = int(v_id[1:])*10
        self.commit_list = [init_val]  # transaction ID: commit timestamp
        self.readable = True
        self.replicated = replicated
        self.fail = False
        self.tempVal = None


class DataManager:
    def __init__(self, site_id: int):
        self.site_id = site_id
        self.is_running = True
        self.data_table = {}
        self.lock_table = {}
        self.fail_ts = []
        self.recover_ts = []
        self.readable = set()

        # Initialize data variables
        for i in range(1, 21):
            v_id = "x" + str(i)
            if i % 2 == 0:
                self.data_table[v_id] = Variable(v_id, CommitValue(i*10,0), True)
                self.lock_table[v_id] = LockManager(v_id)
            elif i % 10 + 1 == self.site_id:
                self.data_table[v_id] = Variable(v_id, CommitValue(i*10,0), False)
                self.lock_table[v_id] = LockManager(v_id)
                
                
    def dump(self):
        status = "running" if self.is_running else "failed"
        output = f"Site {self.site_id} - {status}"

        for k, v in self.data_table.items():
            output += f" {k}: {v.val}"
        print(output)
        
    def fail(self, ts: int):
        self.is_running = False
        self.fail_ts.append(ts)

        for k, v in self.lock_table.items():
            self.lock_table[k] = None
    
    def recover(self, ts: int):
        self.is_running = True
        self.recover_ts.append(ts)

        for _, v in self.data_table.items():
            if v.replicated:
                self.readable = False
                
    def read_snapshot(self, v_id, ts:int):
        
        # print("================ DM :: READ_SNAPSHOT ================")
        # print("v_id :: {}".format(v_id))
        var : Variable = self.data_table[v_id]
        if var.readable :
            # print("================ DM :: READ_SNAPSHOT :: Var.Readable :) ================")
            for commit in var.commits :
                # print("commit :: {}".format(commit))
                # print("ts :: {}".format(ts))
                if commit <= ts : 
                    # print("commit <= ts")
                    if var.replicated:
                        # print("var.replicated")
                        for fail in self.fail_ts:
                            # print("fail :: {}".format(fail))
                            if commit < fail and fail <= ts :
                                # print("if commit < fail and fail <= ts")
                                return False
                    return True
        return False

           
        
    def read(self, t_id: int, v_id: int):
        var: Variable = self.data_table[v_id]
        if var.readable:
            lm: LockManager = self.lock_table[v_id]
            current_lock = lm.current_lock
            
            if current_lock:
                if current_lock.lock_type == LockType.READ:
                    if t_id in current_lock.transaction_id_set:
                        return Result(True, var.commit_list[0].value)
                    if not lm.has_write_lock(t_id):
                        lm.share_lock(t_id)
                        return Result(True, var.commit_list[0].value)
                    lm.add_queue(QueuedLock(t_id, v_id, LockType.READ))
                    return Result(False, None)
                
                elif current_lock.lock_type == LockType.WRITE:
                    if t_id == current_lock.transaction_id:
                        return Result(True, var.tempVal)
                    lm.add_queue(QueuedLock(t_id, v_id, LockType.READ))
                    return Result(False, None)
            

            lm.set_lock(ReadLock(t_id, v_id))
            return Result(True, var.commit_list[0].value)
        
        return Result(False, None)
        
        
        
    def write(self, t_id: int, v_id: int, val: int):
        lm: LockManager = self.lock_table[v_id]
        var: Variable = self.data_table[v_id]

        assert lm is not None and var is not None
        current_lock = lm.current_lock
        print("================ DM :: def write() ================")
        print("lm :: {}".format(lm))
        print("var :: {}".format(var))
        print("current_lock :: {}".format(current_lock))
        if current_lock:
            
            print("current_lock.lock_type :: {}".format(current_lock.lock_type))
        #     if current_lock.lock_type == LockType.R:
        #         # If current lock is a read lock, then it must be of the same transaction,
        #         # and there should be no other locks waiting in queue.
        #         assert len(lm.shared_read_lock) == 1 and \
        #                t_id in lm.shared_read_lock and \
        #                not lm.has_other_write_lock(t_id)
        #         lm.promote_current_lock(WriteLock(t_id, v_id))
        #         v.temporary_value = TemporaryValue(value, t_id)
        #     else:
        #         # If current lock is a write lock, then it must of the same transaction.
        #         assert tid == current_lock.tid
        #         v.temporary_value = TemporaryValue(value, t_id)
        # else:
        #     lm.set_current_lock(WriteLock(t_id, v_id))
        #     v.temporary_value = TemporaryValue(value, t_id)

   
           
    def commit(self, t_id: int, c_ts: int):
        print(self.lock_table)
        for lm in self.lock_table.values():
            lm.releaseCurrentLock(t_id)

        # for v in self.data.values():
        #     if v.temporary_value is not None and v.temporary_value.tid == tid:
        #         commit_value = v.temporary_value.value
        #         v.add_commit_value(CommitValue(commit_value, commit_time))
        #         v.temporary_value = None
        #         v.is_readable = True
        # self.update_lock_table()
        
    
    def abort(self, t_id: int):
        pass
            
        # else:
        #     var.fail = True
        #     var.val = None
        
    def acquire_lock(self, t_id: int, v_id: int):
        lm: LockManager = self.lock_table[v_id]
        current_lock = lm.current_lock
        if current_lock:
            if current_lock.lock_type == LockType.R:
                if len(current_lock.transaction_id_set) != 1:
                    # Multiple transactions holding R-lock on the same variable
                    lm.add_to_queue(
                        QueuedLock(v_id, t_id, LockType.W))
                    return False
                # Only one transaction holding an R-lock
                # Which one?
                if t_id in current_lock.transaction_id_set:
                    # Only this transaction holds the R-lock
                    # Can it be promoted to W-lock?
                    if lm.has_other_queued_write_lock(t_id):
                        lm.add_to_queue(
                            QueuedLock(v_id, t_id, LockType.W))
                        return False
                    return True
                # One other transaction is holding the R-lock
                lm.add_to_queue(
                    QueuedLock(v_id, transaction_id, LockType.W))
                return False
            # current lock is W-lock
            if transaction_id == current_lock.transaction_id:
                # This transaction already holds a W-lock
                return True
            # Another transaction is holding W-lock
            lm.add_to_queue(
                QueuedLock(v_id, transaction_id, LockType.W))
            return False
        # No existing lock on the variable
        return True

    def has_variable(self, variable_id):
        """
        Check if a variable is stored at this site.
        """
        return self.data_table.get(variable_id)

    


