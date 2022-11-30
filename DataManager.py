from LockManager import LockManager
from Config import *

class Variable(object):
    def __init__(self, v_id: int, replicated: bool):
        self.v_id = v_id
        self.val = int(v_id[1:])*10
        self.commit_list = [self.val]  # transaction ID: commit timestamp
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
                self.data_table[v_id] = Variable(v_id, True)
                self.lock_table[v_id] = LockManager(v_id)
            elif i % 10 + 1 == self.site_id:
                self.data_table[v_id] = Variable(v_id, False)
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
        
        print("================ DM :: READ_SNAPSHOT ================")
        print("v_id :: {}".format(v_id))
        var : Variable = self.data_table[v_id]
        if var.readable :
            print("================ DM :: READ_SNAPSHOT :: Var.Readable :) ================")
            for commit in var.commits :
                print("commit :: {}".format(commit))
                print("ts :: {}".format(ts))
                if commit <= ts : 
                    print("commit <= ts")
                    if var.replicated:
                        print("var.replicated")
                        for fail in self.fail_ts:
                            print("fail :: {}".format(fail))
                            if commit < fail and fail <= ts :
                                # print("if commit < fail and fail <= ts")
                                return False
                    return True
        return False

           
        
    def read(self, t_id: int, v_id: int):
        return True
        pass
    
    def write(self, t_id: int, v_id: int, val: int):
        lm: LockManager = self.lock_table.get(v_id)
        v: Variable = self.data.get(v_id)

        assert lm is not None and v is not None
        current_lock = lm.current_lock

        if current_lock:
            if current_lock.lock_type == LockType.R:
                # If current lock is a read lock, then it must be of the same transaction,
                # and there should be no other locks waiting in queue.
                assert len(lock_manager.shared_read_lock) == 1 and \
                       tid in lock_manager.shared_read_lock and \
                       not lock_manager.has_other_write_lock(tid)
                lock_manager.promote_current_lock(WriteLock(tid, vid))
                v.temporary_value = TemporaryValue(value, tid)
            else:
                # If current lock is a write lock, then it must of the same transaction.
                assert tid == current_lock.tid
                v.temporary_value = TemporaryValue(value, tid)
        else:
            lock_manager.set_current_lock(WriteLock(tid, vid))
            v.temporary_value = TemporaryValue(value, tid)

   
           
    def commit(self, t_id: int, commit_time: int):
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
        return True
        pass

class CommitValue:
    """Represents a committed value of a variable."""

    def __init__(self, value, ts):
        """
        Initialize a CommitValue instance.
        :param value: the committed value
        :param commit_ts: the timestamp of the commit
        """
        self.value = value
        self.cm_ts = ts

