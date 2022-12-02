"""
Due on Saturday, 12/03/2022

Author: Wonkwon Lee, Young Il Kim

"""
from Config import *
from LockManager import LockManager
from collections import defaultdict

class DataManager(object):
    """
    Initialize a data manager which manages all of the variables at a site.
    Data manager is responsible for managing locks and data values during transactions.
    
    Args:
        site_id (int): Site ID
        
    Returns:
        DataManager: A data manager object one for each site. Even variables are replicated at all sites and odd variables are not replicated.
    """
    def __init__(self, site_id: str):
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
                self.lock_table[v_id] = LockManager(v_id)
            elif i % 10 + 1 == self.site_id:    # Odd variables are not replicated
                self.data_table[v_id] = Variable(v_id, Commit(i*10,0), False)
                self.lock_table[v_id] = LockManager(v_id)
                   
    def read_snapshot(self, v_id: int, ts:int):
        """
        Read a variable from a snapshot during read-only transactions. 

        Args:
            v_id (int): Variable ID
            ts (int): Timestamp of the snapshot
            
        Returns:
            Output: An output object containing the result of the read_snapshot operation.
        """
        var = self.data_table[v_id]
        
        if not var.readable:
            return Output(False)
        else:
            for cv in var.val_list:
                if cv.ts <= ts:
                    if var.replicated:
                        for f in self.fail_ts:
                            if cv.ts < f and f <= ts:
                                return Output(False)
                    return Output(True, cv.val)
        
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
            lock = lm.lock
            
            if lock:
                if lock.type == LockType.READ:
                    if t_id in lock.t_table:
                        return Output(True, var.val_list[0].val)
                    if not lm.has_other_queued_write_lock():
                        lm.share_lock(t_id)
                        return Output(True, var.val_list[0].val)
                    lm.add_queue(QLock(t_id, v_id, LockType.READ))
                    return Output(False, None)
                
                elif lock.type == LockType.WRITE:
                    if t_id == lock.t_id:
                        return Output(True, var.tempVal)
                    lm.add_queue(QLock(t_id, v_id, LockType.READ))
                    return Output(False, None)
                else:
                    print("Invalid lock type")
                    return Output(False, None)
            lm.lock = RLock(t_id, v_id)
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
        lock = lm.lock
        # print("================ DM :: def write() ================")
        # print("lm :: {}".format(lm))
        # print("var :: {}".format(var))
        # print("lock :: {}".format(lock))
        if lock:
            if lock.type == LockType.READ: 
                if len(lock.t_table) != 1:
                    print("Write lock cannot be acquired. Need to wait.")
                    return
                if t_id in lock.t_table:
                    if lm.has_other_queued_write_lock(t_id):
                        print("Write lock cannot be acquired. Need to wait.")
                        return
                    lm.process_lock(WLock(t_id, v_id))
                    var.temp = Temp(val, t_id)
                    return
                print("Write lock cannot be acquired. Need to wait.")
                return
            if t_id == lock.t_id:
                var.temp = Temp(val, t_id)
                return
            print("Write lock cannot be acquired. Need to wait.")
            return
        lm.lock = WLock(t_id, v_id)
        var.temp = Temp(val, t_id)
        return

    def fail(self, ts: int):
        """
        Fail the site and release all locks.
        
        Args:
            ts (int): Timestamp of the failure
        """
        self.is_running = False
        self.fail_ts.append(ts)
        for k, v in self.lock_table.items():
            v.lock = None
            v.lock_queue = []

    def recover(self, ts: int):
        """
        Recover the site and set the site status to up.
        
        Args:
            ts (int): Timestamp of the recovery
        """
        self.is_running = True
        self.recover_ts.append(ts)
        for k, v in self.data_table.items():
            if v.replicated:
                v.readable = False
                
    def dump(self):
        """
        Dump all variables at this site and print them out.
        """
        status = "running" if self.is_running else "failed"
        output = f"Site {self.site_id} - {status}"

        for k, v in self.data_table.items():
            output += " {} : {}".format(k, v.val_list[0].val)
        print(output)

    def abort(self, t_id: int):
        """
        Abort all variables that are written by the transaction.

        Args:
            t_id (int): Transaction ID
        """
        for k, v in self.lock_table.items():
            v.release_current_lock_by_transaction(t_id)
            for ql in list(v.lock_queue):
                if ql.t_id == t_id:
                    v.lock_queue.remove(ql)
        self.release_lock()                
                           
    def commit(self, t_id: int, ts: int):
        """
        Commit all variables that are written by the transaction.

        Args:
            t_id (int): Transaction ID
            ts (int): Timestamp of the commit
        """
        for k, v in self.lock_table.items():
            v.release_current_lock_by_transaction(t_id)
            for ql in list(v.lock_queue):
                # print("ql.t_id {}".format(ql.t_id))
                # print("t_id {}".format(t_id))
                if ql.t_id == t_id:
                    continue
        for k, v in self.data_table.items():
            if v.temp and v.temp.t_id == t_id:
                v.val_list.insert(0, Commit(v.temp.val, ts))
                v.readable = True
                # print("v.temp {}".format(v.temp.value))
                # print("v's commits {}".format(v.commits[0].val))
        self.release_lock()
    
    def acquire_wlock(self, t_id: int, v_id: int):
        """
        Acquire a write lock on a variable. If the lock is not available, add the transaction to the queue.
        
        Args:
            t_id (int): Transaction ID
            v_id (int): Variable ID
        Returns:
            bool: True if the lock is acquired, False otherwise.
        """
        lm = self.lock_table[v_id]
        lock = lm.lock
        if lock:
            if lock.type == LockType.READ:
                if len(lock.t_table) != 1:
                    lm.add_queue(QLock(t_id, v_id, LockType.WRITE))
                    return False
                # print("lock.t_table :: {}".format(lock.t_table))
                if t_id in lock.t_table:
                    if lm.has_other_queued_write_lock(t_id):
                        lm.add_queue(QLock(t_id, v_id, LockType.WRITE))
                        return False
                    return True
                lm.add_queue(QLock(t_id, v_id, LockType.WRITE))
                return False
            if t_id == lock.t_id:
                return True
            lm.add_queue(QLock(t_id, v_id,LockType.WRITE))
            return False
        return True
    
    def init_block_graph(self):
        """
        Initialize the block graph for the site.
        

        Returns:
            graph (dict): Blocking graph
        """
        graph = defaultdict(set)
        for k, v in self.lock_table.items():
            if not v.lock or not v.lock_queue:
                continue
            for l in v.lock_queue:
                if self.check_qlock(v.lock, l):
                    if v.lock.type == LockType.READ:
                        for t in v.lock.t_table:
                            if t != l.t_id:
                                graph[l.t_id].add(l.t_id)
                    else:
                        if v.lock.t_id != l.t_id:
                            graph[l.t_id].add(v.lock.t_id)
                        
            for i in range(len(v.lock_queue)):
                for j in range(i):
                    if self.check_queue(v.lock_queue[j], v.lock_queue[i]):
                        graph[v.lock_queue[i].t_id].add(v.lock_queue[j].t_id)
        return graph
        
    def check_qlock(self, lock, qlock):
        if lock.type == LockType.READ:
            if qlock.type == LockType.READ or len(lock.t_table) == 1:
                if qlock.t_id in lock.t_table:
                    return False
            return True
        return not lock.t_id == qlock.t_id

    def check_queue(self, head, tail):
        if head.type == LockType.READ and tail.type == LockType.READ:
            return False
        return not head.t_id == tail.t_id


    def release_lock(self):
        """
        Check the lock table and move queued locks ahead if necessary.
        """
        for k, v in self.lock_table.items():
            if v.lock_queue:
                if not v.lock:
                    # current lock is None
                    # pop the first queued lock and add to
                    lock = v.lock_queue.pop(0)
                    if lock.type == LockType.READ:
                        v.lock = RLock(lock.t_id, lock.v_id)
                    else:
                        v.lock = WLock(lock.t_id, lock.v_id)
                if v.lock.type == LockType.READ:
                    for ql in list(v.lock_queue):
                        if ql.type == LockType.WRITE:
                            if len(v.lock.t_table) == 1 and ql.t_id in v.lock.t_table:
                                v.process_lock(WLock(ql.t_id, ql.v_id))
                                v.lock_queue.remove(ql)
                            break
                        v.share_lock(ql.t_id)
                        v.lock_queue.remove(ql)
