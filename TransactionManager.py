from Transaction import Transaction, ReadOnlyTransaction, ReadWriteTransaction, Operation
from DataManager import DataManager
import SiteManager
import LockManager

class TransactionManager:
    transaction_table = {}
    ts = 0
    operation_list = []
    
    
    def __init__(self):
        self.dm_list = []
        
        for dm in range(1, 11):
            self.dm_list.append(DataManager(dm))
    
    def read_operation(self, t_id, v_id):
        """
        Read operation
        """
        if not t_id in self.transaction_table:
            print("Transaction table does not contains {}".format(t_id),'\n')
            return False
        self.ts += 1
        self.operation_list.append(Operation('R', t_id, v_id, None))
    
    def write_operation(self, t_id, v_id, val):
        """
        Write operation
        """
        if not t_id in self.transaction_table:
            print("Transaction table does not contains {}".format(t_id),'\n')
            return False
        self.ts += 1
        self.operation_list.append(Operation('W', t_id, v_id, val))
    
    def run_operation(self):
        """
        Run an operation.
        """
        for operation in self.operation_list:
            if not operation.t_id in self.transaction_table:
                # print("Transaction id {} not in table".format(operation.t_id))
                self.operation_list.remove(operation)
            else:
                if operation.op == 'R':
                    if self.transaction_table[operation.t_id].is_ro:
                        result = self.read_snapshot(operation.t_id, operation.v_id)
                    else:
                        result = self.read(operation.t_id, operation.v_id)
                elif operation.op == 'W':
                    result = self.write(operation.t_id, operation.v_id, operation.val)              
                else:
                    print("Invalid operation")
                
                if result:   
                    self.operation_list.remove(operation)
    
    '''
    Functions for instruction execution
    '''  
    def begin(self, t_id):
        """
        Begin a new transaction.
        """
        self.ts += 1
        self.transaction_table[t_id] = Transaction(t_id, self.ts, is_ro=False)
        print("Transaction {} begins at time stamp {}".format(t_id, self.ts),'\n')
    
    def beginRO(self, t_id):
        """
        Begin a new read-only transaction.
        """
        self.ts += 1
        self.transaction_table[t_id] = Transaction(t_id, self.ts, is_ro=True)
        print("Read-only transaction {} begins at time stamp {}".format(t_id, self.ts),'\n')
    
    def read_snapshot(self, t_id, v_id):
        """
        Read a variable from the snapshot of a read-only transaction.
        """
        if not t_id in self.transaction_table:
            print("Transaction {} aborts".format(t_id),'\n')
            return False
        for dm in self.dm_list:
            if dm.is_running and v_id in dm.data_table:
                result = dm.read_snapshot(t_id, v_id)
                if result:
                    self.transaction_table[t_id].visited_sites.append(dm.site_id)
                    print("Transaction {} reads variable {} of {} on site {} at time stamp {}".format(t_id, v_id, dm.data_table[v_id].val, dm.site_id, self.ts),'\n')
                    return True
        return False
            
          
    def read(self, t_id, v_id):
        """
        Read a variable from a read-write transaction.
        """
        if not t_id in self.transaction_table:
            print("Transaction {} aborts".format(t_id),'\n')
            return False
        for dm in self.dm_list:
            if dm.is_running and v_id in dm.data_table:
                result = dm.read_snapshot(t_id, v_id)
                if result:
                    self.transaction_table[t_id].visited_sites.append(dm.site_id)
                    print("Transaction {} reads variable {} of {} on site {}".format(t_id, v_id, dm.data_table[v_id].val, dm.site_id),'\n')
                    return True
        return False
        
        
    
    # TODO: Implement LOCK function
    def write(self, t_id, v_id, val):
        """
        Write a variable from a read-write transaction.
        """
        if not t_id in self.transaction_table:
            print("Transaction {} aborts".format(t_id),'\n')
            return False
        for dm in self.dm_list:
            # print(dm.data_table)
            if dm.is_running and v_id in dm.data_table:
                result = dm.write(t_id, v_id, val)
                if result:
                    self.transaction_table[t_id].visited_sites.append(dm.site_id)
                    print("Transaction {} writes variable {} on site {} at time stamp {}".format(t_id, v_id, val, dm.site_id, self.ts),'\n')
                    return True
        return False
        
        
    
    def dump(self):
        """
        Dump the state of all sites.
        """
        for dm in self.dm_list:
            dm.dump()
                
    def end(self, t_id):
        """
        End a transaction.
        """
        if self.transaction_table[t_id].is_aborted:
            self.abort(t_id)
        else:
            self.commit(t_id, self.ts)
            
    def abort(self, t_id):
        """
        Abort a transaction.
        """
        for dm in self.dm_list:
            dm.abort(t_id)
        del self.transaction_table[t_id]
        print("Transaction {} aborts at time stamp {}".format(t_id, self.ts),'\n')

    def commit(self, t_id, ts):
        """
        Commit a transaction.
        """
        for dm in self.dm_list:
            dm.commit(t_id, ts)
        del self.transaction_table[t_id]
        print("Transaction {} commits at time stamp {}".format(t_id, self.ts),'\n')
        
    def recover(self, site_id):
        """
        Recover a site.
        """
        self.ts += 1
        self.dm_list[int(site_id) - 1].recover(self.ts)
        print("Site {} recovers at time stamp {}".format(site_id, self.ts),'\n')
            
    def fail(self, site_id):
        """
        Fail a site.
        """
        self.ts += 1
        self.dm_list[int(site_id) - 1].fail(self.ts)    
        print("Site {} fails".format(site_id),'\n')
        
    def getTimeStamp(self) :
        """
        Get Time Stamp
        """
        print("Time Stamp :: {}".format(self.ts),'\n')
        return self.ts