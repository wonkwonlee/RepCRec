from Transaction import Transaction, ReadOnlyTransaction, ReadWriteTransaction, Operation
from DataManager import DataManager
import SiteManager
import LockManager

class TransactionManager:
    transaction_table = {}
    ts = 0
    operation_list = []
    
    
    def __init__(self):
        self.site_list = []
        
        for site in range(1, 11):
            self.site_list.append(DataManager(site))
    
    def read_operation(self, t_id, v_id):
        """
        Read operation
        """
        self.operation_list.append(Operation('R', t_id, v_id, None))
    
    def write_operation(self, t_id, v_id, val):
        """
        Write operation
        """
        self.operation_list.append(Operation('W', t_id, v_id, val))
        
    
    def run_operation(self, operation):
        """
        Run an operation.
        """
        if operation.operation_type == 'R':
            self.read(operation.t_id, operation.v_id)
        elif operation.operation_type == 'W':
            self.write(operation.t_id, operation.v_id, operation.val)
    
    
    '''
    Functions for instruction execution
    '''  
    def begin(self, t_id):
        """
        Begin a new transaction.
        """
        self.transaction_table[t_id] = ReadWriteTransaction(t_id, self.ts, is_ro=False)
        self.ts += 1
        print("Transaction {} begins with time stamp {}".format(t_id, self.ts),'\n')
    
    def beginRO(self, t_id):
        """
        Begin a new read-only transaction.
        """
        self.transaction_table[t_id] = ReadOnlyTransaction(t_id, self.ts, is_ro=True)
        self.ts += 1
        print("Read-only transaction {} begins with time stamp {}".format(t_id, self.ts),'\n')
    
    def read_snapshot(self, t_id, v_id):
        """
        Read a variable from the snapshot of a read-only transaction.
        """
        if v_id not in self.transaction_table[t_id].snapshot:
            print("Transaction {} aborts".format(t_id),'\n')
            self.transaction_table[t_id].is_aborted = True
        else:
            print("Transaction {} reads variable {} = {}".format(t_id, v_id, self.transaction_table[t_id].snapshot[v_id]),'\n')
            
    def read(self, t_id, v_id):
        """
        Read a variable from a read-write transaction.
        """
        if self.transaction_table[t_id].is_aborted:
            print("Transaction {} aborts".format(t_id),'\n')
        else:
            if v_id not in self.transaction_table[t_id].read_values:
                print("Transaction {} aborts".format(t_id),'\n')
                self.transaction_table[t_id].is_aborted = True
            else:
                print("Transaction {} reads variable {} = {}".format(t_id, v_id, self.transaction_table[t_id].read_values[v_id]),'\n')
                
    # TODO: Implement LOCK function
    def write(self, t_id, v_id, val):
        """
        Write a variable from a read-write transaction.
        """
        if self.transaction_table[t_id].is_aborted:
            print("Transaction id {} not in table".format(t_id))
            print("Transaction {} aborts".format(t_id),'\n')
        else:
            if v_id not in self.transaction_table[t_id].write_values:
                print("Transaction id {} not in table".format(t_id))
                print("Transaction {} aborts".format(t_id),'\n')
                self.transaction_table[t_id].is_aborted = True
            else:
                print("Transaction {} writes variable {} = {}".format(t_id, v_id, val),'\n')
                self.transaction_table[t_id].write_values[v_id] = val
                self.transaction_table[t_id].write_ts[v_id] = self.ts
                self.ts += 1
    
    def dump(self):
        """
        Dump the state of all sites.
        """
        for site in self.site_list:
            site.dump()
                
    def end(self, t_id):
        """
        End a transaction.
        """
        if self.transaction_table[t_id].is_aborted:
            self.abort(t_id)
        else:
            self.commit(t_id)
            
    def abort(self, t_id):
        """
        Abort a transaction.
        """
        for site in self.site_list:
            site.abort(t_id)
        del self.transaction_table[t_id]
        print("Transaction {} aborts".format(t_id),'\n')

    def commit(self, t_id, c_ts):
        """
        Commit a transaction.
        """
        for site in self.site_list:
            site.commit(t_id, c_ts)
        del self.transaction_table[t_id]
        print("Transaction {} commits".format(t_id),'\n')
        
    def recover(self, site_id):
        """
        Recover a site.
        """
        self.site_list[site_id - 1].recover(site_id, self.ts)
        print("Site {} recovers".format(site_id),'\n')
            
    def fail(self, site_id):
        """
        Fail a site.
        """
        self.site_list[site_id - 1].fail(site_id, self.ts)
        print("Site {} fails".format(site_id),'\n')
        
    def getTimeStamp(self) :
        """
        Get Time Stamp
        """
        print("Time Stamp :: {}".format(ts),'\n')
        return self.ts