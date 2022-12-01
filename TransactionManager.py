from Transaction import Transaction, ReadOnlyTransaction, ReadWriteTransaction, Operation
from DataManager import DataManager
import SiteManager
import LockManager2
from Config import *
from collections import defaultdict

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
                result = False
                if operation.op == 'R':
                    # print("================ TM :: RUN_OPERATION ================")
                    # print("self.transaction_table[{}].is_ro :: {}".format(operation.t_id,self.transaction_table[operation.t_id].is_ro))
                    if self.transaction_table[operation.t_id].is_ro:
                        # print("Do self.read_snapshot()")
                        result = self.read_snapshot(operation.t_id, operation.v_id)
                    else:
                        # print("Do self.read()")
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
                # print("================ TM :: READ_SNAPSHOT ================")
                # print("v_id :: {}".format(v_id))
                result = dm.read_snapshot(v_id, self.ts)
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
                # print("================ TM :: READ ================")
                # print("v_id :: {}".format(v_id))
                result = dm.read(t_id, v_id)
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
        sitesDownFlag = True
        getAllWriteLocksFlag = True
        for dm in self.dm_list:
            # print(dm.data_table)
            # print(dm.is_running)
            # print(v_id in dm.data_table)
            #if dm.is_running and v_id in dm.data_table:
            if dm.is_running and dm.has_variable(v_id):
                #print("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                # result = dm.write(t_id, v_id, val)
                result = dm.get_write_lock(t_id, v_id)
                
                sitesDownFlag = False
                if not result:
                    print("Transaction Waiting")
                    getAllWriteLocksFlag = False
                
        if not sitesDownFlag and getAllWriteLocksFlag :
            sitesList = []
            for dm in self.dm_list:
                # print(dm.data_table)
                if dm.is_running and v_id in dm.data_table:
                    dm.write(t_id, v_id, val)
                    self.transaction_table[t_id].visited_sites.append(dm.site_id)
                    sitesList.append(dm.site_id)
            # print("****************************************")
            print("Transaction {} writes variable {} with value {} on site {} at time stamp {}".format(t_id, v_id, val, dm.site_id, self.ts),'\n')

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
        if not t_id in self.transaction_table:
            print("Transaction table does not contains {}".format(t_id),'\n')
            return False
        if self.transaction_table[t_id].is_aborted:
            self.abort(t_id, True)
        else:
            self.commit(t_id, self.ts)
            
    def abort(self, t_id, siteFailFlag = False):
        """
        Abort a transaction.
        """
        for dm in self.dm_list:
            dm.abort(t_id)
        del self.transaction_table[t_id]
        if siteFailFlag :
            print("Transaction {} aborts at time stamp {} due to site failure".format(t_id, self.ts),'\n')
        else:
            print("Transaction {} aborts at time stamp {} due to deadlock".format(t_id, self.ts),'\n')

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
        if not self.dm_list[int(site_id) - 1] :
            print("Site {} is already down".format(site_id))
        self.ts += 1
        self.dm_list[int(site_id) - 1].recover(self.ts)
        print("Site {} recovers at time stamp {}".format(site_id, self.ts),'\n')
            
    def fail(self, site_id):
        """
        Fail a site.
        """
        if not self.dm_list[int(site_id) - 1].is_running:
            print("Site {} is already down".format(site_id))
            return
        self.ts += 1
        self.dm_list[int(site_id) - 1].fail(self.ts)    
        print("Site {} fails".format(site_id),'\n')
        
        for k, v in self.transaction_table.items():
            if not v.is_ro and not v.is_aborted and site_id in v.visited_sites:
                v.is_aborted = True
                return
        

        # TODO
        # for t in self.transaction_table.values():
        #     if (not t.is_ro) and (not t.will_abort) and (
        #             site_id in t.sites_accessed):
        #         # not applied to read-only transaction
        #         t.will_abort = True
        #         # print("{} will abort!!!".format(t.transaction_id))
        
    def getTimeStamp(self) :
        """
        Get Time Stamp
        """
        print("Time Stamp :: {}".format(self.ts),'\n')
        return self.ts
    
    def detect_cycle(self, src, dest, graph, visited):
        visited.add(src)
        
        for adj in graph[src]:
            if adj == dest:
                return True
            if adj not in visited:
                if self.detect_cycle(adj, dest, graph, visited):
                    return True
        return False    
    
    def resolve_deadlock(self):
        """
        Detect deadlocks using cycle detection and abort the youngest
        transaction in the cycle.
        :return: True if a deadlock is resolved, False if no deadlock detected
        """
        blocking_graph = defaultdict(set)
        for dm in self.dm_list:
            if dm.is_running:
                graph = dm.generate_blocking_graph()
                for node, adj_list in graph.items():
                    blocking_graph[node].update(adj_list)
        # print(dict(blocking_graph))
        youngest_t_id = None
        youngest_ts = -1
        for node in list(blocking_graph.keys()):
            visited = set()
            if has_cycle(node, node, visited, blocking_graph):
                if self.transaction_table[node].ts > youngest_ts:
                    youngest_t_id = node
                    youngest_ts = self.transaction_table[node].ts
        if youngest_t_id:
            print("Deadlock detected: aborting {}".format(youngest_t_id))
            self.abort(youngest_t_id)
            return True
        return False

    def detect_deadlock(self):
        block_graph = dict(set)
        
        for dm in self.dm_list:
            if dm.is_running:
                g = dm.initialize_block_graph(self.dm_list)
            
                for k, v in g.items():
                    block_graph[k] = v
        
        new_t_id = None
        new_ts = None
        
        for k in list(block_graph.keys()):
            visited = set()
            if self.detect_cycle(k, k, block_graph, visited):
                if self.transaction_table[k].ts > new_ts:
                    new_t_id = k
                    new_ts = self.transaction_table[k].ts
        
        if new_t_id:
            print("Deadlock detected, aborting transaction {}".format(new_t_id),'\n')
            self.abort(new_t_id)
            return True
        return False
    
    def solve_deadlock(self):
        """
        Detect deadlocks using cycle detection and abort the youngest
        transaction in the cycle.
        :return: True if a deadlock is resolved, False if no deadlock detected
        """
        blocking_graph = defaultdict(set)
        for dm in self.dm_list:
            #if dm.is_running and v_id in dm.data_table:
            if dm.is_running:
                graph = dm.generate_blocking_graph()
                for node, adj_list in graph.items():
                    blocking_graph[node].update(adj_list)
        # print(dict(blocking_graph))
        youngest_t_id = None
        youngest_ts = -1
        for node in list(blocking_graph.keys()):
            visited = set()
            if has_cycle(node, node, visited, blocking_graph):
                if self.transaction_table[node].ts > youngest_ts:
                    youngest_t_id = node
                    youngest_ts = self.transaction_table[node].ts
        if youngest_t_id:
            print("Deadlock detected: aborting {}".format(youngest_t_id))
            self.abort(youngest_t_id)
            return True
        return False


def has_cycle(current, root, visited, blocking_graph):
    """Helper function that detects cycle in blocking graph using dfs."""
    visited.add(current)
    for neighbour in blocking_graph[current]:
        if neighbour == root:
            return True
        if neighbour not in visited:
            if has_cycle(neighbour, root, visited, blocking_graph):
                return True
    return False

            