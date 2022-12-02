"""
Created on Friday, 2022-12-02

Author: Wonkwon Lee, Young Il Kim

"""
from Config import *
from DataManager import DataManager
from collections import defaultdict

class TransactionManager(object):
    """
    Transaction manager is responsible for managing transactions.
    Each transaction manager has a list of data managers, transaction table, and operation list.
    Data manager list is initialized with 10 data managers.
    """
    transaction_table = {}  # Transaction table
    operation_list = []     # List of read/write operations
    ts = 0                  # Time stamp
    
    def __init__(self):
        """
        Initialize the transaction manager with 10 data managers.
        """    
        self.dm_list = []
        for dm in range(1, 11):
            self.dm_list.append(DataManager(dm))
    
    def read_operation(self, t_id: int, v_id: int):
        """
        Add read operation to the operation list.
        
        Args:
            t_id (int): Transaction id
            v_id (int): Variable id
        """
        if not t_id in self.transaction_table:
            print("Transaction table does not contains {}".format(t_id),'\n')
            return
        self.ts += 1
        self.operation_list.append(Operation('R', t_id, v_id, None))
    
    def write_operation(self, t_id: int, v_id: int, val: int):
        """
        Add write operation to the operation list.
        
        Args:
            t_id (int): Transaction id
            v_id (int): Variable id
            val (int): Value to write
        """
        if not t_id in self.transaction_table:
            print("Transaction table does not contains {}".format(t_id),'\n')
            return
        self.ts += 1
        self.operation_list.append(Operation('W', t_id, v_id, val))
    
    def run_operation(self):
        """
        Run operations in the operation list in order. 
        If the operation is read, read the variable from the transaction. 
        If the operation is write, write the variable to the transaction.
        """
        for op in self.operation_list:
            if not op.t_id in self.transaction_table:
                # print("Transaction id {} not in table".format(operation.t_id))
                self.operation_list.remove(op)
            else:
                result = False
                if op.op == 'R':    # Read operation
                    # print("================ TM :: RUN_OPERATION ================")
                    # print("self.transaction_table[{}].is_ro :: {}".format(operation.t_id,self.transaction_table[operation.t_id].is_ro))
                    if self.transaction_table[op.t_id].is_ro:
                        result = self.read_snapshot(op.t_id, op.v_id)
                    else:
                        result = self.read(op.t_id, op.v_id)
                elif op.op == 'W': # Write operation
                    result = self.write(op.t_id, op.v_id, op.val)              
                else: 
                    print("Invalid operation")
                if result:   
                    self.operation_list.remove(op)
    
    def begin(self, t_id: int):
        """
        Begin a new read-write transaction.
        
        Args:
            t_id (int): Transaction id
        """
        self.ts += 1
        self.transaction_table[t_id] = Transaction(t_id, self.ts, is_ro=False)
        print("Transaction {} begins at time stamp {}".format(t_id, self.ts),'\n')
    
    def beginRO(self, t_id: int):
        """
        Begin a new read-only transaction.
        
        Args:
            t_id (int): Transaction id
        """
        self.ts += 1
        self.transaction_table[t_id] = Transaction(t_id, self.ts, is_ro=True)
        print("Read-only transaction {} begins at time stamp {}".format(t_id, self.ts),'\n')
    
    def read_snapshot(self, t_id: int, v_id: int):
        """
        Read a variable from the snapshot of a read-only transaction.
        
        Args:
            t_id (int): Transaction id
            v_id (int): Variable id
            
        Returns:
            bool: True if the read snapshot operation succeeds, False otherwise
        """
        if not t_id in self.transaction_table:
            print("Transaction {} aborts".format(t_id), '\n')
            return False
        for dm in self.dm_list:
            if dm.is_running and v_id in dm.data_table:
                # print("================ TM :: READ_SNAPSHOT ================")
                # print("v_id :: {}".format(v_id))
                result = dm.read_snapshot(v_id, self.ts)
                if result:
                    self.transaction_table[t_id].visited_sites.append(dm.site_id)
                    print("Transaction {} reads variable {} of {} on site {} at time stamp {}"
                          .format(t_id, v_id, dm.data_table[v_id].val_list[0].val, dm.site_id, self.ts),'\n')
                    return True
        return False
          
    def read(self, t_id: int, v_id: int):
        """
        Read a variable from a read-write transaction.
        
        Args:
            t_id (int): Transaction id
            v_id (int): Variable id
            
        Returns:
            bool: True if the read operation succeeds, False otherwise
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
                    print("Transaction {} reads variable {} of {} on site {} at time stamp {}"
                          .format(t_id, v_id, dm.data_table[v_id].val_list[0].val, dm.site_id, self.ts), '\n')
                    return True
        return False
    
    def write(self, t_id: int, v_id: int, val: int):
        """
        Write a variable from a read-write transaction. 

        Args:
            t_id (int): Transaction id
            v_id (int): Variable id
            val (int): Value to write
        
        Returns:
            bool: True if the write operation succeeds, False otherwise
        """
        sites = []
        if not t_id in self.transaction_table:
            print("Transaction {} aborts".format(t_id), '\n')
            return False
        
        for dm in [site for site in self.dm_list if site.is_running and site.data_table.get(v_id)]:   
            # Check if site is running and variable is in the data table
            wlock = dm.acquire_wlock(t_id, v_id)
            if not wlock:
                print("Write lock conflict found. Transaction {} is waiting.".format(t_id), '\n')
                return False
            sites.append(dm.site_id)
        if not sites:
            return False
        for site_id in sites:
            dm = self.dm_list[site_id - 1]
            dm.write(t_id, v_id, val)
            # print(target_site.data_table[v_id])
            # print(target_site.data_table[v_id].val)
            self.transaction_table[t_id].visited_sites.append(site_id)
        print("Transaction {} writes variable {} with value {} to sites {} at time stamp {}."
              .format(t_id, v_id, val, sites, self.ts), '\n')
        return True
        
    def dump(self):
        """
        Dump the state of all sites.
        """
        for dm in self.dm_list:
            dm.dump()
                
    def end(self, t_id: int):
        """
        End a transaction and abort or commit it.
        
        Arg:
            t_id (int): Transaction id
        """
        self.ts += 1
        if not t_id in self.transaction_table:
            print("Transaction table does not contains {}".format(t_id),'\n')
            return
        if self.transaction_table[t_id].is_aborted:
            self.abort(t_id, True)
        else:
            self.commit(t_id, self.ts)
            
    def abort(self, t_id: int, fail=False):
        """
        Abort a transaction.
        
        Args:
            t_id (int): Transaction id
            fail (bool): True if the site is failed, False otherwise
        """
        for dm in self.dm_list:
            dm.abort(t_id)
        del self.transaction_table[t_id]
        if fail:
            print("Transaction {} aborts due to site failure at time stamp {} ".format(t_id, self.ts),'\n')
        else:
            print("Transaction {} aborts due to deadlock at time stamp {} ".format(t_id, self.ts),'\n')

    def commit(self, t_id: int, ts: int):
        """
        Commit a transaction.
        
        Args:
            t_id (int): Transaction id
            ts (int): Time stamp
        """
        for dm in self.dm_list:
            dm.commit(t_id, ts)
        del self.transaction_table[t_id]
        print("Transaction {} commits at time stamp {}".format(t_id, self.ts),'\n')
        
    def recover(self, site_id: str):
        """
        Recover a site.
        
        Args:
            site_id (str): Site id
        """
        if not self.dm_list[int(site_id) - 1]:
            print("Site {} is already down".format(site_id))
        self.ts += 1
        self.dm_list[int(site_id) - 1].recover(self.ts)
        print("Site {} recovers at time stamp {}".format(site_id, self.ts),'\n')
            
    def fail(self, site_id: str):
        """
        Fail a site.
        
        Args:
            site_id (str): Site id
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
    

###################################################################################################
######################################## TODO #####################################################
###################################################################################################           
    def detect_deadlock(self):
        """Detect if there is a deadlock among existing transactions.
        """
        blocking_graph = self.get_block_graph(self.dm_list)
        victim = self.detect(self.transaction_table, blocking_graph)
        if victim is not None:
            print("Found deadlock, aborts the youngest transaction {}".format(victim))
            self.abort(victim)
            return True
        return False


    def get_block_graph(self, sites):
        """
        Collect blocking information from all up sites, and generate
        a complete blocking graph for all the existing transactions.
        """
        blocking_graph = defaultdict(set)
        for site in [x for x in sites if x.is_running]:
            graph = site.initialize_block_graph()
            for tid, adjacent_tid_set in graph.items():
                blocking_graph[tid].update(adjacent_tid_set)
        return blocking_graph


    def has_cycle(self, start, end, visited, blocking_graph):
        """Use DFS to judge if there is a cycle in the blocking graph.
        Principle:
            For all the arcs that starts from a node, if this node's parent
            existed as the end of an arc, then there is a cycle in the graph.
        """
        visited[start] = True
        for adjacent_tid in blocking_graph[start]:
            if adjacent_tid == end:
                return True
            if not visited[adjacent_tid]:
                if self.has_cycle(adjacent_tid, end, visited, blocking_graph):
                    return True
        return False


    def detect(self, transaction_table, blocking_graph):
        """
        Find out if there is a cycle in the blocking graph. If so, then there exists
        a deadlock, and this function will return the youngest transaction id. Otherwise,
        return nothing.
        """
        victim_timestamp = float('-inf')
        victim_tid = None
        # To avoid `RuntimeError: dictionary changed size during iteration`,
        # we should use list() function or .copy() method.
        for tid in list(blocking_graph.keys()):
            visited = defaultdict(bool)
            if self.has_cycle(tid, tid, visited, blocking_graph):
                if transaction_table[tid].ts > victim_timestamp:
                    victim_timestamp = transaction_table[tid].ts
                victim_tid = tid
        return victim_tid


    # def detect_cycle(self, src, dest, graph, visited):
    #     visited.add(src)
        
    #     for adj in graph[src]:
    #         if adj == dest:
    #             return True
    #         if adj not in visited:
    #             if self.detect_cycle(adj, dest, graph, visited):
    #                 return True
    #     return False    
    
    # def resolve_deadlock(self):
    #     """
    #     Detect deadlocks using cycle detection and abort the youngest
    #     transaction in the cycle.
    #     :return: True if a deadlock is resolved, False if no deadlock detected
    #     """
    #     blocking_graph = defaultdict(set)
    #     for dm in self.dm_list:
    #         if dm.is_running:
    #             graph = dm.initialize_block_graph()
    #             for node, adj_list in graph.items():
    #                 blocking_graph[node].update(adj_list)
    #     # print(dict(blocking_graph))
    #     youngest_t_id = None
    #     youngest_ts = -1
    #     for node in list(blocking_graph.keys()):
    #         visited = set()
    #         if has_cycle(node, node, visited, blocking_graph):
    #             if self.transaction_table[node].ts > youngest_ts:
    #                 youngest_t_id = node
    #                 youngest_ts = self.transaction_table[node].ts
    #     if youngest_t_id:
    #         print("Deadlock detected: aborting {}".format(youngest_t_id))
    #         self.abort(youngest_t_id)
    #         return True
    #     return False
