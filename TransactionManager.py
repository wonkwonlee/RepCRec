"""
Due on Saturday, 12/03/2022

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
        
    def init_graph(self, dm_list: list[DataManager]):
        """
        Initialize a blocking graph for deadlock detection.

        Args:
            dm_list (list[DataManager]): List of data managers

        Returns:
            graph (dict): Blocking graph
        """
        graph = defaultdict(set)
        for dm in [x for x in dm_list if x.is_running]:
            g = dm.init_block_graph()
            for k, v in g.items():
                graph[k].update(v)
        return graph 

    def is_cyclic(self, src: int, dest: int, visited: defaultdict, graph: defaultdict):
        """
        Check if there is a cycle in the blocking graph using DFS algorithm.
    
        Args:
            src (int): Source node transaction ID
            dest (int): Destination node transaction ID
            visited (defaultdict): Visited nodes
            graph (defaultdict): Blocking graph

        Returns:
            bool: True if there is a cycle, False otherwise
        """
        visited[src] = True
        for node in graph[src]:
            if node == dest:
                return True
            if not visited[node]:
                if self.is_cyclic(node, dest, visited, graph):
                    return True
        return False

    def find_cycle(self, table: dict, graph: defaultdict):
        """
        Detect the cycle in the blocking graph and return the youngest transaction ID.

        Args:
            table (dict): Transaction table
            graph (defaultdict): Blocking graph

        Returns:
            int: Youngest transaction ID
        """
        t_ts = -1
        t_id = None
        
        for k, v in list(graph.items()):
            visited = defaultdict(bool)
            if self.is_cyclic(k, k, visited, graph):
                if table[k].ts > t_ts:
                    ts = table[k].ts
                t_id = k
        return t_id

    def check_deadlock(self):
        """
        Check if there is a deadlock by detecting a cycle in the blocking graph.

        Returns:
            bool: True if there is a deadlock, False otherwise
        """
        graph = self.init_graph(self.dm_list)
        cyclic = self.find_cycle(self.transaction_table, graph)
        if cyclic is None:
            return False
        else:
            print("Deadlock detected. Abort transaction {}".format(cyclic))
            self.abort(cyclic)
            return True
