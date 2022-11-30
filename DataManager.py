class Variable(object):
    def __init__(self, v_id: int, replicated: bool):
        self.v_id = v_id
        self.val = int(v_id[1:])*10
        self.commits = {}  # transaction ID: commit timestamp
        self.readable = True
        self.replicated = replicated
        self.fail = False


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
                self.lock_table[v_id] = None
            elif i % 10 + 1 == self.site_id:
                self.data_table[v_id] = Variable(v_id, False)
                self.lock_table[v_id] = None
                
                
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
                
    def read_snapshot(self, v_id: int, ts: int):
        # var = self.data_table[v_id]
        # print(var)
        # if not var is var.readable:
        #     return False
    
        return True

           
        
    def read(self, t_id: int, v_id: int):
        return True
        pass
    
    def write(self, t_id: int, v_id: int, val: int):
        return True
        pass
   
           
    def commit(self, t_id: int):
        pass
    
    def abort(self, t_id: int):
        pass
            
        # else:
        #     var.fail = True
        #     var.val = None