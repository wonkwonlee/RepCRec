class Variable(object):
    def __init__(self, v_id: int):
        self.v_id = v_id
        self.val = v_id * 10
        self.commits = {}  # transaction ID: commit timestamp
        self.readable = True
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
            if i % 2 == 0 or i % 10 + 1 == self.site_id:
                self.data_table[i] = Variable(i)
                self.lock_table[i] = None
                
                
    def dump(self):
        status = "running" if self.is_running else "failed"
        output = f"Site {self.site_id} - {status}"

        for k, v in self.data_table.items():
            output += f" x{k}: {v.val}"
        print(output)
        
    def fail(self, ts):
        self.is_running = False
        self.fail_ts.append(ts)

        for k, v in self.lock_table.items():
            self.lock_table[k] = None
    
    def recover(self, ts):
        self.is_running = True
        self.recover_ts.append(ts)

        for k, v in self.data_table.items():
            if k % 2 != 0:
                self.readable.add(k)
                
    def read_snapshot(self, v_id: int, ts: int):
        var = self.data_table[v_id]
        
        if not var.readable:
            return
        else:
            for site in self.fail_ts:
                if site < ts:
                    return var.val
           
    def commit(self, t_id: int, ts: int):
        pass
    
    def abort(self, t_id: int):
        pass
            
        # else:
        #     var.fail = True
        #     var.val = None