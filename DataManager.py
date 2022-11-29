class Variable(object):
    def __init__(self, id: int):
        self.id = id
        self.val = id * 10
        self.readable = True


class DataManager:
    def __init__(self, site_id: int):
        self.site_id = site_id
        self.is_running = True
        self.data = {}
        self.lock = {}
        self.fail_ts = []
        self.recover_ts = []
        self.readable = set()
        
        # # Initialize sites
        # for i in range(10):
            

        # Initialize data variables
        for i in range(1, 21):
            if i % 2 == 0 or i % 10 + 1 == self.site_id:
                self.data[i] = Variable(i)
                self.lock[i] = None
                
                
    def dump(self):
        status = "running" if self.is_running else "failed"
        output = f"Site {self.site_id} - {status}"

        for k, v in self.data.items():
            output += f" x{k}: {v.val}"
        
        print(output)
        
    def fail(self, ts):
        self.is_running = False
        self.fail_ts.append(ts)

        for k, v in self.lock.items():
            self.lock[k] = None
    
    def recover(self, ts):
        self.is_running = True
        self.recover_ts.append(ts)

        for k, v in self.data.items():
            if k % 2 != 0:
                self.readable.add()
                