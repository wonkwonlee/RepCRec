class Variable(object):
    def __init__(self, id: int):
        self.id = id
        self.val = id * 10


class SiteManager:
    def __init__(self, site_id: int):
        self.site_id = site_id
        self.is_up = True
        self.data_table = {}
        self.lock_table = {}
        self.fail_list = []
        self.recover_list = []
        
        # Initialize sites
        for i in range(10):
            

        # Initialize data variables
        for i in range(1, 21):
            if i % 2 == 0:
                self.data_table[i] = Variable(i)
            elif  i % 10 + 1 == self.site_id:
                self.data_table[i] = Variable(i)

                
        
        
