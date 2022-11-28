class SiteManager:
    def __init__(self, site_id: int):
        self.site_id = site_id
        self.is_up = True
        self.data = {}
        self.lock_table = {}
        self.transaction_table = {}
        
        for i in range(1, 21):
            v_id = "x" + str(i)
            if i % 2 == 0:
                # self.data[i] = self.
        
    