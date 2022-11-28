class Variable(object):
    """_summary_

    Args:
        object (_type_): _description_
    """
    
    def __init__(self, id: int):
        self.id = id
        self.val = id * 10
    

class SiteManager:
    def __init__(self, site_id: int):
        self.site_id = site_id
        self.is_up = True
        self.data = {}
        self.lock_table = {}
        self.transaction_table = {}
        
        # Initialize data
        for i in range(1, 21):
            if i % 2 == 0 or i % 10 + 1 == self.site_id:
        
