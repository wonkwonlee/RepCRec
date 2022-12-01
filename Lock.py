from DataManager import DataManager, Variable

class Lock(object):
    def __init__(self, type: str, t_id: int, site_id: int, var: Variable):
        """
        Initialize a Lock object.
        :param type: lock type (read/write)
        :param t_id: transaction id
        :param site_id: site id
        """
        self.type = type
        self.t_id = t_id
        self.site_id = site_id
        self.var = var
    