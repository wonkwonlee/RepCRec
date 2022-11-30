import DataManager

class LockManager:
     def __init__(self, v_id):
        """
        Initialize a LockManager instance.
        :param variable_id: variable's id for a lock manager
        """
        self.v_id = v_id
        self.current_lock = None
        self.list = []  
    