import collections;

class RollingStats:
    # Container for stats with a rolling window
    # Stats are contained in a dictionary
    # Each new datapoint is stored in FIFO order in the deque queue
    # Each stat have an associated 'group' function
    def __init__(self):
        self.stats = {}
        self.stats_meta = {}

    def add_stat(self, name, function, window):
        self.stats[name] = collections.deque([], window)
        self.stats_meta[name] = {'function' : function,
                                 'window' : window,
                                 'num_processed' : 0}
            

    def get_collected_stats(self):
        return self.stats.keys()

    def update_stat(self, name, value):
        self.stats[name].append(value)

    def calculate_stat(self, name):
        if len(self.stats[name]) == self.stats_meta[name]['window']:
            result = self.stats_meta[name]['function'] ( self.stats[name] )
        else:
            result = None
        return result

