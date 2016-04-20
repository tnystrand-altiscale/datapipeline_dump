import abc

class Scheduler(object):
    # Contains the current settings, whether this scheudler is active and how many miutes is over the limit for a burst decision
    # 'counter for number of minutes that are 'burst-worthy''
    def __init__(self, scheduler_settings, active, min_over_limit):
        self.scheduler_settings = scheduler_settings
        self.active = active
        self.min_over_limit = min_over_limit

    # If the requirements are fulfilled (e.g enough waiting mem), schduler makes a decision
    def fulfill_requirements(self):
        return self.scheduler_settings.scheduler_interval <= \
            self.scheduler_settings.scheduler_busy_ratio*self.min_over_limit

    # If the waiting memory is over the limit increment counter towards making a decision
    @abc.abstractmethod
    def update(self, memory_in_wait, minutes):
        return
            
    def add_for_decision(self, minutes):
        if minutes + self.min_over_limit < self.settings.scheduler_limit:
            self.min_over_limit = minutes + self.min_over_limit
        else:
            self.min_over_limit = self.settings.scheduler_limit

    def sub_for_decision(self, minutes):
        if minutes - self.min_over_limit > 0:
            self.min_over_limit = minutes - self.min_over_limit
        else:
            self.min_over_limit = 0
