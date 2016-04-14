from Scheduler import Scheduler
class BurstOffScheduler(Scheduler):
    def __init__(self, scheduler_settings, active = False, min_over_limit = 0):
        super(BurstOffScheduler, self).__init__(scheduler_settings, active, min_over_limit)

    # If the waiting memory is under the limit increment counter towards making a decision
    def update(self, memory_in_wait, minutes):
        if memory_in_wait < self.scheduler_settings.scheduler_limit and \
                self.min_over_limit < self.scheduler_settings.scheduler_interval * self.scheduler_settings.scheduler_busy_ratio:
            self.min_over_limit = self.min_over_limit + 1 
        elif self.min_over_limit > 0:
            self.min_over_limit = self.min_over_limit - 1
            
