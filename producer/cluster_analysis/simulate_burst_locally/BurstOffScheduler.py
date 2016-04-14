import Scheduler
class BurstOffScheduler(Scheduler):
    def __init__(self, scheduler_settings, active = False, min_over_limit = 0):
        super(BurstOffScheduler, self).__init__()

    def fulfill_requirements(self):
        return self.scheduler_settings.scheduler_limit <= self.scheduler_settings.scheduler_busy_ratio*self.min_over_limit:
