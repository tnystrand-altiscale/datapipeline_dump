class SchedulerSettings:
    # Number of minutes to check before a burst decision is made and how many of these needs a certain busy level
    # The scheulder also have an active setting (to determine whether it is currently switched on or off)
    def __init__(self, scheduler_interval, scheduler_busy_ratio, scheduler_limit, active=False):
        self.scheduler_interval = scheduler_interval
        self.scheduler_busy_ratio = scheduler_busy_ratio
        self.scheduler_limit = scheduler_limit
        self.active = active
