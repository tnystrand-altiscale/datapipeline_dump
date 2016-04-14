import sys
import logging

from SchedulerSettings import SchedulerSettings
from HeaderInput import HeaderInput

def main(cts_filename, logger=None):
    logger = logger or logging.getLogger(__name__)
    logger.info('Starting the burst processing marking')

    # Number of minutes to activate burst after a scheduling decision is reached
    burst_delay = 20
    # Number of minutes to deactivate burst after a descheduling decision is reached
    burst_delay = 0

    burst_deciders = []
    unburst_deciders = []
    
    schedulerset_1 = SchedulerSettings(10, 1.0, 2.5)
    schedulerset_2 = SchedulerSettings(60, 0.75, 2.5)

    de_schedulerset_1 = SchedulerSettings(10, 1.0, 2.5)
    de_schedulerset_2 = SchedulerSettings(60, 0.75, 2.5)
   
    scheduler_1 = BurstScheduler(schedulerset_1, False, 0)
    scheduler_2 = BurstScheduler(schedulerset_2, False, 0)

    de_scheduler_1 = BurstScheduler(de_schedulerset_1, False, 0)
    de_scheduler_2 = BurstScheduler(de_schedulerset_2, False, 0)

    burst_deciders.append(scheduler_1)
    burst_deciders.append(scheduler_2)
    unburst_deciders.append(descheduler_1)
    unburst_deciders.append(descheduler_2)

    # Cluster starts non-bursted
    bursted = False

    # Read the file with memory per minute
    logger.info('Reading file ' + cts_filename)
    with open(cts_filename) as f:
        header = f.readline()
        series = f.readlines()

    if "." in header:
        split_header = re.split('\t|\n', header)
        # Pick out last item of ever 'dot' split
        column_names = [head.split('.')[-1] for head in split_header]

        new_header = "\t".join(column_names)

    # Counter for number of minutes that have been processed
    processed_minutes = 0

    burst_marked_file = cts_filename + '.burst_marked.tsv'    
    coplete_header = new_header + '\t' + 'bursted' + '\n'
    logger.info('Writing to file ' + burst_marked_file)
    with open(burst_marked_file, 'w') as f:
        f.write(header)
        # Pretend that the last known minute_start was 0 minute before (do not pad anything)
        minute_diff = 0
        last_minute_start = int( series[0].split() [HeaderInput.minute_start] )

        for line in series:
            turn_on_burst = False
            turn_off_burst = False

            split_line = line.split()

            minute_start = int( split_line[ HeaderInput.minute_start ] )
            memory_in_wait = int( split_line[ HeaderInput.memory_in_wait ] )

            # Number of minutes for which nothing is recorded in container_time_series
            for empty_minute_start in range(last_minute_start, minute_start - 60, 60):
                update_all_schedulers(burst_deciders, unburst_deciders, 0)
            update_all_schedulers(burst_deciders, unburst_deciders, memory_in_wait)

            last_minute_start = minute_start

def update_all_schedulers(burst_deciders, unburst_deciders, memory_in_wait):
   # Check all bursting schedulers
   for scheduler in burst_deciders:
       scheduler.update(memory_in_wait, 1) # Update for the last entry
       if scheduler.fulfill_requirements():
           turn_on_burst = True

   # Check all unbursting schedulers
   for scheduler in unburst_deciders:
       scheduler.update(memory_in_wait, 1) # Update for the last entry
       if scheduler.fulfill_requirements():
           turn_off_burst = True

            



if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    cts_filename = sys.argv[1]
    main(cts_filename)
