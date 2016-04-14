import sys
import re
import logging

from SchedulerSettings import SchedulerSettings
from HeaderInput import HeaderInput
from BurstOnScheduler import BurstOnScheduler
from BurstOffScheduler import BurstOffScheduler

def main(cts_filename, logger=None):
    logger = logger or logging.getLogger(__name__)
    logger.info('Starting the burst processing marking')

    # Number of minutes to activate burst after a scheduling decision is reached
    burst_delay = 20
    # Number of minutes to deactivate burst after a descheduling decision is reached
    unburst_delay = 20

    # Scheduling deciders
    burst_deciders = []
    unburst_deciders = []
    
    schedulerset_1 = SchedulerSettings(10, 1.0, 2.5)
    schedulerset_2 = SchedulerSettings(60, 0.75, 2.5)

    de_schedulerset_1 = SchedulerSettings(10, 1.0, 2.5)
    de_schedulerset_2 = SchedulerSettings(60, 0.75, 2.5)
   
    scheduler_1 = BurstOnScheduler(schedulerset_1, False, 0)
    scheduler_2 = BurstOnScheduler(schedulerset_2, False, 0)

    de_scheduler_1 = BurstOffScheduler(de_schedulerset_1, False, 0)
    de_scheduler_2 = BurstOffScheduler(de_schedulerset_2, False, 0)

    burst_deciders.append(scheduler_1)
    burst_deciders.append(scheduler_2)
    unburst_deciders.append(de_scheduler_1)
    unburst_deciders.append(de_scheduler_2)

    # Cluster starts non-bursted
    bursted = False
    modes = ['normal','bursted']
    burst_mode = modes[bursted]


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

    # Header for output file
    burst_marked_file = cts_filename + '.burst_marked.tsv'    
    complete_header = new_header + '\t' + 'burst_mode_extended' + '\t' + 'burst_mode' + '\n'
    logger.info('Constructed header: ' + complete_header)

    # Counter for number of minutes that have been processed
    processed_minutes = 0
    empty_minutes = 0

    # Empty row to be reused for minutes without entries in container_time_series
    empty_row = HeaderInput.empty_row()

    # Counters for burst delays
    minutes_of_delay_left = 0
    # Burst cannot change while having lock
    lock_burst = False 

    logger.info('Writing to file: ' + burst_marked_file)
    with open(burst_marked_file, 'w') as f:
        f.write(complete_header)
        # Pretend that the last known minute_start was 0 minute before (do not pad anything)
        minute_diff = 0
        last_minute_start = int( series[0].split() [HeaderInput.minute_start] )

        logger.info('Beginning at minute: %d' % last_minute_start)

        for line in series:

            split_line = line.split()

            minute_start = int( split_line[ HeaderInput.minute_start ] )
            memory_in_wait = float( split_line[ HeaderInput.memory_in_wait ] )

            # Need for adding the last known cluster_memory and cluster_vcores
            cluster_memory = series[0].split() [HeaderInput.cluster_memory]
            cluster_vcores = series[0].split() [HeaderInput.cluster_vcores]
            system = series[0].split() [HeaderInput.system]
            date = series[0].split() [HeaderInput.date]

            # Number of minutes for which nothing is recorded in container_time_series
            # up till current minute
            for empty_minute_start in range(last_minute_start + 60, minute_start, 60):
                # If waiting for burst to change, we cannot change burst status
                if not lock_burst:
                    turn_on_burst, turn_off_burst = update_all_schedulers(burst_deciders, unburst_deciders, 0)
                    change_burst = swap_burst(turn_on_burst, turn_off_burst, bursted)
                    if change_burst:
                        lock_burst = True
                        burst_mode = 'changing'
                        minutes_of_delay_left = get_delay(bursted, burst_delay, unburst_delay)
                # When no minutes left of burst, release burst mode changes, and lock is released
                elif minutes_of_delay_left == 0:
                    bursted = not bursted
                    burst_mode = modes[bursted]
                    lock_burst = False
                else:
                    minutes_of_delay_left = minutes_of_delay_left - 1
                
                # Adding minute_start to the data
                empty_row[HeaderInput.minute_start] = str(empty_minute_start)

                empty_row[HeaderInput.cluster_memory] = cluster_memory
                empty_row[HeaderInput.cluster_vcores] = cluster_vcores
                empty_row[HeaderInput.date] = date
                empty_row[HeaderInput.system] = system

                write_to_file(f, empty_row, bursted, burst_mode)
                processed_minutes = processed_minutes + 1
                empty_minutes = empty_minutes + 1

            # Update the last non-empty 'current' minute
            # If waiting for burst to change, we cannot change burst status
            if not lock_burst:
                turn_on_burst, turn_off_burst = update_all_schedulers(burst_deciders, unburst_deciders, memory_in_wait)
                change_burst = swap_burst(turn_on_burst, turn_off_burst, bursted)
                if change_burst:
                    lock_burst = True
                    burst_mode = 'changing'
                    minutes_of_delay_left = get_delay(bursted, burst_delay, unburst_delay)
            # When no minutes left of burst, release burst mode changes, and lock is released
            elif minutes_of_delay_left == 0:
                bursted = not bursted
                burst_mode = modes[bursted]
                lock_burst = False
            else:
                minutes_of_delay_left = minutes_of_delay_left - 1

            write_to_file(f, split_line, bursted, burst_mode)

            last_minute_start = minute_start
            processed_minutes = processed_minutes + 1

        logger.info('Empty minutes: %d' % empty_minutes)
        logger.info('Processed minutes: %d' % processed_minutes)

def get_delay(bursted, delay_burst, delay_unburst):
    # If not bursted we have to wait for burst to start
    if not bursted:
        return delay_burst
    else:
        return delay_unburst

def write_to_file(f, line, bursted, burst_mode):
    line = line + [burst_mode]
    if bursted:
        f.write('\t'.join( line + ['bursted\n']))
    else:
        f.write('\t'.join( line + ['normal\n']))

def update_all_schedulers(burst_deciders, unburst_deciders, memory_in_wait):
    turn_on_burst = False
    turn_off_burst = False
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

    return turn_on_burst, turn_off_burst


def swap_burst(turn_on_burst, turn_off_burst, bursted):
    switch = False
    # If no conflicting switches are made
    if turn_on_burst and not turn_off_burst and not bursted:
        switch = True
    elif not turn_on_burst and turn_off_burst and bursted:
        switch = True
    # If conflict and both turns are true: switch
    elif turn_on_burst and turn_off_burst and not bursted:
        switch = True
    elif turn_on_burst and turn_off_burst and bursted:
        switch = True
    return switch
        
            



if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    cts_filename = sys.argv[1]
    main(cts_filename)
