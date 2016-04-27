import sys
import re
import logging
import functools

import numpy as np
import math

from SchedulerSettings import SchedulerSettings
from HeaderInput import HeaderInput
from BurstOnScheduler import BurstOnScheduler
from BurstOffScheduler import BurstOffScheduler
from RollingStats import RollingStats

INPUT_PERCENTILES = [0, 0.25, 0.5, 0.75, 1.0]

# Defining scheduling deciders
def setup_schedulers():
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

    return burst_deciders, unburst_deciders


def calculate_percentile(N, percent):
    k = (len(N)-1) * percent
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return N[int(k)]
    d0 = N[int(f)] * (c-k)
    d1 = N[int(c)] * (k-f)
    return d0+d1   

# Return for which value the input data has had less then or equal value percentile amount of time for past minutes
# The number of minutes is set by the length of the data array
def get_memory_levels(data, percentiles):
    memory_levels = []
    data_array = np.array(list(data))
    # Good idea to maintaina sorted linked data structure instead of unsorted deque
    sorted_array = np.sort(data_array)
    for percentile in percentiles:
        memory_levels.append( calculate_percentile( sorted_array, percentile) )
    return memory_levels


# All statistics collect in the run
def setup_stats():
    memory_level_function = functools.partial(get_memory_levels, percentiles=INPUT_PERCENTILES)
    rolling_stats = RollingStats()
    # Save percentiles for a window of 10 minutes
    rolling_stats.add_stat('percentiles', memory_level_function, 10)
    
    return rolling_stats

def main(cts_filename, logger=None):
    logger = logger or logging.getLogger(__name__)
    logger.info('Starting the burst processing marking')

    # Number of minutes to activate burst after a scheduling decision is reached
    burst_delay = 20
    # Number of minutes to deactivate burst after a descheduling decision is reached
    unburst_delay = 40

    # Setup scheduling deciders
    burst_deciders, unburst_deciders = setup_schedulers()

    # Setup rolling stat collectors
    rolling_stats = setup_stats()

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
        # Pick out last item of every 'dot' split
        column_names = [head.split('.')[-1] for head in split_header]
        # Create new tab separated header by joining header and removing last new line
        new_header = "\t".join(column_names[:-1])

    # Header for output file
    burst_marked_file = cts_filename + '_burst_marked.tsv'    
    complete_header = new_header + '\t' + 'burst_mode_extended' + '\t' + 'burst_mode' + '\n'
    logger.info('Constructed header: ' + complete_header)

    # Counter for number of minutes that have been processed
    processed_minutes = 0
    empty_minutes = 0
    missing_memory_minutes = 0
    missing_vcore_minutes = 0

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
        first_minute_start = int( series[0].split() [HeaderInput.minute_start] )
        last_minute_start = first_minute_start

        # For simplicity assume First memory capacity is 0. 
        # (problematic if cluster_memory_capacity = NULL for first minute for instance)
        last_memory_capacity = 0
        last_vcore_capacity = 0

        logger.info('Beginning at minute: %d' % first_minute_start)

        # Hacking and assuming input file is ordered by systems
        last_system = ''
        for line in series:

            split_line = line.split()

            # Need for adding the last known cluster_memory and cluster_vcores
            system = split_line[HeaderInput.system]

            minute_start = int( split_line[ HeaderInput.minute_start ] )
            if system != 'dogfood':
                memory_in_wait = float( split_line[ HeaderInput.memory_in_wait ] )
            else:
                memory_in_wait = float( split_line[ HeaderInput.production_memory_in_wait ] )


            if system != last_system:
                last_memory_capacity = 0
                last_vcore_capacity = 0
                last_minute_start = minute_start
                

            # Number of minutes for which nothing is recorded in container_time_series
            # and cluster_resrouce_dim comes after current_minute_start. 
            for current_minute_start in range(last_minute_start + 60, minute_start + 60, 60):
                # If resource_dim has NULL values for the cluster_memory_capacity and/or vcores pad these
                cluster_memory_capacity = split_line[HeaderInput.cluster_memory_capacity]
                cluster_vcore_capacity = split_line[HeaderInput.cluster_vcore_capacity]
                date = split_line[HeaderInput.date]

                # Default to using last known value if missing cluster_resource_dim values
                if cluster_memory_capacity == 'NULL' \
                        or cluster_memory_capacity == '0.0' \
                        or cluster_memory_capacity == '0':
                    split_line[ HeaderInput.cluster_memory_capacity ] = last_memory_capacity
                    missing_memory_minutes = missing_memory_minutes + 1
                else:
                    last_memory_capacity = cluster_memory_capacity

                if cluster_vcore_capacity == 'NULL' \
                        or cluster_vcore_capacity == '0.0' \
                        or cluster_vcore_capacity == '0':
                    split_line[ HeaderInput.cluster_vcore_capacity ] = last_vcore_capacity
                    missing_vcore_minutes = missing_vcore_minutes + 1
                else:
                    last_vcore_capacity = cluster_vcore_capacity

                # Empty minute if traversing more than the first minute_start
                # Should happen very seldomly 
                if current_minute_start != minute_start:
                    empty_minutes = empty_minutes + 1
                    memory_in_wait = 0
                    # Adding minute_start to the data
                    empty_row[HeaderInput.minute_start] = str(current_minute_start)

                    empty_row[HeaderInput.cluster_memory_capacity] = split_line[HeaderInput.cluster_memory_capacity]
                    empty_row[HeaderInput.cluster_vcore_capacity] = split_line[HeaderInput.cluster_vcore_capacity]
                    empty_row[HeaderInput.date] = date
                    empty_row[HeaderInput.system] = system
                    split_line = empty_row

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

                # Updating the stats
                rolling_stats.update_stat('percentiles', memory_in_wait)
                memory_percentiles = rolling_stats.calculate_stat('percentiles')
                if memory_percentiles is None:
                    memory_percentiles = ['NULL']*len(INPUT_PERCENTILES) 

                write_to_file(f, split_line, bursted, burst_mode, memory_percentiles)
                processed_minutes = processed_minutes + 1
            last_minute_start = minute_start
            last_system = system


        logger.info('Processed minutes: %d' % processed_minutes)
        logger.info('Empty minutes: %d' % empty_minutes)
        logger.info('Missing memory minutes: %d' % missing_memory_minutes)
        logger.info('Missing vcore minutes: %d' % missing_vcore_minutes)

def get_delay(bursted, delay_burst, delay_unburst):
    # If not bursted we have to wait for burst to start
    if not bursted:
        return delay_burst
    else:
        return delay_unburst

def write_to_file(f, line, bursted, burst_mode, memory_percentiles):
    memories = [str(m) for m in memory_percentiles]
    line = line + memories + [burst_mode]
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
