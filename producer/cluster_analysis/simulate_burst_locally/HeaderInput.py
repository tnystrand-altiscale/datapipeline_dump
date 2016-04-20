class HeaderInput:
    minute_start = 0
    memory = 1
    vcores = 2
    container_wait_time = 3
    memory_in_wait = 4
    production_memory_in_wait = 5

    num_running_containers = 6
    num_waiting_containers = 7

    cluster_memory_capacity = 8
    cluster_vcore_capacity = 9

    system = 10
    date = 11

    @staticmethod
    def empty_row():
        row = []
        # Structured for this for clarity
        # Can also just make an empty str list of 0's of
        # the same length as local varaibles
        row.append( str( HeaderInput.minute_start * 0) )
        row.append( str( HeaderInput.memory * 0) )
        row.append( str( HeaderInput.vcores * 0) )
        row.append( str( HeaderInput.container_wait_time * 0) )
        row.append( str( HeaderInput.memory_in_wait * 0) )
        row.append( str( HeaderInput.production_memory_in_wait * 0) )

        row.append( str( HeaderInput.num_running_containers * 0) )
        row.append( str( HeaderInput.num_waiting_containers * 0) )

        row.append( str( HeaderInput.cluster_memory_capacity * 0) )
        row.append( str( HeaderInput.cluster_vcore_capacity * 0) )

        row.append( str( HeaderInput.system * 0) )
        row.append( str( HeaderInput.date * 0) )
        return row

    def __def__(self):
        pass
