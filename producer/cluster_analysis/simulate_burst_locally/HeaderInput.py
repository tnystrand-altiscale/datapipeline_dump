class HeaderInput:
    minute_start = 0
    container_wait_time = 1
    memory = 2
    vcores = 3
    memory_in_wait = 4
    system = 5
    cluster_memory = 6
    cluster_vcores = 7
    date = 8

    @staticmethod
    def empty_row():
        row = []
        row.append( str( HeaderInput.minute_start * 0) )
        row.append( str( HeaderInput.container_wait_time * 0) )
        row.append( str( HeaderInput.memory * 0) )
        row.append( str( HeaderInput.vcores * 0) )
        row.append( str( HeaderInput.memory_in_wait * 0) )
        row.append( str( HeaderInput.system * 0) )
        row.append( str( HeaderInput.cluster_memory * 0) )
        row.append( str( HeaderInput.cluster_vcores * 0) )
        row.append( str( HeaderInput.date * 0) )
        return row

    def __def__(self):
        pass
