for i in range(20):
    print (','.join(['{}']*10)).format(*[i]*10)