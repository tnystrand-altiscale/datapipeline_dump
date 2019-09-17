import pyhs2
 
with pyhs2.connect(host='hiveserver-dogfood.s3s.altiscale.com:',
                   port=10000,
                   authMechanism="PLAIN",
                   user='tnystrand',
                   database='default') as conn:
    with conn.cursor() as cur:
        #Show databases
        print cur.getDatabases()
 
        #Execute query
        cur.execute("show databases")
 
        #Return column info from query
        print cur.getSchema()
 
        #Fetch table results
        for i in cur.fetch():
            print i
