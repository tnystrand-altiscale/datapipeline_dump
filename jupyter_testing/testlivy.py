import json, pprint, requests, textwrap
host = 'http://localhost:8998'
data = {'kind': 'spark'}
headers = {'Content-Type': 'application/json'}
#r = requests.post(host + '/sessions', data=json.dumps(data), headers=headers)
#print r.json()
#print r.headers
#session_url = host + r.headers['location']
#print session_url
#r = requests.get(session_url, headers=headers)
#print r.json()

session_urls = [host + '/sessions/' + str(i) for i in range(16)]
for sess in session_urls:
    r = requests.get(sess, headers=headers)
    rstr = r.json()
    if 'not found' in rstr:
        print(rstr)
    else:
        rstr['log'] = ''
        print(rstr)
#{u'state': u'starting', u'id': 0, u'kind': u'spark'}
