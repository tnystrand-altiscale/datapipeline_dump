import json
import re
import logging
import traceback
import urllib
import urllib2


class PagerDutyAlert:

    def __init__(self, logger=None):
        logger = logger or logging.getLogger(__name__)
        self.api_access_key = None
        self.service_key = None
        self.logger = logger

    def get_access_key(self, path):
        """ Looks for pagerduty access key and service key in path, which should be json file"""
        key = None
        try:
            f = open(path)
        except IOError:
            self.logger.error("Could not locate file with key at '%s'" % path)
            raise
        else:
            self.logger.info("Found file with key at '%s'" % path)
            with f:
                keys = json.load(f)

        try:
            self.api_access_key = keys['api-access-key']
        except KeyError:
            self.logger.error("Pagerduty access key not found, attempting to run without")
        else:
            self.logger.info("Pagerduty access key found")

        try:
            self.service_key = keys['service-key']
        except KeyError:
            self.logger.error("Pagerduty service key not found, attempting to run without")
        else:
            self.logger.info("Pagerduty service key found")
   
 
    def trigger_incident(self, incident_key, description, message):
        """ Trigger a pagerduty incident with a POST command"""
        url = 'https://events.pagerduty.com/generic/2010-04-15/create_event.json'

        headers = {
            'Authorization': 'Token token={0}'.format(self.api_access_key),
            'Content-type': 'application/json'
        }

        data = json.dumps({
            "service_key": self.service_key,
            "incident_key": incident_key,
            "event_type": "trigger",
            "description": description,
            "details": {
                "Full message": message
            }
        })

        self.logger.info('url > %s' % url)

        req = urllib2.Request(url, data, headers)
        response = urllib2.urlopen(req)
        print response.read() 


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    alert = PagerDutyAlert()
    alert.get_access_key('/home/tnystrand/.pgr_apikey.json')
    incident_key = 'jhist'
    description = 'jhist monitor failure'
    message = 'longmessage\n'*30
    alert.trigger_incident(incident_key, description, message)
