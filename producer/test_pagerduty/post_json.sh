curl -H "Content-type: application/json" -X POST \
    -d '{ 
      "service_key": "b6f873ecdce34bba9440b825a0cadec3",
      "incident_key": "test_trigger_2",
      "event_type": "trigger",
      "description": "Test FAILURE",
      "details": {
        "full_log": "mt 7) (2016-02-28) (2016-02-29) \n geargaergaebraerbaebraerb \n aergaergaergaegraerg \n aergaergaerg mr aergaerg"
        }
      }' \
    "https://events.pagerduty.com/generic/2010-04-15/create_event.json"
