#!/opt/python-2.7/bin/python
import os, re
import threading
from elasticsearch import Elasticsearch
import time
import urllib, json
#POST, PUT json data
import requests

def analyzer(id,ip,agent,discontinuity,dropframe,index,elasticsearch):
        result = es.search(
                index='%s' % (index),
                size=1000,
                body={
                        'query': {
                                'filtered': {
                                        'query': {
                                                'match': {"message":'%s' % (ip)}
                                                },
                                        'filter': {
                                                'and' : [
                                                {
                                                        'range': {
                                                                '@timestamp': {
                                                                        'gt': 'now-1m',
                                                                        'lt': 'now'
                                                                }
                                                        }
                                                },{
                                                        'prefix': {"host":'%s' % (agent)}
                                                },{
                                                        'prefix': {"message":'Detected discontinuity'}
                                                }  ]
                                        }
                                }
                        }
                } )
#       print result['hits']['total']
        if result['hits']['total']!=0:
                for i in range(0, result['hits']['total']):
                        discontinuity_new+=int(re.search('(?<=skips:)\d+',result['hits']['hits'][i]['_source']['message']).group(0))
        if discontinuity_new != discontinuity:
                try:
                        requests.put(api+"profile_agent/"+str(id)+"/", json={"discontinuity": discontinuity_new})
                except requests.exceptions.RequestException:
                        print "can't connect API!"

                        configfile='/opt/monitor/config.py'
if os.path.exists(configfile):
        execfile(configfile)
else:
        print "can't read file config";
        exit(1)
es = Elasticsearch(elastic_server,timeout=3.5)
localtime = time.gmtime()
date_now=time.strftime("%Y.%m.%d",localtime)
index='logstash-%s' % (date_now)

response = urllib.urlopen(api+"profile_agent/analyzer/")
if response.getcode()==200:
    print "200"
    profile_agents = json.loads(response.read())
    for profile_agent in profile_agents['profile_analyzer']:
        while threading.activeCount() > 20:
            time.sleep(1)
        t = threading.Thread(target=analyzer, args=(profile_agent['id'],profile_agent['ip'].split(':30120')[0],profile_agent['agent_ip'],profile_agent['discontinuity'],profile_agent['dropframe'],index,es,))
        t.start()
time.sleep(20)
