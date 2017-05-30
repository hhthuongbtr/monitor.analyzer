#!/opt/python-2.7/bin/python
import os, re
import threading
from elasticsearch import Elasticsearch
import time
import urllib, json
#POST, PUT json data
import requests

def cc_get(ip,agent,index,elasticsearch,time_to,time_from):
        discontinuity_new=0
        result = elasticsearch.search(
                index='%s' % (index),
                size="1000",
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
                                                                        'gt': '%s' % (time_to),
                                                                        'lt': '%s' % (time_from)
                                                                }
                                                        }
                                                },{
                                                        'prefix': {"host":'%s' % (agent)}
                                                },{
                                                        'prefix': {"message":'discontinuity'}
                                                }  ]
                                        }
                                }
                        }
                } )
        if result['hits']['total']!=0:
                for i in range(0, result['hits']['total']):
#               temp=re.search('(?<=skips:)\d+',result['hits']['hits'][i]['_source']['message'])
                        discontinuity_new+=int(re.search('(?<=skips:)\d+',result['hits']['hits'][i]['_source']['message']).group(0))
        return discontinuity_new
def drop_get(ip,agent,index,elasticsearch,time_to,time_from):
        result = elasticsearch.search(
                index='%s' % (index),
                size="1000",
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
                                                                        'gt': '%s' % (time_to),
                                                                        'lt': '%s' % (time_from)
                                                                }
                                                        }
                                                },{
                                                        'prefix': {"host":'%s' % (agent)}
                                                },{
                                                        'prefix': {"message":'30120'}
                                                }  ]
                                        }
                                }
                        }
                })
        return int(result['hits']['total'])
def analyzer_check(id,ip,agent,analyzer_status,dropframe,dropframe_threshold,discontinuity,discontinuity_threshold,index,elasticsearch):
        drop=0
        ccerror=0
        drop_new=0
        ccerror_new=0
        status_cc=1
        status_drop=1
        status_new=1
        count_drop=0
        count_cc=0
#       list_time=['now','now-2m','now-4m','now-6m','now-8m','now-10m','now-12m','now-14m','now-16m']
        list_time=['now','now-1m','now-2m','now-3m','now-4m','now-5m','now-6m','now-7m','now-8m']
        if dropframe_threshold!=0:
                drop=(int(dropframe)*100)/int(dropframe_threshold)
        if discontinuity_threshold!=0:
                ccerror=(int(discontinuity)*100)/int(discontinuity_threshold)
        if drop >= 25:
                tmp=0
                for i in range(5):
                        drop_new=(int(drop_get(ip,agent,index,elasticsearch,list_time[i+1],list_time[i]))*100)/int(dropframe_threshold)
                        if drop_new > 0:
                                count_drop+=1
                                if drop_new > 0 and drop_new < 40:
                                        tmp+=2
                                elif drop_new >= 40 and drop_new < 100:
                                        tmp+=3
                                elif drop_new >= 100:
                                        tmp+=4
                if count_drop >=3:
                        status_drop=int(tmp/count_drop)
        if ccerror >= 25:
                discontinuity_new=0
                tmp=0
                for i in range(5):
                        cc_new=int(cc_get(ip,agent,index,elasticsearch,list_time[i+1],list_time[i]))*100/int(discontinuity_threshold)
                        if cc_new > 0:
                                count_cc+=1
                                if cc_new > 0 and cc_new < 40:
                                        tmp+=2
                                elif cc_new >= 40 and cc_new < 100:
                                        tmp+=3
                                elif cc_new >= 100:
                                        tmp+=4
                if count_cc >=3:
                        statuc_cc=int(tmp/count_cc)
        if status_drop <= status_cc and status_drop!=1:
                status_new=status_drop
        elif status_drop <= status_cc and status_drop==1:
                status_new=status_cc
        elif status_cc <= status_drop and status_cc!=1:
                status_new=status_cc
        elif status_cc <= status_drop and status_cc==1:
                status_new=status_drop
        if status_new != analyzer_status:
                text="IPmulticast=%s  agent=%s status=%d" % (ip, agent, status_new)
                try:
                        #print status_new
                        #print text
                        #update analyzer status
                        requests.put(api+"profile_agent/"+str(id)+"/", json={"analyzer_status": status_new})
                        #write logs
                        requests.post(api+"log/", json={"host": agent, "tag": 'analyzer', "msg": text})
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

response = urllib.urlopen(api+"profile_agent/analyzer_check/")
if response.getcode()==200:
    print "200"
    profile_agents = json.loads(response.read())
    for profile_agent in profile_agents['profile_analyzer_check']:
        #print profile_agent['id']
        while threading.activeCount() > 10:
            time.sleep(1)
        t = threading.Thread(target=analyzer_check, args=(profile_agent['id'],profile_agent['ip'].split(':30120')[0],profile_agent['agent_ip'],profile_agent['analyzer_status'],profile_agent['dropframe'],profile_agent['dropframe_threshold'],profile_agent['discontinuity'],profile_agent['discontinuity_threshold'],index,es,))
        t.start()
time.sleep(30)
