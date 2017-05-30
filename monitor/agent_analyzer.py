#!/opt/python-2.7/bin/python
import os, sys, subprocess, shlex, re, fnmatch, signal
from subprocess import call
import threading
from elasticsearch import Elasticsearch
import time
#from datetime import datetime

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

list_time=['now','now-1m','now-2m','now-3m','now-4m','now-5m','now-6m','now-7m','now-8m']
es = Elasticsearch('localhost:9200',timeout=3.5)
localtime = time.gmtime()
date_now=time.strftime("%Y.%m.%d",localtime)
index='logstash-%s' % (date_now)
new_drop = cc_get('225.1.1.248','172.28.0.218',index,es,list_time[1],list_time[2])
print new_drop
