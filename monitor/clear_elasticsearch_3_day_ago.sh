#!/bin/bash
a=$(/bin/date +%s)
#a=$(expr $a - 604800)
a=$(expr $a - 345600)
a=$(echo '@'$a)
#echo "$b"
date_old=$(/bin/date -d "$a" +'%Y.%m.%d')
#echo "$date_old"
/usr/bin/curl -XDELETE "10.0.21.123:9200/logstash-$date_old"
#/usr/bin/curl -XDELETE "10.0.21.123:9200/nginx-$date_old"
