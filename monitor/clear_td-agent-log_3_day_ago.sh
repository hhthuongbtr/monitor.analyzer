#!/bin/bash
a=$(/bin/date +%s)
#a=$(expr $a - 604800)
a=$(expr $a - 345600)
a=$(echo '@'$a)
#echo "$b"
date_old=$(/bin/date -d "$a" +'%Y%m%d')
echo "$date_old"
/bin/rm -rf "/var/log/td-agent/*$date_old*"
