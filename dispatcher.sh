#!/bin/bash
. /etc/profile.d/mdsplus.sh
echo $1
echo $2
echo "start the dispatcher ..."
/opt/RedisActionDispatcher/dispatcher.py $1 $2
echo "exiting"
