#!/bin/bash

case $1 in
"start"){
	for i in spark1 spark2 spark3
	do
	 ssh $i "/usr/local/zookeeper-3.5.5/bin/zkServer.sh start"
	done
};;
"stop"){
	for i in spark1 spark2 spark3
	do
	 ssh $i "/usr/local/zookeeper-3.5.5/bin/zkServer.sh stop"
	done
};;
"status"){
	for i in spark1 spark2 spark3
	do
	 ssh $i "/usr/local/zookeeper-3.5.5/bin/zkServer.sh status"
	done
};;
esac
