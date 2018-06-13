#!/usr/bin/env bash

# Hadoop Archive commands below

hadoop fs -ls /sunil
hadoop archive -archiveName myfiles.har -p /sunil /tmp
hadoop fs -ls /tmp/myfiles.har
hadoop fs -cat /tmp/myfiles.har/_index
hadoop fs -cat /tmp/myfiles.har/_masterindex
hadoop fs -ls /sunil


hadoop fs -ls /tmp/myfiles.har
hadoop fs -ls har:///tmp/myfiles.har
hadoop fs -ls har:///tmp/myfiles.har/hdfs/copyviaprogram
hadoop fs -ls har://hdfs-localhost:8020/tmp/myfiles.har/hdfs/copyviaprogram
hadoop fs -ls har://hdfs-localhost/tmp/myfiles.har/hdfs/copyviaprogram
