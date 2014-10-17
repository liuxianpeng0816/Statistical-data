#! /bin/sh

day=`date -d "1 days ago" +"%Y%m%d"`

if [ $1 ]
then
	day=$1
fi

cd /home/hdp-voice/
source .bashrc

lastday=`date -d "1 days ago $day" +"%Y%m%d"`

hadoop="hadoop"
stream="jar /home/hdp-voice/hadoop-fb-0.20.1.9-streaming.jar"

mr="/home/hdp-voice/"
root="/home/hdp-voice/voice"
result="/home/hdp-voice/voice/stat-result-wy"

$hadoop fs -rmr $result/CLIENT_$day

$hadoop $stream \
		-D mapred.reduce.tasks=3 \
		-Dmap.output.key.field.separator="|" \
		-Dmapred.text.key.partitioner.options=-k1,1 \
		-input $root/client-log/$day/*/*.log \
		-output $result/CLIENT_$day \
		-mapper $mr/client_Map.pl \
		-reducer $mr/client_Red.pl \
		-file $mr/client_Map.pl \
		-file $mr/client_Red.pl \
		-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
		-jobconf mapred.job.name="liuxianpeng_Client_$day"  \
				
$hadoop fs -cat $result/CLIENT_$day/part* | $mr/client_SQL.pl CLIENT_STAT_INFO_2013 >> $mr/sql_script/calculate_sql_$day.sql
