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
result="/home/hdp-voice/voice/stat-result-lxp"


$hadoop fs -rmr $result/GROUP_$day
$hadoop $stream			-D mapred.reduce.tasks=3 \
				-input $root/group-log/$day/*/* \
				-input $root/group-db/$day/*/*  \
				-output $result/GROUP_$day \
				-mapper $mr/360_ContactVoice_GroupdbMap.pl \
				-reducer $mr/360_ContactVoice_GroupdbRed.pl \
				-file $mr/360_ContactVoice_GroupdbMap.pl \
				-file $mr/360_ContactVoice_GroupdbRed.pl \
				-jobconf mapred.job.name="liuxianpeng_Groupdb_$day"
				
$hadoop fs -cat $result/GROUP_$day/part* | $mr/group_SQL.pl GROUP_QLOG_STAT_2013  >> $mr/sql_script/calculate_sql_$day.sql
