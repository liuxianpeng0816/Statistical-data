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

$hadoop fs -rmr $result/ALL_CHAT_ACTIVE_$day

$hadoop $stream   \
		-D mapred.reduce.tasks=3 \
		-input $root/chatroom/$day/*/*  \
		-output $result/ALL_CHAT_ACTIVE_$day  \
		-mapper $mr/All_chat_active_Map.pl \
		-reducer $mr/All_chat_active_Red.pl  \
		-file $mr/All_chat_active_Map.pl  \
		-file $mr/All_chat_active_Red.pl \
		-cmdenv CURRENT_DATE=$day \
		-jobconf mapred.job.name="liuxianpeng_ALL_CHAT_ACTIVE_$day"

$hadoop fs -cat $result/ALL_CHAT_ACTIVE_$day/part* | $mr/All_chat_active_SQL.pl CHAT_ACTIVE_USER_INFO_2013 >> $mr/sql_script/All_chat_calculate_sql_$day.sql
