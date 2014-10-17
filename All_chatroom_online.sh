#! /bin/sh

day=`date -d "1 days ago" +"%Y%m%d"`
day2=`date +"%Y%m%d"`

if [ $1 ]
then
    day2=$1
fi

cd /home/hdp-voice/
source .bashrc


hadoop="hadoop"
stream="jar /home/hdp-voice/hadoop-fb-0.20.1.9-streaming.jar"

mr="/home/hdp-voice/"
root="/home/hdp-voice/voice"
result="/home/hdp-voice/voice/stat-result-wy"

cd /home/hdp-voice/
source .bashrc

$hadoop fs -rmr $result/ALL_CHATONLINE_$day
$hadoop $stream  \
		-D mapred.reduce.tasks=3 \
		-input $root/chatonline/$day2/*/*  \
		-output $result/ALL_CHATONLINE_$day  \
		-mapper $mr/All_chatroom_online_Map.pl    \
		-reducer $mr/All_chatroom_online_Red.pl  \
		-file $mr/All_chatroom_online_Map.pl \
		-file $mr/All_chatroom_online_Red.pl \
		-cmdenv CURRENT_DATE=$day \
		-jobconf mapred.job.name="liuxianpeng_All_ChatOnlineStat_$day"


$hadoop fs -cat $result/ALL_CHATONLINE_$day/part* | $mr/All_chatroom_online_SQL.pl CHAT_ACTIVE_USER_INFO_2013 >> $mr/sql_script/All_chat_calculate_sql_$day.sql
