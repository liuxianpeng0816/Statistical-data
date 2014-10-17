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


$hadoop fs -rmr $result/ALL_CHAT_$day
$hadoop $stream			-D mapred.reduce.tasks=3 \
				-input $root/chat-db/$day/*/*  \
				-input $root/chatroom/$day/*/*  \
				-output $result/ALL_CHAT_$day \
				-mapper $mr/All_360_ContactVoice_ChatdbMap.pl \
				-reducer $mr/All_360_ContactVoice_ChatdbRed.pl \
				-file $mr/All_360_ContactVoice_ChatdbMap.pl \
				-file $mr/All_360_ContactVoice_ChatdbRed.pl \
		                -file $mr/Roomid-Game1 \
                                -cmdenv INPUT_TYPE1="chatdb" \
                                -cmdenv INPUT_TYPE2="chatroom" \
		                -cmdenv CURRENT_DATE=$day \
				-jobconf mapred.job.name="liuxianpeng_All_chatdb_$day"
				
$hadoop fs -cat $result/ALL_CHAT_$day/part* | $mr/All_360_ContactVoice_Chatdb_SQL.pl CHAT_QLOG_STAT_2013 CHAT_GAMEINFO_2013>> $mr/sql_script/All_chat_calculate_sql_$day.sql
