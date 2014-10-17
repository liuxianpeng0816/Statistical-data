#! /bin/sh

time=`date +%Y%m%d%H`
day=${time:0:8}
hour=${time:8:2}

if [ $1 ]
then 
	time=$1
	day=${time:0:8}
	hour=${time:8:2}
fi

if [ $hour -eq 00 ];
then
        day=`date -d "1 days ago" +"%Y%m%d"`
        hour1=23
	hour2=22
	hour3=21
	hour4=20
	
fi

if [ $hour -eq 04 ];
then
        hour1=03
	hour2=02
	hour3=01
	hour4=00
	
fi

if [ $hour -eq 08 ];
then
        hour1=07
	hour2=06
	hour3=05
	hour4=04
	
fi

if [ $hour -eq 12 ];
then
        hour1=11
	hour2=10
	hour3=09
	hour4=08
	
fi

if [ $hour -eq 16 ];
then
        hour1=15
	hour2=14
	hour3=13
	hour4=12
	
fi

if [ $hour -eq 20 ];
then
        hour1=19
	hour2=18
	hour3=17
	hour4=16
	
fi

cd /home/hdp-voice
source .bashrc

sqlhour=`date -d "1 hour ago" +"%Y%m%d%H"`

hadoop="hadoop"
stream="jar /home/hdp-voice/hadoop-fb-0.20.1.9-streaming.jar"

mr="/home/hdp-voice"
root="/home/hdp-voice/voice"
result="/home/hdp-voice/voice/stat-result-lxp"


$hadoop fs -rmr $result/CHAT_$time
$hadoop $stream			-D mapred.reduce.tasks=3 \
				-input $root/chat-db/$day/$hour1/*  \
				-input $root/chat-db/$day/$hour2/*  \
				-input $root/chat-db/$day/$hour3/*  \
				-input $root/chat-db/$day/$hour4/*  \
				-input $root/chatroom/$day/$hour1/*  \
				-input $root/chatroom/$day/$hour2/*  \
				-input $root/chatroom/$day/$hour3/*  \
				-input $root/chatroom/$day/$hour4/*  \
				-output $result/CHAT_$time \
				-mapper $mr/360_ContactVoice_ChatdbMap.pl \
				-reducer $mr/360_ContactVoice_ChatdbRed.pl \
				-file $mr/360_ContactVoice_ChatdbMap.pl \
				-file $mr/360_ContactVoice_ChatdbRed.pl \
		                -file $mr/Roomid-Game1 \
                                -cmdenv INPUT_TYPE1="chatdb" \
                                -cmdenv INPUT_TYPE2="chatroom" \
		                -cmdenv CURRENT_DATE=$time \
				-jobconf mapred.job.name="liuxianpeng_chatdb_$time"
				
$hadoop fs -cat $result/CHAT_$time/part* | $mr/360_ContactVoice_Chatdb_SQL.pl CHAT_QLOG_STAT_2014 CHAT_GAMEINFO_2014 >> $mr/sql_script/chat_calculate_sql_$sqlhour.sql
