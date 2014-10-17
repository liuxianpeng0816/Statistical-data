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

$hadoop fs -rmr $result/ACTIVE_$day

$hadoop $stream   \
		-D mapred.reduce.tasks=3 \
		-input $root/usr-log/$day/*/*  \
		-output $result/ACTIVE_$day  \
		-mapper $mr/active_Map.pl \
		-reducer $mr/active_Red.pl  \
		-file $mr/active_Map.pl  \
		-file $mr/active_Red.pl \
		-file $mr/mobile-cn \
		-cmdenv CURRENT_DATE=$day \
		-jobconf mapred.job.name="liuxianpeng_ACTIVE_$day"

$hadoop fs -cat $result/ACTIVE_$day/part* | $mr/active_SQL.pl USER_SUM_INFO_2013 ACTIVE_USER_INFO_2013 >> $mr/sql_script/calculate_sql_$day.sql
