#! /bin/sh

day=`date -d "1 days ago" +"%Y%m%d"`

if [ $1 ]
then
    day=$1
fi

cd /home/hdp-voice/
source .bashrc


hadoop="hadoop"
stream="jar /home/hdp-voice/hadoop-fb-0.20.1.9-streaming.jar"

mr="/home/hdp-voice/"
root="/home/hdp-voice/voice"
result="/home/hdp-voice/voice/stat-result-wy"

$hadoop fs -rmr $result/WEIMIMSG_$day

$hadoop $stream  \
		-D mapred.reduce.tasks=3 \
		-input $root/weimimsg/$day/*/*  \
		-input $root/weimidistribute/$day/*/*  \
		-output $result/WEIMIMSG_$day  \
		-mapper $mr/All_weimi_msg_Map.pl    \
		-reducer $mr/All_weimi_msg_Red.pl  \
		-file $mr/All_weimi_msg_Map.pl \
		-file $mr/All_weimi_msg_Red.pl \
                -cmdenv INPUT_TYPE1="weimidistribute" \
                -cmdenv INPUT_TYPE2="weimimsg" \
		-cmdenv CURRENT_DATE=$day \
		-jobconf mapred.job.name="liuxianpeng_WeimiMsg_$day"


$hadoop fs -cat $result/WEIMIMSG_$day/part* | $mr/All_weimi_msg_SQL.pl WEIMI_ACTIVE_2013 >> $mr/sql_script/All_weimi_calculate_sql_$day.sql
