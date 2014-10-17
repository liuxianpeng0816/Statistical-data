#! /bin/sh

day=`date -d "1 days ago" +"%Y%m%d"`

if [ $1 ]
then
    day=$1
fi

cd /home/hdp-voice/
source .bashrc

one_day_ago=`date -d "1 days ago $day" +"%Y%m%d"`
two_days_ago=`date -d "2 days ago $day" +"%Y%m%d"`
three_days_ago=`date -d "3 days ago $day" +"%Y%m%d"`
four_days_ago=`date -d "4 days ago $day" +"%Y%m%d"`
five_days_ago=`date -d "5 days ago $day" +"%Y%m%d"`
six_days_ago=`date -d "6 days ago $day" +"%Y%m%d"`
seven_days_ago=`date -d "7 days ago $day" +"%Y%m%d"`

hadoop="hadoop"
stream="jar /home/hdp-voice/hadoop-fb-0.20.1.9-streaming.jar"

mr="/home/hdp-voice/"
root="/home/hdp-voice/voice"
result="/home/hdp-voice/voice/stat-result-wy"

$hadoop fs -rmr $result/ALL_QIAOQIAO_REMAIN_$day
$hadoop $stream  \
		-D mapred.reduce.tasks=3 \
		-Dmap.output.key.field.separator="_" \
		-Dmapred.text.key.partitioner.options=-k1,1 \
		-input $root/qiaoqiao-reg/$one_day_ago/*/*  \
		-input $root/qiaoqiao-reg/$two_days_ago/*/*  \
		-input $root/qiaoqiao-reg/$three_days_ago/*/*  \
		-input $root/qiaoqiao-reg/$four_days_ago/*/*  \
		-input $root/qiaoqiao-reg/$five_days_ago/*/*  \
		-input $root/qiaoqiao-reg/$six_days_ago/*/*  \
		-input $root/qiaoqiao-reg/$seven_days_ago/*/*  \
		-input $root/qiaoqiao-active/$day/*/*  \
		-output $result/ALL_QIAOQIAO_REMAIN_$day  \
		-mapper $mr/All_qiaoqiao_remain_Map.pl  \
		-reducer $mr/All_qiaoqiao_remain_Red.pl  \
		-file $mr/All_qiaoqiao_remain_Map.pl  \
		-file $mr/All_qiaoqiao_remain_Red.pl \
                -cmdenv INPUT_TYPE1="qiaoactive" \
                -cmdenv INPUT_TYPE2="qiaoqiao" \
		-cmdenv CURRENT_DATE=$day \
		-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
		-jobconf mapred.job.name="liuxianpeng_qiaoqiaoremain_$day"

$hadoop fs -cat $result/ALL_QIAOQIAO_REMAIN_$day/part* | $mr/All_qiaoqiao_remain_SQL.pl QIAOQIAO_REMAIN_INFO_2014 >> $mr/sql_script/All_qiaoqiao_calculate_sql_$day.sql
