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

$hadoop fs -rmr $result/REMAIN_$day
$hadoop $stream  \
		-D mapred.reduce.tasks=3 \
		-Dmap.output.key.field.separator="_" \
		-Dmapred.text.key.partitioner.options=-k1,1 \
		-input $root/register-log/$one_day_ago/*/*  \
		-input $root/register-log/$two_days_ago/*/*  \
		-input $root/register-log/$three_days_ago/*/*  \
		-input $root/register-log/$four_days_ago/*/*  \
		-input $root/register-log/$five_days_ago/*/*  \
		-input $root/register-log/$six_days_ago/*/*  \
		-input $root/register-log/$seven_days_ago/*/*  \
		-input $root/usr-log/$day/*/*.log  \
		-output $result/REMAIN_$day  \
		-mapper $mr/remain_Map.pl  \
		-reducer $mr/remain_Red.pl  \
		-file $mr/remain_Map.pl  \
		-file $mr/remain_Red.pl \
		-cmdenv CURRENT_DATE=$day \
		-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
		-jobconf mapred.job.name="liuxianpeng_remain_$day"

$hadoop fs -cat $result/REMAIN_$day/part* | $mr/remain_SQL.pl USER_REMAIN_INFO_2013 >> $mr/sql_script/calculate_sql_$day.sql
