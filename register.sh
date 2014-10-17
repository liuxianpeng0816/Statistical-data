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

cd /home/hdp-voice/
source .bashrc

$hadoop fs -rmr $result/REG_$day
$hadoop $stream  \
		-D mapred.reduce.tasks=3 \
		-input $root/register-log/$day/*/*  \
		-input $root/down-reg/$day/*/*  \
		-output $result/REG_$day  \
		-mapper $mr/register_Map.pl    \
		-reducer $mr/register_Red.pl  \
		-file $mr/register_Map.pl \
		-file $mr/register_Red.pl \
		-cmdenv CURRENT_DATE=$day \
		-jobconf mapred.job.name="liuxianpeng_RegStat_$day"


$hadoop fs -cat $result/REG_$day/part* | $mr/register_SQL.pl USER_SUM_INFO_2013 >> $mr/sql_script/calculate_sql_$day.sql
