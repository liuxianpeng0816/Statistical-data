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

$hadoop fs -rmr $result/VOCUSR_$day
$hadoop $stream  \
		-D mapred.reduce.tasks=3 \
		-input $root/txl-msg/$day/*/*.log  \
		-output $result/VOCUSR_$day  \
		-mapper $mr/vocusr_Map.pl    \
		-reducer $mr/vocusr_Red.pl  \
		-file $mr/vocusr_Map.pl \
		-file $mr/vocusr_Red.pl \
		-cmdenv CURRENT_DATE=$day \
		-cmdenv INPUT_TYPE1="dba" \
		-jobconf mapred.job.name="liuxianpeng_vocusr_$day"

$hadoop fs -cat $result/VOCUSR_$day/part* | $mr/vocusr_SQL.pl PEER_SUM_INFO_2013 >> $mr/sql_script/calculate_sql_$day.sql			
