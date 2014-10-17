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

$hadoop fs -rmr $result/MSKSHARE_$day

$hadoop $stream  \
		-D mapred.reduce.tasks=3 \
		-input $root/msksharemessage/$day/*/*  \
		-output $result/MSKSHARE_$day  \
		-mapper $mr/All_msk_sharemessage_Map.pl    \
		-reducer $mr/All_msk_sharemessage_Red.pl  \
		-file $mr/All_msk_sharemessage_Map.pl \
		-file $mr/All_msk_sharemessage_Red.pl \
		-file $mr/ShareMessageSaveKey.$day \
		-cmdenv CURRENT_DATE=$day \
		-jobconf mapred.job.name="liuxianpeng_MskShareStat_$day"


$hadoop fs -cat $result/MSKSHARE_$day/part* | $mr/All_msk_sharemessage_SQL.pl MSK_ACTIVE_2013 >> $mr/sql_script/All_msk_calculate_sql_$day.sql
