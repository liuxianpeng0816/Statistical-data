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

rm -rf ShareMessageSaveKey.$day

$hadoop fs -rmr $result/MSKSHAREKEY_$day

$hadoop $stream  \
		-D mapred.reduce.tasks=3 \
		-input $root/msksharemessage/$day/*/*  \
		-output $result/MSKSHAREKEY_$day  \
		-mapper $mr/All_msk_sharemessagekey_Map.pl    \
		-reducer $mr/All_msk_sharemessagekey_Red.pl  \
		-file $mr/All_msk_sharemessagekey_Map.pl \
		-file $mr/All_msk_sharemessagekey_Red.pl \
		-cmdenv CURRENT_DATE=$day \
		-jobconf mapred.job.name="liuxianpeng_MskShareKeyStat_$day"

$hadoop fs -cat $result/MSKSHAREKEY_$day/part* >>ShareMessageSaveKey.$day
