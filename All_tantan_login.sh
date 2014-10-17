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

$hadoop fs -rmr $result/TANTANLOGIN_$day

$hadoop $stream  \
		-D mapred.reduce.tasks=1 \
		-input $root/push/$day/*/*  \
		-output $result/TANTANLOGIN_$day  \
		-mapper $mr/All_tantan_login_Map.pl    \
		-reducer $mr/All_tantan_login_Red.pl  \
		-file $mr/All_tantan_login_Map.pl \
		-file $mr/All_tantan_login_Red.pl \
		-cmdenv CURRENT_DATE=$day \
		-jobconf mapred.job.name="liuxianpeng_tantan_$day"

