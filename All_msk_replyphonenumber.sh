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

cd /home/hdp-voice/
source .bashrc

$hadoop fs -rmr $result/MSKREPLYPHONE_$day
$hadoop $stream  \
		-D mapred.reduce.tasks=3 \
		-input $root/mskreply/*/*/*  \
		-output $result/MSKREPLYPHONE_$day  \
		-mapper $mr/All_msk_replyphonenumber_Map.pl    \
		-reducer $mr/All_msk_replyphonenumber_Red.pl  \
		-file $mr/All_msk_replyphonenumber_Map.pl \
		-file $mr/All_msk_replyphonenumber_Red.pl \
		-cmdenv CURRENT_DATE=$day \
		-jobconf mapred.job.name="liuxianpeng_MskReplyPhone_$day"


$hadoop fs -cat $result/MSKREPLYPHONE_$day/part* >>ReplyPhoneNumber1
cat ReplyPhoneNumber1 |sort|uniq >ReplyPhoneNumber
