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

$hadoop fs -rmr $result/MSKREGPHONE_$day
$hadoop $stream  \
		-D mapred.reduce.tasks=3 \
		-input $root/mskprereg/$day/*/*  \
		-output $result/MSKREGPHONE_$day  \
		-mapper $mr/All_msk_register_phonenumber_Map.pl    \
		-reducer $mr/All_msk_register_phonenumber_Red.pl  \
		-file $mr/All_msk_register_phonenumber_Map.pl \
		-file $mr/All_msk_register_phonenumber_Red.pl \
		-file $mr/ReplyPhoneNumber \
		-cmdenv CURRENT_DATE=$day \
		-jobconf mapred.job.name="liuxianpeng_MskRegPhone_$day"


$hadoop fs -cat $result/MSKREGPHONE_$day/part* | $mr/All_msk_register_phonenumber_SQL.pl MSK_ACTIVE_2013 >> $mr/sql_script/All_msk_calculate_sql_$day.sql
