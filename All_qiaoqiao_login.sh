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

$hadoop fs -rmr $result/QIAOQIAOLOGIN_$day

$hadoop $stream  \
		-D mapred.reduce.tasks=3 \
		-input $root/qiaoqiao-login/$day/*/*  \
		-input $root/qiao-getmessage/$day/*/*  \
		-output $result/QIAOQIAOLOGIN_$day  \
		-mapper $mr/All_qiaoqiao_login_Map.pl    \
		-reducer $mr/All_qiaoqiao_login_Red.pl  \
		-file $mr/All_qiaoqiao_login_Map.pl \
		-file $mr/All_qiaoqiao_login_Red.pl \
                -cmdenv INPUT_TYPE1="qiaologin" \
                -cmdenv INPUT_TYPE2="qiaogetmessage" \
		-cmdenv CURRENT_DATE=$day \
		-jobconf mapred.job.name="liuxianpeng_QiaoQiaoLoginStat_$day"


$hadoop fs -cat $result/QIAOQIAOLOGIN_$day/part* | $mr/All_qiaoqiao_login_SQL.pl QIAOQIAO_ACTIVE_2013 >> $mr/sql_script/All_qiaoqiao_calculate_sql_$day.sql
