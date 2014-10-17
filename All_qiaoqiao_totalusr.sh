#! /bin/sh

day=`date -d "1 days ago" +"%Y%m%d"`

if [ $1 ]
then
    day=$1
fi

hour=23

hadoop="hadoop"
stream="jar /home/hdp-voice/hadoop-fb-0.20.1.9-streaming.jar"

mr="/home/hdp-voice/"
root="/home/hdp-voice/voice"

cd /home/hdp-voice/
source .bashrc


$hadoop fs -cat $root/qiaoqiao-total/$day/$hour/* | awk -F'<|>' '{print $2}' | awk -F',' '{print $1}' | awk -F':' '{print "INSERT INTO QIAOQIAO_ACTIVE_2013 (DATES, TYPES, DIME, VAL) VALUES ('\'''$day''\'', '\''MANREG'\'', '\''ALL'\'', "$2") ;"}' >> $mr/sql_script/All_qiaoqiao_calculate_sql_$day.sql
$hadoop fs -cat $root/qiaoqiao-total/$day/$hour/* | awk -F'<|>' '{print $2}' | awk -F',' '{print $3}' | awk -F':' '{print "INSERT INTO QIAOQIAO_ACTIVE_2013 (DATES, TYPES, DIME, VAL) VALUES ('\'''$day''\'', '\''INVITEREG'\'', '\''ALL'\'', "$2") ;"}' >> $mr/sql_script/All_qiaoqiao_calculate_sql_$day.sql
