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

cd /home/hdp-voice/
source .bashrc


#$hadoop fs -cat $root/total-usr/$day/* | awk -F'<|>' '{print $2}' | awk -F':' '{print "INSERT INTO USER_SUM_INFO_2013 (DATES, TYPES, DIME, VAL) VALUES ('\'''$day''\'', '\''TOTAL'\'', '\''ALL'\'', "$2") ;"}' >> $mr/sql_script/calculate_sql_$day.sql
$hadoop fs -cat $root/total-usr/$day/* | awk -F'<|>' '{print $2}' | awk -F',' '{print $1}' | awk -F':' '{print "INSERT INTO USER_SUM_INFO_2013 (DATES, TYPES, DIME, VAL) VALUES ('\'''$day''\'', '\''TOTAL'\'', '\''ALL'\'', "$2") ;"}' >> $mr/sql_script/calculate_sql_$day.sql
