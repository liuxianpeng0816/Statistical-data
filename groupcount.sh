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

echo "CREATE TABLE IF NOT EXISTS GROUP_SUM_INFO_2013 (DATES date, TYPES varchar(16), DIME varchar(64), VAL int, constraint PK_GROUP_SUM_INFO_2013 primary key clustered (DATES,TYPES,DIME));" >> $mr/sql_script/calculate_sql_$day.sql

$hadoop fs -cat $root/group-dump/$day/* | awk '{gsub(/></,",",$0);gsub(/<|>/,"",$0)}END{print $0}' | awk '{split($0,a,",")}END{for(k in a){print a[k]}}' | awk -F':' '{gsub(/[[:blank:]]*/,"",$2);a[$1]=$2}END{for(k in a){print "INSERT INTO GROUP_SUM_INFO_2013 (DATES, TYPES,DIME, VAL) VALUES ('\'''$day''\'', '\''"k"'\'', '\''ALL'\'', "a[k]") ;"}}' >> $mr/sql_script/calculate_sql_$day.sql
