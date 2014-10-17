#! /bin/sh

if [ $1 ]
then
    time=$1
    day=${time:0:8}                                                                                          
    hour=${time:8:2} 
fi

if [ $hour -eq 00 ];
then
        day=`date -d "1 days ago" +"%Y%m%d"`
        hour=23
fi

if [ $hour -eq 04 ];
then
        hour=03
	
fi

if [ $hour -eq 08 ];
then
        hour=07
	
fi

if [ $hour -eq 12 ];
then
        hour=11
	
fi

if [ $hour -eq 16 ];
then
        hour=15
	
fi

if [ $hour -eq 20 ];
then
        hour=19
	
fi

sqlhour=`date -d "1 hour ago" +"%Y%m%d%H"`

time1=$time"0000"

hadoop="hadoop"
stream="jar /home/hdp-voice/hadoop-fb-0.20.1.9-streaming.jar"

mr="/home/hdp-voice/"
root="/home/hdp-voice/voice"

cd /home/hdp-voice/
source .bashrc

#<mb:100, qid:0, total invitee:196, qid invitee:0, total:296>	
$hadoop fs -cat $root/msktotal/$day/$hour/* | awk -F'<|>' '{print $2}' | awk -F',' '{print $1}' | awk -F':' '{print "INSERT INTO MSK_ACTIVE_2014 (DATES, TYPES, DIME, VAL) VALUES ('\'''$time1''\'', '\''MANREG_TOTAL'\'', '\''ALL'\'', "$2") ;"}' >> $mr/sql_script/msk_calculate_sql_$sqlhour.sql
$hadoop fs -cat $root/msktotal/$day/$hour/* | awk -F'<|>' '{print $2}' | awk -F',' '{print $3}' | awk -F':' '{print "INSERT INTO MSK_ACTIVE_2014 (DATES, TYPES, DIME, VAL) VALUES ('\'''$time1''\'', '\''INVITEREG_TOTAL'\'', '\''ALL'\'', "$2") ;"}' >> $mr/sql_script/msk_calculate_sql_$sqlhour.sql
