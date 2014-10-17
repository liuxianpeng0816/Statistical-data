#! /bin/sh

day=`date -d "1 days ago" +"%Y%m%d"`

if [ $1 ]
then
	day=$1
fi


/home/hdp-voice/group_Man.sh $day
/home/hdp-voice/register.sh $day
/home/hdp-voice/active.sh $day
/home/hdp-voice/remain.sh $day
/home/hdp-voice/totalusr.sh $day
/home/hdp-voice/client.sh $day
/home/hdp-voice/groupcount.sh $day
/home/hdp-voice/vocusr.sh $day
scp /home/hdp-voice/sql_script/calculate_sql_$day.sql hdp-voice@211.151.122.96:/home/hdp-voice/
