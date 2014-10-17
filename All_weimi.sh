#! /bin/sh

day=`date -d "1 days ago" +"%Y%m%d"`

if [ $1 ]
then
day=$1
fi

/home/hdp-voice/All_weimi_msg.sh $day
scp /home/hdp-voice/sql_script/All_weimi_calculate_sql_$day.sql hdp-voice@211.151.122.96:/home/hdp-voice/
