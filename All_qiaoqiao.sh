#! /bin/sh

day=`date -d "1 days ago" +"%Y%m%d"`

if [ $1 ]
then
day=$1
fi


/home/hdp-voice/All_qiaoqiao_register.sh $day
/home/hdp-voice/All_qiaoqiao_login.sh $day
/home/hdp-voice/All_qiaoqiao_active.sh $day
/home/hdp-voice/All_qiaoqiao_totalusr.sh $day
/home/hdp-voice/All_qiaoqiao_sharemessage.sh $day
/home/hdp-voice/All_qiaoqiao_remain.sh $day
scp /home/hdp-voice/sql_script/All_qiaoqiao_calculate_sql_$day.sql hdp-voice@211.151.122.96:/home/hdp-voice/
