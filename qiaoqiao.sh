#! /bin/sh

time=`date +%Y%m%d%H`
sqlhour=`date -d "1 hour ago" +"%Y%m%d%H"`

if [ $1 ]
then
time=$1
fi


/home/hdp-voice/qiaoqiao_register.sh $time
/home/hdp-voice/qiaoqiao_login.sh $time
/home/hdp-voice/qiaoqiao_active.sh $time
/home/hdp-voice/qiaoqiao_totalusr.sh $time
/home/hdp-voice/qiaoqiao_sharemessage.sh $time
scp /home/hdp-voice/sql_script/qiaoqiao_calculate_sql_$sqlhour.sql hdp-voice@211.151.122.96:/home/hdp-voice/
