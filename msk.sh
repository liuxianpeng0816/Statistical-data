#! /bin/sh

time=`date +%Y%m%d%H`
sqlhour=`date -d "1 hour ago" +"%Y%m%d%H"`

if [ $1 ]
then
time=$1
fi


/home/hdp-voice/msk_login.sh $time
/home/hdp-voice/msk_register.sh $time
/home/hdp-voice/msk_totalusr.sh $time
/home/hdp-voice/msk_active.sh $time
/home/hdp-voice/msk_sharemessagekey.sh $time
/home/hdp-voice/msk_sharemessage.sh $time
/home/hdp-voice/msk_dongtai.sh $time
/home/hdp-voice/msk_reply.sh $time
scp /home/hdp-voice/sql_script/msk_calculate_sql_$sqlhour.sql hdp-voice@211.151.122.96:/home/hdp-voice/
