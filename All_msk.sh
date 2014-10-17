#! /bin/sh

day=`date -d "1 days ago" +"%Y%m%d"`

if [ $1 ]
then
day=$1
fi


/home/hdp-voice/All_msk_login.sh $day
/home/hdp-voice/All_msk_replyphonenumber.sh  $day
/home/hdp-voice/All_msk_register_phonenumber.sh  $day
/home/hdp-voice/All_msk_register.sh $day
/home/hdp-voice/All_msk_totalusr.sh $day
/home/hdp-voice/All_msk_active.sh $day
/home/hdp-voice/All_msk_sharemessagekey.sh $day
/home/hdp-voice/All_msk_sharemessage.sh $day
/home/hdp-voice/All_msk_dongtai.sh $day
/home/hdp-voice/All_msk_download.sh $day
/home/hdp-voice/All_msk_relation.sh $day
/home/hdp-voice/All_msk_reply.sh $day
scp /home/hdp-voice/sql_script/All_msk_calculate_sql_$day.sql hdp-voice@211.151.122.96:/home/hdp-voice/
