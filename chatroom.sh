#! /bin/sh

time=`date +%Y%m%d%H`
sqlhour=`date -d "1 hour ago" +"%Y%m%d%H"`

if [ $1 ]
then
time=$1
fi


/home/hdp-voice/chat_register.sh $time
#/home/hdp-voice/chat_totalusr.sh $time
/home/hdp-voice/chat_active.sh $time
/home/hdp-voice/chat_activegame.sh $time
/home/hdp-voice/360_ContactVoice_Chatdb.sh $time
scp /home/hdp-voice/sql_script/chat_calculate_sql_$sqlhour.sql hdp-voice@211.151.122.96:/home/hdp-voice/
