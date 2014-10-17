#! /bin/sh

day=`date -d "1 days ago" +"%Y%m%d"`
day2=`date +"%Y%m%d"`

if [ $1 ]
then
day=$1
fi


/home/hdp-voice/All_chat_register.sh $day
/home/hdp-voice/All_chat_totalusr.sh $day
/home/hdp-voice/All_chat_active.sh $day
/home/hdp-voice/All_chat_activegame.sh $day
/home/hdp-voice/All_360_ContactVoice_Chatdb.sh $day
/home/hdp-voice/All_chatroom_online.sh $day2
scp /home/hdp-voice/sql_script/All_chat_calculate_sql_$day.sql hdp-voice@211.151.122.96:/home/hdp-voice/
