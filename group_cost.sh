#! /bin/sh

root=/home/hdp-voice/
hour=`date -d "1 hour ago" +"%Y%m%d%H"`

if [ $1 ]
then
    hour=$1
fi

day=${hour:0:8}

$root/360_ContactVoice_QMan_Group_general.sh set-notify $hour /home/hdp-voice/voice/msgrouter-log/$day/${hour:8:2}/  /home/hdp-voice/voice/group-log/$day/${hour:8:2}/ /home/hdp-voice/voice/stat-result-wy/QLOG_SN_$day/${hour:8:2} 
$root/360_ContactVoice_QMan_Group_general.sh set-set $hour /home/hdp-voice/voice/msgrouter-log/$day/${hour:8:2}/  /home/hdp-voice/voice/group-db/$day/${hour:8:2}/  /home/hdp-voice/voice/stat-result-wy/QLOG_GROUP_SS_$day/${hour:8:2}  
$root/360_ContactVoice_QMan_Group_general.sh prepare-get $hour /home/hdp-voice/voice/msgrouter-log/$day/${hour:8:2}/  /home/hdp-voice/voice/group-db/$day/${hour:8:2}/  /home/hdp-voice/voice/stat-result-wy/QLOG_GROUP_PG_$day/${hour:8:2} 
$root/360_ContactVoice_QMan_Group_general.sh set-get $hour  /home/hdp-voice/voice/group-db/$day/*/  /home/hdp-voice/voice/stat-result-wy/QLOG_GROUP_SG_$day/${hour:8:2} 

scp $root/sql_script/group_qlog_sql_$hour.sql hdp-voice@211.151.122.96:/home/hdp-voice/
