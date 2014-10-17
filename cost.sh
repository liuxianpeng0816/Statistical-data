#! /bin/sh

root=/home/hdp-voice/
hour=`date -d "1 hour ago" +"%Y%m%d%H"`

if [ $1 ]
then
    hour=$1
fi

day=${hour:0:8}

$root/360_ContactVoice_QMan_general.sh set-notify $hour /home/hdp-voice/voice/msgrouter-log/$day/${hour:8:2}/  /home/hdp-voice/voice/stat-result-wy/QLOG_SN_$day/${hour:8:2} 
$root/360_ContactVoice_QMan_general.sh set-set $hour /home/hdp-voice/voice/msgrouter-log/$day/${hour:8:2}/  /home/hdp-voice/voice/txl-msg/$day/${hour:8:2}/  /home/hdp-voice/voice/stat-result-wy/QLOG_SS_$day/${hour:8:2} 
$root/360_ContactVoice_QMan_general.sh set-get $hour  /home/hdp-voice/voice/txl-msg/$day/*/  /home/hdp-voice/voice/stat-result-wy/QLOG_SG_$day/${hour:8:2} 
$root/360_ContactVoice_QMan_general.sh prepare-get $hour /home/hdp-voice/voice/msgrouter-log/$day/${hour:8:2}/  /home/hdp-voice/voice/txl-msg/$day/${hour:8:2}/  /home/hdp-voice/voice/stat-result-wy/QLOG_PG_$day/${hour:8:2} 
$root/360_ContactVoice_QMan_general.sh sn-lose $hour /home/hdp-voice/voice/usr-log/$day/${hour:8:2}/ /home/hdp-voice/voice/distribute-log/$day/${hour:8:2}/ /home/hdp-voice/voice/pushagent-log/$day/${hour:8:2}/ /home/hdp-voice/voice/stat-result-wy/SN_LOSE_$day/${hour:8:2}
$root/360_ContactVoice_QMan_general.sh up-download $hour /home/hdp-voice/voice/media-upload/$day/${hour:8:2}/  /home/hdp-voice/voice/media-download/$day/${hour:8:2}/  /home/hdp-voice/voice/stat-result-wy/UP_DOWNLOAD_$day/${hour:8:2}

scp $root/sql_script/peer_qlog_sql_$hour.sql hdp-voice@211.151.122.96:/home/hdp-voice/
