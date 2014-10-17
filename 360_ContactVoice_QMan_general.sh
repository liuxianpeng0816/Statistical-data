#! /bin/sh

hadoop=hadoop
stream="jar /home/hdp-voice/hadoop-fb-0.20.1.9-streaming.jar"
root=/home/hdp-voice/voice
mr=/home/hdp-voice

cd /home/hdp-voice/
source .bashrc

if [ $# != 4 ] && [ $# != 5 ] && [ $# != 6 ]; 
then
    echo "Error:Parameter Error!"
    exit
fi

if [ $1 ]
then
    type=$1
fi

if [ $2 ]
then   
    hour=$2
fi
if [ $type = "set-notify" ]
then
    if [ $3 ]
    then
        input=$3
    fi

    if [ $4 ]
    then
        output=$4
    fi

    map=$mr/360_ContactVoice_QMap_sn.pl
    red=$mr/360_ContactVoice_QRed_sn.pl
    $hadoop fs -rmr $output
    $hadoop $stream \
	-D mapred.reduce.tasks=3 \
	-input $input  \
        -output $output  \
        -mapper $map  \
        -reducer $red  \
        -file $map    \
        -file $red \
        -cmdenv INPUT_TYPE1="msgrouter" \
        -cmdenv INPUT_HOUR=$hour \
	-jobconf mapred.job.name="liuxianpeng_sn_$day" 
    $hadoop fs -cat $output/part* | $mr/foundation_data_process.pl sn $hour PEER_QLOG_STAT_INFO_2013 PEER_QLOG_MSG_TYPE_INFO_2013 PEER_QLOG_MSG_COST_INFO_2013 >> $mr/sql_script/peer_qlog_sql_$hour.sql
	
elif [ $type = "set-set" ]
then
    if [ $3 ]
    then
        input1=$3
    fi

    if [ $4 ]
    then
        input2=$4
    fi

    if [ $5 ]
    then
        output=$5
    fi

    map=$mr/360_ContactVoice_QMap_ss.pl
    red=$mr/360_ContactVoice_QRed_ss.pl
    $hadoop fs -rmr $output
    $hadoop $stream \
	-D mapred.reduce.tasks=3 \
	-input $input1  \
	-input $input2  \
        -output $output  \
        -mapper $map  \
        -reducer $red  \
        -file $map    \
        -file $red \
        -cmdenv INPUT_TYPE1="msgrouter" \
        -cmdenv INPUT_TYPE2="dba" \
        -cmdenv INPUT_HOUR=$hour  \
	-jobconf mapred.job.name="liuxianpeng_ss_$day" 
        
    $hadoop fs -cat $output/part* | $mr/foundation_data_process.pl ss $hour PEER_QLOG_STAT_INFO_2013 PEER_QLOG_MSG_TYPE_INFO_2013 PEER_QLOG_MSG_COST_INFO_2013 >> $mr/sql_script/peer_qlog_sql_$hour.sql
	
elif [ $type = "set-get" ]
then

    if [ $3 ]
    then
        input=$3
    fi

    if [ $4 ]
    then
        output=$4
    fi

    map=$mr/360_ContactVoice_QMap_sg.pl
    red=$mr/360_ContactVoice_QRed_sg.pl
    $hadoop fs -rmr $output
    $hadoop $stream \
	-D mapred.reduce.tasks=3 \
	-input $input  \
        -output $output  \
        -mapper $map  \
        -reducer $red  \
        -file $map    \
        -file $red \
        -cmdenv INPUT_TYPE1="dba" \
        -cmdenv INPUT_HOUR=$hour \
	-jobconf mapred.job.name="liuxianpeng_sg_$day" 

    $hadoop fs -cat $output/part* | ./foundation_data_process.pl sg $hour PEER_QLOG_STAT_INFO_2013 PEER_QLOG_MSG_TYPE_INFO_2013 PEER_QLOG_MSG_COST_INFO_2013 >> $mr/sql_script/peer_qlog_sql_$hour.sql
elif [ $type = "prepare-get" ]
then
    if [ $3 ]
    then
        input1=$3
    fi

    if [ $4 ]
    then
        input2=$4
    fi

    if [ $5 ]
    then
        output=$5
    fi

    map=$mr/360_ContactVoice_QMap_pg.pl
    red=$mr/360_ContactVoice_QRed_pg.pl

    $hadoop fs -rmr $output
      $hadoop $stream \
	-D mapred.reduce.tasks=3 \
	-input $input1  \
	-input $input2  \
        -output $output  \
        -mapper $map  \
        -reducer $red  \
        -file $map    \
        -file $red \
        -cmdenv INPUT_TYPE1="msgrouter" \
        -cmdenv INPUT_TYPE2="dba" \
        -cmdenv INPUT_HOUR=$hour \
	-jobconf mapred.job.name="liuxianpeng_pg_$day" 
	$hadoop fs -cat $output/part* | ./foundation_data_process.pl pg $hour PEER_QLOG_STAT_INFO_2013 PEER_QLOG_MSG_TYPE_INFO_2013 PEER_QLOG_MSG_COST_INFO_2013 >> $mr/sql_script/peer_qlog_sql_$hour.sql
elif [ $type = "sn-lose" ]
then
    if [ $3 ]
    then
        input1=$3
    fi

    if [ $4 ]
    then
        input2=$4
    fi

    if [ $5 ]
    then
        input3=$5
    fi

    if [ $6 ]
    then
        output=$6
    fi
    map=$mr/360_ContactVoice_QMap_sn_lose.pl
    red=$mr/360_ContactVoice_QRed_sn_lose.pl
    $hadoop fs -rmr $output
    $hadoop $stream \
	-D mapred.reduce.tasks=3 \
	-input $input1  \
	-input $input2  \
	-input $input3  \
        -output $output  \
        -mapper $map  \
        -reducer $red  \
        -file $map    \
        -file $red \
        -cmdenv INPUT_TYPE1="usr-log" \
        -cmdenv INPUT_TYPE1="distribute-log" \
        -cmdenv INPUT_TYPE1="pushagent-log" \
        -cmdenv INPUT_HOUR=$hour \
	-jobconf mapred.job.name="liuxianpeng_en_$day" 
    $hadoop fs -cat $output/part* | $mr/foundation_data_process.pl en $hour PEER_QLOG_STAT_INFO_2013 PEER_QLOG_MSG_TYPE_INFO_2013 PEER_QLOG_MSG_COST_INFO_2013 >> $mr/sql_script/peer_qlog_sql_$hour.sql
elif [ $type = "up-download" ]
then
    if [ $3 ]
    then
        input1=$3
    fi

    if [ $4 ]
    then
        input2=$4
    fi

    if [ $5 ]
    then
        output=$5
    fi

    map=$mr/360_updownload_Map.pl
    red=$mr/360_updownload_Red.pl
    $hadoop fs -rmr $output
    $hadoop $stream \
	-D mapred.reduce.tasks=3 \
	-input $input1  \
	-input $input2  \
        -output $output  \
        -mapper $map  \
        -reducer $red  \
        -file $map    \
        -file $red \
        -cmdenv INPUT_TYPE1="upload" \
        -cmdenv INPUT_TYPE2="download" \
        -cmdenv INPUT_HOUR=$hour  \
	-jobconf mapred.job.name="liuxianpeng_updown_$day" 
        
    $hadoop fs -cat $output/part* | $mr/foundation_data_process.pl ud $hour PEER_QLOG_STAT_INFO_2013 PEER_QLOG_MSG_TYPE_INFO_2013 PEER_QLOG_MSG_COST_INFO_2013 >> $mr/sql_script/peer_qlog_sql_$hour.sql
fi

   

