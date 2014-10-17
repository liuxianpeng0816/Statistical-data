#! /bin/sh

hadoop=hadoop
stream="jar /home/hdp-voice/hadoop-fb-0.20.1.9-streaming.jar"
root=/home/hdp-voice/voice
mr=/home/hdp-voice/

cd /home/hdp-voice/
source .bashrc

if [ $# != 4 ] && [ $# != 5 ]; 
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

    map=$mr/360_ContactVoice_QMap_GROUP_sn.pl
    red=$mr/360_ContactVoice_QRed_GROUP_sn.pl
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
        -cmdenv INPUT_TYPE2="groupserver" \
        -cmdenv INPUT_HOUR=$hour \
	-jobconf mapred.job.name="liuxianpeng_group_sn_$day" 
    $hadoop fs -cat $output/part* | $mr/group_foundation_data_process.pl sn $hour GROUP_QLOG_STAT_INFO_2013 GROUP_QLOG_MSG_TYPE_INFO_2013 GROUP_QLOG_MSG_COST_INFO_2013 >> $mr/sql_script/group_qlog_sql_$hour.sql

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

    map=$mr/360_ContactVoice_QMap_GROUP_ss.pl
    red=$mr/360_ContactVoice_QRed_GROUP_ss.pl
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
        -cmdenv INPUT_TYPE2="groupdb" \
        -cmdenv INPUT_HOUR=$hour  \
	-jobconf mapred.job.name="liuxianpeng_group_ss_$day" 
    $hadoop fs -cat $output/part* | $mr/group_foundation_data_process.pl ss $hour GROUP_QLOG_STAT_INFO_2013 GROUP_QLOG_MSG_TYPE_INFO_2013 GROUP_QLOG_MSG_COST_INFO_2013 >> $mr/sql_script/group_qlog_sql_$hour.sql
	
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

    map=$mr/360_ContactVoice_QMap_GROUP_sg.pl
    red=$mr/360_ContactVoice_QRed_GROUP_sg.pl
    $hadoop fs -rmr $output
    $hadoop $stream \
	-D mapred.reduce.tasks=3 \
	-input $input  \
        -output $output  \
        -mapper $map  \
        -reducer $red  \
        -file $map    \
        -file $red \
        -cmdenv INPUT_TYPE1="groupdb" \
        -cmdenv INPUT_HOUR=$hour \
	-jobconf mapred.job.name="liuxianpeng_group_sg_$day" 

    $hadoop fs -cat $output/part* | ./group_foundation_data_process.pl sg $hour GROUP_QLOG_STAT_INFO_2013 GROUP_QLOG_MSG_TYPE_INFO_2013 GROUP_QLOG_MSG_COST_INFO_2013 >> $mr/sql_script/group_qlog_sql_$hour.sql
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

    map=$mr/360_ContactVoice_QMap_GROUP_pg.pl
    red=$mr/360_ContactVoice_QRed_GROUP_pg.pl

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
        -cmdenv INPUT_TYPE2="groupdb" \
        -cmdenv INPUT_HOUR=$hour \
	-jobconf mapred.job.name="liuxianpeng_group_pg_$day" 
	$hadoop fs -cat $output/part* | ./group_foundation_data_process.pl pg $hour GROUP_QLOG_STAT_INFO_2013 GROUP_QLOG_MSG_TYPE_INFO_2013 GROUP_QLOG_MSG_COST_INFO_2013 >> $mr/sql_script/group_qlog_sql_$hour.sql
fi
   

