#! /bin/sh

day=`date -d "1 days ago" +"%Y%m%d"`

if [ $1 ]
then
    day=$1
fi

cd /home/hdp-voice/
source .bashrc


hadoop="hadoop"
stream="jar /home/hdp-voice/hadoop-fb-0.20.1.9-streaming.jar"

mr="/home/hdp-voice/"
root="/home/hdp-voice/voice"
result="/home/hdp-voice/voice/stat-result-wy"

time=`date +%Y%m%d%H`
day=${time:0:8}
hour=${time:8:2}

if [ $1 ]
then 
	time=$1
	day=${time:0:8}
	hour=${time:8:2}
fi

if [ $hour -eq 00 ];
then
        day=`date -d "1 days ago" +"%Y%m%d"`
        hour1=23
	hour2=22
	hour3=21
	hour4=20
	
fi

if [ $hour -eq 04 ];
then
        hour1=03
	hour2=02
	hour3=01
	hour4=00
	
fi

if [ $hour -eq 08 ];
then
        hour1=07
	hour2=06
	hour3=05
	hour4=04
	
fi

if [ $hour -eq 12 ];
then
        hour1=11
	hour2=10
	hour3=09
	hour4=08
	
fi

if [ $hour -eq 16 ];
then
        hour1=15
	hour2=14
	hour3=13
	hour4=12
	
fi

if [ $hour -eq 20 ];
then
        hour1=19
	hour2=18
	hour3=17
	hour4=16
	
fi

sqlhour=`date -d "1 hour ago" +"%Y%m%d%H"`

cd /home/hdp-voice/
source .bashrc


hadoop="hadoop"
stream="jar /home/hdp-voice/hadoop-fb-0.20.1.9-streaming.jar"

mr="/home/hdp-voice/"
root="/home/hdp-voice/voice"
result="/home/hdp-voice/voice/stat-result-wy"

rm -rf ShareMessageSaveKey.$time
$hadoop fs -rmr $result/MSKSHAREKEY_$time

$hadoop $stream  \
		-D mapred.reduce.tasks=3 \
		-input $root/msksharemessage/$day/$hour1/*  \
		-input $root/msksharemessage/$day/$hour2/*  \
		-input $root/msksharemessage/$day/$hour3/*  \
		-input $root/msksharemessage/$day/$hour4/*  \
		-output $result/MSKSHAREKEY_$time  \
		-mapper $mr/msk_sharemessagekey_Map.pl    \
		-reducer $mr/msk_sharemessagekey_Red.pl  \
		-file $mr/msk_sharemessagekey_Map.pl \
		-file $mr/msk_sharemessagekey_Red.pl \
		-cmdenv CURRENT_DATE=$time \
		-jobconf mapred.job.name="liuxianpeng_MskShareKeyStat_$time"

$hadoop fs -cat $result/MSKSHAREKEY_$time/part* >>ShareMessageSaveKey.$time
