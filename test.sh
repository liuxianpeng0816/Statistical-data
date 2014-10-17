#! /bin/sh

time=`date +%Y%m%d%H`
year=${time:0:4}
month=${time:4:2}
day=${time:6:2}
hour=${time:8:2}

if [ $1 ]
then 
	time=$1
	day=${time:0:8}
	hour=${time:8:2}
fi

if [ $hour -eq 00 ];
then
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
echo $hour1
echo $hour2
echo $hour3
echo $hour4
#echo $year
#echo $month
#echo $day
#echo $hour
#MyselectTime=`date -d "1 hours ago" +"\%Y\%m\%d\%H"`
#MyselectTime=$year"-"$month"-"$day" "$hour
#echo $MyselectTime
#echo "cat /home/hdp-voice/chat_calculate_sql_`date -d "1 hours ago" +"%Y%m%d%H"`.sql"

