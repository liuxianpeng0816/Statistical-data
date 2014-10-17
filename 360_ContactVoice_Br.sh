#! /bin/sh

start=`date +"%s"`
hour=`date -d "1 hour ago" +"%Y%m%d%H"`

if [ $1 ]
then
	hour=$1
fi

hadoop=hadoop
stream="jar /home/hdp-voice/hadoop-fb-0.20.1.9-streaming.jar"
root=/home/hdp-voice/voice
br=/home/hdp-voice/base64rc4
proc=/home/hdp-voice

cd /home/hdp-voice/
source .bashrc

type=`cat $proc/360_ContactVoice_Br.cfg | grep -v "^#" | awk -F'|' '{print $1}'`
if [ $2 ]
then
	type=$2
fi

for t in $type
do
{
	# groupdump|/home/hdp-voice/voice/rc4-base64-log/group-dump|/home/hdp-voice/voice/group-dump|vnet:1,bjcc:1|1|23
	# client|/home/hdp-voice/voice/rc4-base64-log/client-log|/home/hdp-voice/voice/client-log|vnet:1,bjcc:1|1
	
	src=`cat $proc/360_ContactVoice_Br.cfg | grep "^$t" |  grep -v "^#" | awk -F'|' '{print $1,$2,$3,$4,$5,$6}' | cut -d ' ' -f 2`
	des=`cat $proc/360_ContactVoice_Br.cfg | grep "^$t" |  grep -v "^#" | awk -F'|' '{print $1,$2,$3,$4,$5,$6}' | cut -d ' ' -f 3`
	verfs=`cat $proc/360_ContactVoice_Br.cfg | grep "^$t" |  grep -v "^#" | awk -F'|' '{print $1,$2,$3,$4,$5,$6}' | cut -d ' ' -f 4`
	part=`cat $proc/360_ContactVoice_Br.cfg | grep "^$t" |  grep -v "^#" | awk -F'|' '{print $1,$2,$3,$4,$5,$6}' | cut -d ' ' -f 5`
	once=`cat $proc/360_ContactVoice_Br.cfg | grep "^$t" |  grep -v "^#" | awk -F'|' '{print $1,$2,$3,$4,$5,$6}' | cut -d ' ' -f 6`
	
	ver_vnet_n=`echo $verfs | awk -F',' '{a[$1]=1;a[$2]=1}END{for(k in a){print k}}' | grep vnet | cut -d ':' -f 2`
	ver_bjcc_n=`echo $verfs | awk -F',' '{a[$1]=1;a[$2]=1}END{for(k in a){print k}}' | grep bjcc | cut -d ':' -f 2`
	
	date2=$hour
	hour2=${hour:8:2}
	
	if [ $once ]
	then
		if [ $hour2 -ne $once ]
		then
			continue
		fi
		
		hour2=""
		date2=${hour:0:8}
	fi
	
	while [ 1 ]
	do
		vern_vnet=`$hadoop fs -ls $src/${hour:0:8}/*_$hour.fi | grep vnet | grep ".fi" | wc -l`
		vern_bjcc=`$hadoop fs -ls $src/${hour:0:8}/*_$hour.fi | grep bjcc | grep ".fi" | wc -l`
		
		if [ $vern_vnet -eq $ver_vnet_n -o $vern_bjcc -eq $ver_bjcc_n ]
		then
			$hadoop fs -rmr $des/${hour:0:8}/$hour2
			$hadoop $stream -D mapred.reduce.tasks=$part -input $src/${hour:0:8}/rb-*_$date2.log -output $des/${hour:0:8}/$hour2 -mapper $br -file $br -jobconf mapred.job.name="liuxianpeng-$t-Br-$date2"
			
			parts=`$hadoop fs -ls $des/${hour:0:8}/$hour2/ | grep part | awk '{print $8}'`
			for p in $parts
			do
				$hadoop fs -mv $p ${p/part/$t}.$date2.log
			done
			
			break
		fi
		
		now=`date +"%s"`
		((span=$now-$start))
		
		if [ $span -gt 3600 ]
		then
			echo "Please Check up Immediately! ..." | mail -s "$date2 : RC4 Logs of $t Are Loss or Not Prepared!"liuxianpeng@alarm.360.cn
			break
		fi
		
		sleep 30s
	done
} &
done

# Last Version 2013-11-15 17:19
