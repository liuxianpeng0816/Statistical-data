#! /bin/sh

hour=`date -d "1 hour ago" +"%Y%m%d%H"`

if [ $1 ]
then
	hour=$1
fi

day=${hour:0:8}
loop=`date -d "${hour:0:4}-${hour:4:2}-${hour:6:2} ${hour:8:2}:00:00" +"%s"`
((loop-=3600))
lasthour=`date -d "1970-01-01 UTC $loop seconds" "+%Y%m%d%H"`
((loop+=7200))
nexthour=`date -d "1970-01-01 UTC $loop seconds" "+%Y%m%d%H"`

hadoop=hadoop
stream="jar /home/hdp-voice/hadoop-fb-0.20.1.9-streaming.jar"
root=/home/hdp-voice/voice

map=/home/hdp-voice/360_ContactVoice_QMap.pl
red=/home/hdp-voice/360_ContactVoice_QRed.pl

cd /home/hdp-voice/
source .bashrc

$hadoop fs -rmr $root/stat-result/QLOG_SN_$day/${hour:8:2}
$hadoop $stream -input $root/msgrouter-log/$day/*/* -output $root/stat-result/QLOG_SN_$day/${hour:8:2} -mapper "$map sn $hour" -reducer "$red sn" -file $map -file $red -jobconf mapred.job.name="liugaoshan-Cv-Qlog-SN-$hour"
res=$?
if [ $res -ne 0 ]
then
	echo "Please Check up Immediately! ..." | mail -s "$hour : qlog stat set-notify job failed!" #liugaoshan@alarm.360.cn,zhaogang@alarm.360.cn,chuyun@alarm.360.cn,liuxun@alarm.360.cn,zhangxinrun@alarm.360.cn,zhangjun@alarm.360.cn,guanjianjun@alarm.360.cn,zhangyongqiang@alarm.360.cn,zhaoyulong@alarm.360.cn
fi

$hadoop fs -rmr $root/stat-result/QLOG_SS_$day/${hour:8:2}
$hadoop $stream -input $root/msgrouter-log/$day/*/* -input $root/txl-msg/$day/*/* -output $root/stat-result/QLOG_SS_$day/${hour:8:2} -mapper "$map ss $hour" -reducer "$red ss" -file $map -file $red -jobconf mapred.job.name="liugaoshan-Cv-Qlog-SS-$hour"
res=$?
if [ $res -ne 0 ]
then
	echo "Please Check up Immediately! ..." | mail -s "$hour : qlog stat set-set job failed!" #liugaoshan@alarm.360.cn,zhaogang@alarm.360.cn,chuyun@alarm.360.cn,liuxun@alarm.360.cn,zhangxinrun@alarm.360.cn,zhangjun@alarm.360.cn,guanjianjun@alarm.360.cn,zhangyongqiang@alarm.360.cn,zhaoyulong@alarm.360.cn
fi

$hadoop fs -rmr $root/stat-result/QLOG_SG_$day/${hour:8:2}
$hadoop $stream -input $root/txl-msg/$day/*/* -output $root/stat-result/QLOG_SG_$day/${hour:8:2} -mapper "$map sg $hour" -reducer "$red sg" -file $map -file $red -jobconf mapred.job.name="liugaoshan-Cv-Qlog-SG-$hour"
res=$?
if [ $res -ne 0 ]
then
	echo "Please Check up Immediately! ..." | mail -s "$hour : qlog stat set-get job failed!" #liugaoshan@alarm.360.cn,zhaogang@alarm.360.cn,chuyun@alarm.360.cn,liuxun@alarm.360.cn,zhangxinrun@alarm.360.cn,zhangjun@alarm.360.cn,guanjianjun@alarm.360.cn,zhangyongqiang@alarm.360.cn,zhaoyulong@alarm.360.cn
fi

$hadoop fs -rmr $root/stat-result/QLOG_PG_$day/${hour:8:2}
$hadoop $stream -input $root/msgrouter-log/$day/*/* -input $root/txl-msg/$day/*/* -output $root/stat-result/QLOG_PG_$day/${hour:8:2} -mapper "$map pg $hour" -reducer "$red pg" -file $map -file $red -jobconf mapred.job.name="liugaoshan-Cv-Qlog-PG-$hour"
res=$?
if [ $res -ne 0 ]
then
	echo "Please Check up Immediately! ..." | mail -s "$hour : qlog stat prepare-get job failed!" #liugaoshan@alarm.360.cn,zhaogang@alarm.360.cn,chuyun@alarm.360.cn,liuxun@alarm.360.cn,zhangxinrun@alarm.360.cn,zhangjun@alarm.360.cn,guanjianjun@alarm.360.cn,zhangyongqiang@alarm.360.cn,zhaoyulong@alarm.360.cn
fi


minu="0000 1000 2000 3000 4000 5000"
resp="100 200 300 400 500 600 700 800 900 1000 1100 1200 1300 1400 1500 1600 1700 1800 1900 2000 2100 2200 2300 2400 2500 2600 2700 2800 2900 3000 3001"
msgt="0 1 2 3 4 5 6 7 99 200 201 202 203 204 205 300 301 302 303 304 305 306 307 1000 888000 888002 888009 888100 888101 999000"

echo "CREATE TABLE IF NOT EXISTS QLOG_STAT_"${day:0:6}" (STAT_TIME datetime, TYPES char(4), MSG_TYPE varchar(8), SET_NUM int, TOTAL_SPAN int, LAUNCH int, MAX_SPENT int, FLOW bigint) ;" > /home/hdp-voice/360_QlogStat_$hour.sql
echo "DELETE FROM QLOG_STAT_"${day:0:6}" WHERE LEFT(STAT_TIME,13) = '"${day:0:4}"-"${day:4:2}"-"${day:6:2}" "${hour:8:2}"' ;" >> /home/hdp-voice/360_QlogStat_$hour.sql

fs=`$hadoop fs -cat $root/stat-result/QLOG_SN_$day/${hour:8:2}/part-* | grep -v ER2 | wc -l`
if [ $fs -eq 0 ]
then
	for i in $minu
	do
		echo "INSERT INTO QLOG_STAT_"${day:0:6}" (STAT_TIME, TYPES, SET_NUM, TOTAL_SPAN, LAUNCH, MAX_SPENT, FLOW) VALUES ('"$hour$i"', 'SN', 0, 0, 0, 0, 0) ;" >> /home/hdp-voice/360_QlogStat_$hour.sql
	done
fi
# UN      13980615187_1380158154949_10000000_1000496      20130926105904  1380164344463   NULL    19700101080000  888009  NULL    0
# FI      15173915945_1380155625842_10000000_1000518      20130926101504  1380161704644   1380161704644   20130926101504  0       NULL     0
$hadoop fs -cat $root/stat-result/QLOG_SN_$day/${hour:8:2}/part-* | grep -v ER2 | awk '{if(($1!="")&&($4!="NULL")){a[substr($3,1,11)0]+=1;};if($1=="FI"){x=$5-$4;if(x<0){x=0};b[substr($6,1,11)0]+=x;c[substr($6,1,11)0]+=1;if(($5-$4)>d[substr($6,1,11)0]){d[substr($6,1,11)0]=$5-$4}};e['$hour'00]=1;e['$hour'10]=1;e['$hour'20]=1;e['$hour'30]=1;e['$hour'40]=1;e['$hour'50]=1}END{for(k in e){if(a[k]==""){a[k]=0};if(b[k]==""){b[k]=0};if(c[k]==""){c[k]=0};if(d[k]==""){d[k]=0};print "INSERT INTO QLOG_STAT_'${day:0:6}' (STAT_TIME, TYPES, SET_NUM, TOTAL_SPAN, LAUNCH, MAX_SPENT, FLOW) VALUES ('\''"k"00'\'', '\''SN'\'', "a[k]", "b[k]", "c[k]", "d[k]", 0) ;"}}' >> /home/hdp-voice/360_QlogStat_$hour.sql

fs=`$hadoop fs -cat $root/stat-result/QLOG_SS_$day/${hour:8:2}/part-* | grep -v ER2 | wc -l`
if [ $fs -eq 0 ]
then
	for i in $minu
	do
		echo "INSERT INTO QLOG_STAT_"${day:0:6}" (STAT_TIME, TYPES, SET_NUM, TOTAL_SPAN, LAUNCH, MAX_SPENT, FLOW) VALUES ('"$hour$i"', 'SS', 0, 0, 0, 0, 0) ;" >> /home/hdp-voice/360_QlogStat_$hour.sql
	done
fi
# FI      1000034_946812477207_10000000_1000506   20130926100514  1380161114000   1380161114000   20130926100514  99      399     49
$hadoop fs -cat $root/stat-result/QLOG_SS_$day/${hour:8:2}/part-* | grep -v ER2 | awk '{if(($1!="")&&($4!="NULL")){a[substr($3,1,11)0]+=1;f[substr($3,1,11)0]+=$9};if($1=="FI"){x=$5-$4;if(x<0){x=0};b[substr($6,1,11)0]+=x;c[substr($6,1,11)0]+=1;if(($5-$4)>d[substr($6,1,11)0]){d[substr($6,1,11)0]=$5-$4}};e['$hour'00]=1;e['$hour'10]=1;e['$hour'20]=1;e['$hour'30]=1;e['$hour'40]=1;e['$hour'50]=1}END{for(k in e){if(a[k]==""){a[k]=0};if(b[k]==""){b[k]=0};if(c[k]==""){c[k]=0};if(d[k]==""){d[k]=0};if(f[k]==""){f[k]=0};print "INSERT INTO QLOG_STAT_'${day:0:6}' (STAT_TIME, TYPES, SET_NUM, TOTAL_SPAN, LAUNCH, MAX_SPENT, FLOW) VALUES ('\''"k"00'\'', '\''SS'\'', "a[k]", "b[k]", "c[k]", "d[k]", "f[k]") ;"}}' >> /home/hdp-voice/360_QlogStat_$hour.sql
$hadoop fs -cat $root/stat-result/QLOG_SS_$day/${hour:8:2}/part-* | grep -v ER2 | awk '{a[substr($3,1,11)"000'\'', '\''"$7]+=$9;b[substr($3,1,11)"000'\'', '\''"$7]+=1;}END{for(k in a){if(a[k]==""){a[k]=0};if(b[k]==""){b[k]=0};print "INSERT INTO QLOG_STAT_'${day:0:6}' (STAT_TIME, MSG_TYPE, TYPES, LAUNCH, FLOW) VALUES ('\''"k"'\'', '\''SSF'\'', "b[k]", "a[k]") ;"}}' >> /home/hdp-voice/360_QlogStat_$hour.sql

fs=`$hadoop fs -cat $root/stat-result/QLOG_SG_$day/${hour:8:2}/part-* | grep -v ER2 | wc -l`
if [ $fs -eq 0 ]
then
	for i in $minu
	do
		echo "INSERT INTO QLOG_STAT_"${day:0:6}" (STAT_TIME, TYPES, SET_NUM, TOTAL_SPAN, LAUNCH, MAX_SPENT, FLOW) VALUES ('"$hour$i"', 'SG', 0, 0, 0, 0, 0) ;" >> /home/hdp-voice/360_QlogStat_$hour.sql
	done
fi
# UN      1000017_75_10000000_1000028     20130922104236  1379817756      NULL    NULL    888000  1379815888163   24
# UN      1000495_245_10000000_1000495    NULL    NULL    1379816903      20130922102823  0       NULL    56
# FI      1000023_30_10000000_1000518     20130922102119  1379816479      1379816479      20130922102119  0       1379813277946   64
$hadoop fs -cat $root/stat-result/QLOG_SG_$day/${hour:8:2}/part-* | grep -v ER2 | awk '{if(($4!="NULL")&&($5!="NULL")){a[substr($6,1,11)0]+=1};if($5!="NULL"){b[substr($6,1,11)0]+=1;f[substr($6,1,11)0]+=$9};c['$hour'00]=1;c['$hour'10]=1;c['$hour'20]=1;c['$hour'30]=1;c['$hour'40]=1;c['$hour'50]=1}END{for(k in c){if(a[k]==""){a[k]=0};if(b[k]==""){b[k]=0};if(f[k]==""){f[k]=0};print "INSERT INTO QLOG_STAT_'${day:0:6}' (STAT_TIME, TYPES, SET_NUM, TOTAL_SPAN, LAUNCH, MAX_SPENT, FLOW) VALUES ('\''"k"00'\'', '\''SG'\'', "a[k]", 0, "b[k]", 0, "f[k]") ;"}}' >> /home/hdp-voice/360_QlogStat_$hour.sql
$hadoop fs -cat $root/stat-result/QLOG_SG_$day/${hour:8:2}/part-* | grep -v ER2 | awk '{if($5!="NULL"){a[substr($6,1,11)"000'\'', '\''"$7]+=$9;b[substr($6,1,11)"000'\'', '\''"$7]+=1;}}END{for(k in a){if(a[k]==""){a[k]=0};if(b[k]==""){b[k]=0};print "INSERT INTO QLOG_STAT_'${day:0:6}' (STAT_TIME, MSG_TYPE, TYPES, LAUNCH, FLOW) VALUES ('\''"k"'\'', '\''SGF'\'', "b[k]", "a[k]") ;"}}' >> /home/hdp-voice/360_QlogStat_$hour.sql

fs=`$hadoop fs -cat $root/stat-result/QLOG_SN_$day/${hour:8:2}/part-* | grep -v ER2 | grep FI | wc -l`
if [ $fs -eq 0 ]
then
	for i in $resp
	do
		echo "INSERT INTO QLOG_STAT_"${day:0:6}" (STAT_TIME, TYPES, TOTAL_SPAN, LAUNCH) VALUES ('"$hour"0000', 'SNR', "$i", 0) ;" >> /home/hdp-voice/360_QlogStat_$hour.sql
	done
fi
# FI      15173915945_1380155625842_10000000_1000518      20130926101504  1380161704644   1380161704644   20130926101504  0       NULL     0
$hadoop fs -cat $root/stat-result/QLOG_SN_$day/${hour:8:2}/part-* | grep -v ER2 | grep FI | awk '{x=$5-$4;if(x<0){x=0};if(x<=100){a[100]+=1};b[100]=1;if((x>100)&&(x<=200)){a[200]+=1};b[200]=1;if((x>200)&&(x<=300)){a[300]+=1};b[300]=1;if((x>300)&&(x<=400)){a[400]+=1};b[400]=1;if((x>400)&&(x<=500)){a[500]+=1};b[500]=1;if((x>500)&&(x<=600)){a[600]+=1};b[600]=1;if((x>600)&&(x<=700)){a[700]+=1};b[700]=1;if((x>700)&&(x<=800)){a[800]+=1};b[800]=1;if((x>800)&&(x<=900)){a[900]+=1};b[900]=1;if((x>900)&&(x<=1000)){a[1000]+=1};b[1000]=1;if((x>1000)&&(x<=1100)){a[1100]+=1};b[1100]=1;if((x>1100)&&(x<=1200)){a[1200]+=1};b[1200]=1;if((x>1200)&&(x<=1300)){a[1300]+=1};b[1300]=1;if((x>1300)&&(x<=1400)){a[1400]+=1};b[1400]=1;if((x>1400)&&(x<=1500)){a[1500]+=1};b[1500]=1;if((x>1500)&&(x<=1600)){a[1600]+=1};b[1600]=1;if((x>1600)&&(x<=1700)){a[1700]+=1};b[1700]=1;if((x>1700)&&(x<=1800)){a[1800]+=1};b[1800]=1;if((x>1800)&&(x<=1900)){a[1900]+=1};b[1900]=1;if((x>1900)&&(x<=2000)){a[2000]+=1};b[2000]=1;if((x>2000)&&(x<=2100)){a[2100]+=1};b[2100]=1;if((x>2100)&&(x<=2200)){a[2200]+=1};b[2200]=1;if((x>2200)&&(x<=2300)){a[2300]+=1};b[2300]=1;if((x>2300)&&(x<=2400)){a[2400]+=1};b[2400]=1;if((x>2400)&&(x<=2500)){a[2500]+=1};b[2500]=1;if((x>2500)&&(x<=2600)){a[2600]+=1};b[2600]=1;if((x>2600)&&(x<=2700)){a[2700]+=1};b[2700]=1;if((x>2700)&&(x<=2800)){a[2800]+=1};b[2800]=1;if((x>2800)&&(x<=2900)){a[2900]+=1};b[2900]=1;if((x>2900)&&(x<=3000)){a[3000]+=1};b[3000]=1;if(x>3000){a[3001]+=1};b[3001]=1;}END{for(k in b){if(a[k]==""){a[k]=0};print "INSERT INTO QLOG_STAT_'${day:0:6}' (STAT_TIME, TYPES, TOTAL_SPAN, LAUNCH) VALUES ('\'''$hour'0000'\'', '\''SNR'\'', "k", "a[k]") ;"}}' >> /home/hdp-voice/360_QlogStat_$hour.sql

fs=`$hadoop fs -cat $root/stat-result/QLOG_SS_$day/${hour:8:2}/part-* | grep -v ER2 | grep FI | wc -l`
if [ $fs -eq 0 ]
then
	for i in $resp
	do
		echo "INSERT INTO QLOG_STAT_"${day:0:6}" (STAT_TIME, TYPES, TOTAL_SPAN, LAUNCH) VALUES ('"$hour"0000', 'SSR', "$i", 0) ;" >> /home/hdp-voice/360_QlogStat_$hour.sql
	done
fi
# FI      1000034_946812477207_10000000_1000506   20130926100514  1380161114000   1380161114000   20130926100514  99      399     49
$hadoop fs -cat $root/stat-result/QLOG_SS_$day/${hour:8:2}/part-* | grep -v ER2 | grep FI | awk '{x=$5-$4;if(x<0){x=0};if(x<=100){a[100]+=1};b[100]=1;if((x>100)&&(x<=200)){a[200]+=1};b[200]=1;if((x>200)&&(x<=300)){a[300]+=1};b[300]=1;if((x>300)&&(x<=400)){a[400]+=1};b[400]=1;if((x>400)&&(x<=500)){a[500]+=1};b[500]=1;if((x>500)&&(x<=600)){a[600]+=1};b[600]=1;if((x>600)&&(x<=700)){a[700]+=1};b[700]=1;if((x>700)&&(x<=800)){a[800]+=1};b[800]=1;if((x>800)&&(x<=900)){a[900]+=1};b[900]=1;if((x>900)&&(x<=1000)){a[1000]+=1};b[1000]=1;if((x>1000)&&(x<=1100)){a[1100]+=1};b[1100]=1;if((x>1100)&&(x<=1200)){a[1200]+=1};b[1200]=1;if((x>1200)&&(x<=1300)){a[1300]+=1};b[1300]=1;if((x>1300)&&(x<=1400)){a[1400]+=1};b[1400]=1;if((x>1400)&&(x<=1500)){a[1500]+=1};b[1500]=1;if((x>1500)&&(x<=1600)){a[1600]+=1};b[1600]=1;if((x>1600)&&(x<=1700)){a[1700]+=1};b[1700]=1;if((x>1700)&&(x<=1800)){a[1800]+=1};b[1800]=1;if((x>1800)&&(x<=1900)){a[1900]+=1};b[1900]=1;if((x>1900)&&(x<=2000)){a[2000]+=1};b[2000]=1;if((x>2000)&&(x<=2100)){a[2100]+=1};b[2100]=1;if((x>2100)&&(x<=2200)){a[2200]+=1};b[2200]=1;if((x>2200)&&(x<=2300)){a[2300]+=1};b[2300]=1;if((x>2300)&&(x<=2400)){a[2400]+=1};b[2400]=1;if((x>2400)&&(x<=2500)){a[2500]+=1};b[2500]=1;if((x>2500)&&(x<=2600)){a[2600]+=1};b[2600]=1;if((x>2600)&&(x<=2700)){a[2700]+=1};b[2700]=1;if((x>2700)&&(x<=2800)){a[2800]+=1};b[2800]=1;if((x>2800)&&(x<=2900)){a[2900]+=1};b[2900]=1;if((x>2900)&&(x<=3000)){a[3000]+=1};b[3000]=1;if(x>3000){a[3001]+=1};b[3001]=1;}END{for(k in b){if(a[k]==""){a[k]=0};print "INSERT INTO QLOG_STAT_'${day:0:6}' (STAT_TIME, TYPES, TOTAL_SPAN, LAUNCH) VALUES ('\'''$hour'0000'\'', '\''SSR'\'', "k", "a[k]") ;"}}' >> /home/hdp-voice/360_QlogStat_$hour.sql

# FI      1380247100873_10000000_1000506  20130927105848  1380250728906   1380250728911   20130927105848  2       435     2
$hadoop fs -cat $root/stat-result/QLOG_PG_$day/${hour:8:2}/part-* | grep -v ER2 | awk '{if(($1=="FI")&&($9>0)){x=int(($5-$4)/$9);if(x<0){x=0};a[substr($6,1,11)0]+=1;if(x>b[substr($6,1,11)0]){b[substr($6,1,11)0]=x};c[substr($6,1,11)0]+=x;e['$hour'00]=1;e['$hour'10]=1;e['$hour'20]=1;e['$hour'30]=1;e['$hour'40]=1;e['$hour'50]=1}}END{for(k in e){if(a[k]==""){a[k]=0};if(b[k]==""){b[k]=0};if(c[k]==""){c[k]=0};print "INSERT INTO QLOG_STAT_'${day:0:6}' (STAT_TIME, TYPES, SET_NUM, TOTAL_SPAN, LAUNCH, MAX_SPENT, FLOW) VALUES ('\''"k"00'\'', '\''PG'\'', 0, "c[k]", "a[k]", "b[k]", 0) ;"}}' >> /home/hdp-voice/360_QlogStat_$hour.sql
$hadoop fs -cat $root/stat-result/QLOG_PG_$day/${hour:8:2}/part-* | grep -v ER2 | awk '{if(($1=="FI")&&($9>0)){x=int(($5-$4)/$9);if(x<0){x=0};if(x<=100){a[100]+=1};b[100]=1;if((x>100)&&(x<=200)){a[200]+=1};b[200]=1;if((x>200)&&(x<=300)){a[300]+=1};b[300]=1;if((x>300)&&(x<=400)){a[400]+=1};b[400]=1;if((x>400)&&(x<=500)){a[500]+=1};b[500]=1;if((x>500)&&(x<=600)){a[600]+=1};b[600]=1;if((x>600)&&(x<=700)){a[700]+=1};b[700]=1;if((x>700)&&(x<=800)){a[800]+=1};b[800]=1;if((x>800)&&(x<=900)){a[900]+=1};b[900]=1;if((x>900)&&(x<=1000)){a[1000]+=1};b[1000]=1;if((x>1000)&&(x<=1100)){a[1100]+=1};b[1100]=1;if((x>1100)&&(x<=1200)){a[1200]+=1};b[1200]=1;if((x>1200)&&(x<=1300)){a[1300]+=1};b[1300]=1;if((x>1300)&&(x<=1400)){a[1400]+=1};b[1400]=1;if((x>1400)&&(x<=1500)){a[1500]+=1};b[1500]=1;if((x>1500)&&(x<=1600)){a[1600]+=1};b[1600]=1;if((x>1600)&&(x<=1700)){a[1700]+=1};b[1700]=1;if((x>1700)&&(x<=1800)){a[1800]+=1};b[1800]=1;if((x>1800)&&(x<=1900)){a[1900]+=1};b[1900]=1;if((x>1900)&&(x<=2000)){a[2000]+=1};b[2000]=1;if((x>2000)&&(x<=2100)){a[2100]+=1};b[2100]=1;if((x>2100)&&(x<=2200)){a[2200]+=1};b[2200]=1;if((x>2200)&&(x<=2300)){a[2300]+=1};b[2300]=1;if((x>2300)&&(x<=2400)){a[2400]+=1};b[2400]=1;if((x>2400)&&(x<=2500)){a[2500]+=1};b[2500]=1;if((x>2500)&&(x<=2600)){a[2600]+=1};b[2600]=1;if((x>2600)&&(x<=2700)){a[2700]+=1};b[2700]=1;if((x>2700)&&(x<=2800)){a[2800]+=1};b[2800]=1;if((x>2800)&&(x<=2900)){a[2900]+=1};b[2900]=1;if((x>2900)&&(x<=3000)){a[3000]+=1};b[3000]=1;if(x>3000){a[3001]+=1};b[3001]=1;}}END{for(k in b){if(a[k]==""){a[k]=0};print "INSERT INTO QLOG_STAT_'${day:0:6}' (STAT_TIME, TYPES, TOTAL_SPAN, LAUNCH) VALUES ('\'''$hour'0000'\'', '\''PGR'\'', "k", "a[k]") ;"}}' >> /home/hdp-voice/360_QlogStat_$hour.sql

scp /home/hdp-voice/360_QlogStat_$hour.sql hdp-voice@211.151.122.96:/home/hdp-voice/

echo ${hour:0:4}"-"${hour:4:2}"-"${hour:6:2}" "${hour:8:2}":00:00 ~ "${hour:0:4}"-"${hour:4:2}"-"${hour:6:2}" "${hour:8:2}":59:59 qlog ��Ӧͳ��:"
$hadoop fs -cat $root/stat-result/QLOG_SN_$day/${hour:8:2}/part-* | grep -v ER2 | awk '{if($1=="FI"){x=$5-$4;if(x<0){x=0};b+=x;c+=1;if(x>d){d=x}}}END{if(b==""){b=0};if(c==""){c=0};if(d==""){d=0}}END{print "(1) ϵͳ����֪ͨ�� "c", �ܺ�ʱ "b"����, ��������ʱ "d"����; "}'
$hadoop fs -cat $root/stat-result/QLOG_SS_$day/${hour:8:2}/part-* | grep -v ER2 | awk '{if(($1!="")&&($4!="NULL")){a+=1};if($1=="FI"){x=$5-$4;if(x<0){x=0};b+=x;c+=1;if(x>d){d=x}}}END{if(a==""){a=0};if(b==""){b=0};if(c==""){c=0};if(d==""){d=0}}END{print "(2) ϵͳ�յ���Ϣ�� "a", �������ݿ���Ϣ�� "c", ��ʧ��Ϣ�� "(a-c)", �ܺ�ʱ "b"����, ��������ʱ "d"����; "}'
echo " ��������Ϣ��������(B):"
$hadoop fs -cat $root/stat-result/QLOG_SS_$day/${hour:8:2}/part-* | grep -v ER2 | awk '{if(($1!="")&&($4!="NULL")){a[$7]+=1;b[$7]+=$9}}END{for(k in a){print "  "k"\t"a[k]"\t"b[k]}}'
$hadoop fs -cat $root/stat-result/QLOG_SG_$day/${hour:8:2}/part-* | grep -v ER2 | awk '{if(($4!="NULL")&&($5!="NULL")){a+=1};if($5!="NULL"){b+=1}}END{if(a==""){a=0};if(b==""){b=0}}END{print "(3) ϵͳ������Ϣ�� "b", ���к�����ϵͳ�յ���Ϣ�� "a"."}'
echo
echo
echo "[��] ��Ϣ����˵��"
echo "########################################"
echo "0	�ı�"
echo "1	����"
echo "2	ͼƬ"
echo "3	��ͼ - ���õ���Դö��"
echo "4	����"
echo "5	�û��Զ�����ͼ - �û�����"
echo "6	��Ƭ"
echo "7	����λ��"
echo "99	��Ϣ���ʹ��ִ"
echo "200	Ⱥ֪ͨ��Ϣ - �³�Ա֪ͨ"
echo "201	Ⱥ֪ͨ��Ϣ - ��Ա�˳�֪ͨ"
echo "202	Ⱥ֪ͨ��Ϣ - ��Ա����Ⱥ��Ƭ֪ͨ"
echo "203	Ⱥ֪ͨ��Ϣ - ����Ⱥ��Ϣ֪ͨ"
echo "204	Ⱥ֪ͨ��Ϣ - ����Ⱥ֪ͨ"
echo "205	Ⱥ֪ͨ��Ϣ - �ߵ���Ա֪ͨ"
echo "300	Ⱥ��Ϣ - �ı�"
echo "301	Ⱥ��Ϣ - ����"
echo "302	Ⱥ��Ϣ - ͼƬ"
echo "303	Ⱥ��Ϣ - ��ͼ - ���õ���Դö��"
echo "304	Ⱥ��Ϣ - ����"
echo "305	Ⱥ��Ϣ - �û��Զ�����ͼ - �û�����"
echo "306	Ⱥ��Ϣ - ��Ƭ"
echo "307	Ⱥ��Ϣ - ����λ��"
echo "1000	ע��͸��������޸ĵķ���֪ͨ"
echo "888000	�ĺ󼴷� - �ı�"
echo "888002	�ĺ󼴷� - ͼƬ"
echo "888009	�ĺ󼴷� - ��ִ"
echo "888100	�ĺ󼴷� - Ⱥ���ı�"
echo "888101	�ĺ󼴷� - Ⱥ��ͼƬ"
echo "999000	ʵʱ���� �C ��������뵥��"

# Last Version 2013-10-12 15:07
