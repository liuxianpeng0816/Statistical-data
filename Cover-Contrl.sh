#! /bin/sh

root=/home/hdp-voice/
timehour=(00 01 02 03 04 05 06 07 08 09 10 11 12) 
#timehour=(20140101 20140102 20140103 20140104 20140105 20140106)
i=0
length=${#timehour[@]}
while [ $i -lt $length ]
do
    time=${timehour[$i]}
    $root/360_ContactVoice_Br.sh 20140601$time qiaoactive
    #$root/calculate.sh $time
    let i++
done
