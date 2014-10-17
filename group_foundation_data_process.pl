#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $curr_hour;
my $operation;
my $mysql_table1;
my $mysql_table2;
my $mysql_table3;
if((@ARGV == 5)&&(length($ARGV[0]) == 2)&&(length($ARGV[1]) == 10))
{    
    $operation = substr($ARGV[0],0,2);
    $curr_hour = substr($ARGV[1],0,10);
    $mysql_table1 = $ARGV[2];
    $mysql_table2 = $ARGV[3];
    $mysql_table3 = $ARGV[4];
}
else
{
    print("Parameter Error! or Input Type (ss-set/set, sg-set/get, sn-set/notify) and yyyymmddhh ...\n");
    exit(0);
}

my %fail_num;
 
my %sn_set_num; 
my %sn_total_spent; 
my %sn_match; 
my %sn_max_spent; 

my %snr_total_spent; 
my %snr_match;
 
my %chatmem;
my %ss_set_num; 
my %ss_total_spent; 
my %ss_match; 
my %ss_max_spent; 
my %ss_flow; 

my %ssr_total_spent; 
my %ssr_match; 

my %ssf_msg_type; 
my %ssf_match; 
my %ssf_flow; 

my %sg_set_num; 
my %sg_launch; 
my %sg_flow; 

my %sgf_msg_type; 
my %sgf_launch; 
my %sgf_flow;
 
my %pg_total_spent; 
my %pg_match; 
my %pg_max_spent; 

my %pgr_total_spent; 
my %pgr_match; 

my %pre_array = ("100" => 0,"200" => 0,"300" => 0,"400" => 0,"500" => 0,"600" => 0,"700" => 0,"800" => 0,"900" => 0,"1000" => 0,"1100" => 0,"1200" => 0,"1300" => 0,"1400" => 0,"1500" => 0,"1600" => 0,"1700" => 0,"1800" => 0,"1900" => 0,"2000" => 0,"2100" => 0,"2200" => 0,"2300" => 0,"2400" => 0,"2500" => 0,"2600" => 0,"2700" => 0,"2800" => 0,"2900" => 0,"3000" => 0,"3001" => 0);
my %pre_array2 = ("300" => 0,"301" => 0,"302" => 0,"303" => 0,"304" => 0,"305" => 0,"306" => 0,"307" => 0,"888100" => 0,"888101" => 0,"888009" => 0,"99" => 0,"999000" => 0);

foreach my $key(keys %pre_array)
{

    $snr_match{$curr_hour . "0000:".$key} = 0;
    $snr_match{$curr_hour . "1000:".$key} = 0;
    $snr_match{$curr_hour . "2000:".$key} = 0;
    $snr_match{$curr_hour . "3000:".$key} = 0;
    $snr_match{$curr_hour . "4000:".$key} = 0;
    $snr_match{$curr_hour . "5000:".$key} = 0;

    $ssr_match{$curr_hour . "0000:".$key} = 0;
    $ssr_match{$curr_hour . "1000:".$key} = 0;
    $ssr_match{$curr_hour . "2000:".$key} = 0;
    $ssr_match{$curr_hour . "3000:".$key} = 0;
    $ssr_match{$curr_hour . "4000:".$key} = 0;
    $ssr_match{$curr_hour . "5000:".$key} = 0;

    $pgr_match{$curr_hour . "0000:".$key} = 0;
    $pgr_match{$curr_hour . "1000:".$key} = 0;
    $pgr_match{$curr_hour . "2000:".$key} = 0;
    $pgr_match{$curr_hour . "3000:".$key} = 0;
    $pgr_match{$curr_hour . "4000:".$key} = 0;
    $pgr_match{$curr_hour . "5000:".$key} = 0;

}

foreach my $key(keys %pre_array)
{

    $sgf_launch{$curr_hour . "0000:".$key} = 0; 
    $sgf_launch{$curr_hour . "1000:".$key} = 0; 
    $sgf_launch{$curr_hour . "2000:".$key} = 0; 
    $sgf_launch{$curr_hour . "3000:".$key} = 0; 
    $sgf_launch{$curr_hour . "4000:".$key} = 0; 
    $sgf_launch{$curr_hour . "5000:".$key} = 0;
 
    $ssf_match{$curr_hour . "0000:".$key} = 0; 
    $ssf_match{$curr_hour . "1000:".$key} = 0; 
    $ssf_match{$curr_hour . "2000:".$key} = 0; 
    $ssf_match{$curr_hour . "3000:".$key} = 0; 
    $ssf_match{$curr_hour . "4000:".$key} = 0; 
    $ssf_match{$curr_hour . "5000:".$key} = 0; 

}
while(<STDIN>)
{
    my $row = $_;
    chomp($row);
	
    my @data_array = split(/\t/, $row);
    my $array_len = @data_array;
    my $flag = $data_array[0];
    # 0       1            2          3         4           5            6        7          8  
    # FI    STAT_TIME    TYPES     MSG_TYPE  SET_NUM    TOTAL_SPAN   LAUNCH   MAX_SPENT     FLOW 
    
    if($array_len != 9)
    {
        next;
    }

    if($flag ne "FI")
    {
        next;
    }
    my $time = $data_array[1];
    my $type = $data_array[2];
    my $msg_type = $data_array[3];
    my $total_spent = $data_array[5];
   
    if($operation eq "sn")
    {
        if($type eq "SN")
        {
            $fail_num{$time} += $data_array[3];
            $sn_set_num{$time} += $data_array[4];
            $sn_total_spent{$time} += $data_array[5];
            $sn_match{$time} += $data_array[6];
            if($data_array[7] > $sn_max_spent{$time})
            {
                $sn_max_spent{$time} = $data_array[7];
            }
            next;     
        }

        if($type eq "SNR")
        {
            $snr_match{$time . ":" . $total_spent} += $data_array[6];
            next;     
        }
    }
    elsif($operation eq "ss")
    {
        if($type eq "SS")
        {
            $chatmem{$time} += $data_array[3];
            $ss_set_num{$time} += $data_array[4];
            $ss_total_spent{$time} += $data_array[5];
            $ss_match{$time} += $data_array[6];

            if($data_array[7] > $ss_max_spent{$time})
            {
                $ss_max_spent{$time} = $data_array[7];
            }
            $ss_flow{$time} += $data_array[8];
            next;     
        }

        if($type eq "SSR")
        {
            $ssr_match{$time . ":" . $total_spent} += $data_array[6];
            next;     
        }

        if($type eq "SSF")
        {
            $ssf_match{$time .":".$msg_type} += $data_array[6];
            $ssf_flow{$time.":".$msg_type} += $data_array[8];
            next;     
        }
    }
    elsif($operation eq "sg")
    {
        if($type eq "SG")
        {
            $sg_set_num{$time} += $data_array[4];
            $sg_launch{$time} += $data_array[6];
            $sg_flow{$time} += $data_array[8];
            next;     
        }
    
        if($type eq "SGF")
        {
            $sgf_launch{$time .":".$msg_type} += $data_array[6];
            $sgf_flow{$time.":".$msg_type} += $data_array[8];
            next;     
        }
    
    }
    elsif($operation eq "pg")
    {

        if($type eq "PG")
        {
            $pg_total_spent{$time} += $data_array[5];
            $pg_match{$time} += $data_array[6];
	    if($data_array[7] > $pg_max_spent{$time})
            {
                $pg_max_spent{$time} = $data_array[7];
            }
            next;     
        }

        if($type eq "PGR")
        {
            $pgr_match{$time . ":" . $total_spent} += $data_array[6];
            next;     
        }


    }
}

print("CREATE TABLE IF NOT EXISTS " . $mysql_table1 . " (DATES datetime, TYPES char(15), DIME char(20), VAL bigint ,constraint PK_".$mysql_table1." primary key clustered (DATES,TYPES,DIME)) ;" . "\n");
print("CREATE TABLE IF NOT EXISTS " . $mysql_table2 . " (DATES datetime, TYPES char(15), DIME char(20), TEXT bigint, VOC bigint, IMG bigint, MAPS bigint, ANIMATION  bigint, CUSTOMAPS bigint, CARD bigint, LOCATION bigint, DISTEXT bigint, DISIMG bigint, DISACK bigint, REACHACK bigint, INVITE_VOC bigint,INVITE_END bigint, constraint PK_".$mysql_table2." primary key clustered (DATES,TYPES,DIME)) ;" . "\n");
print("CREATE TABLE IF NOT EXISTS " . $mysql_table3 . " (DATES datetime, TYPES char(15), C100 int, C200 int, C300 int, C400 int, C500 int, C600 int, C700 int, C800 int, C900 int, C1000 int, C1100 int, C1200 int, C1300 int, C1400 int, C1500 int, C1600 int, C1700 int, C1800 int, C1900 int, C2000 int, C2100 int, C2200 int, C2300 int, C2400 int, C2500 int, C2600 int, C2700 int, C2800 int, C2900 int, C3000 int, C3001 int, constraint PK_".$mysql_table3." primary key clustered (DATES,TYPES)) ;" . "\n");

my %time_array;
$time_array{$curr_hour . "0000"} = 1;
$time_array{$curr_hour . "1000"} = 1;
$time_array{$curr_hour . "2000"} = 1;
$time_array{$curr_hour . "3000"} = 1;
$time_array{$curr_hour . "4000"} = 1;
$time_array{$curr_hour . "5000"} = 1;

if($operation eq "sn")
{
    my $time_key;
    foreach $time_key(keys %time_array)
    {
        if($fail_num{$time_key} eq "")
        {
            $fail_num{$time_key} = 0;
        };
        if($sn_set_num{$time_key} eq "")
        {
            $sn_set_num{$time_key} = 0;
        };

        if($sn_total_spent{$time_key} eq "")
        {
            $sn_total_spent{$time_key} = 0;
        };
    
        if($sn_match{$time_key} eq "")
        {
            $sn_match{$time_key} = 0;
        };
    
        if($sn_max_spent{$time_key} eq "")
        {
            $sn_max_spent{$time_key} = 0;
        };

        print("INSERT INTO " . $mysql_table1 . " (DATES,TYPES,DIME,VAL) VALUES (" . $time_key . "," . "\"". "SN" ."\"". "," . "\"" ."FAIL" ."\"". "," . "\"".$fail_num{$time_key}. "\"".");\n");
        print("INSERT INTO " . $mysql_table1 . " (DATES,TYPES,DIME,VAL) VALUES (" . $time_key . "," . "\"". "SN" ."\"". "," . "\"" ."SET_NUM" ."\"". "," . "\"".$sn_set_num{$time_key}. "\"".");\n");
        print("INSERT INTO " . $mysql_table1 . " (DATES,TYPES,DIME,VAL) VALUES (" . $time_key . "," . "\"". "SN" ."\"". "," . "\"" ."MATCH" ."\"". "," . "\"".$sn_match{$time_key}. "\"".");\n");
        print("INSERT INTO " . $mysql_table1 . " (DATES,TYPES,DIME,VAL) VALUES (" . $time_key . "," . "\"". "SN" ."\"". "," . "\"" ."TOTAL_SPENT" ."\"". "," . "\"".$sn_total_spent{$time_key}. "\"".");\n");
        print("INSERT INTO " . $mysql_table1 . " (DATES,TYPES,DIME,VAL) VALUES (" . $time_key . "," . "\"". "SN" ."\"". "," . "\"" ."MAX_SPENT" ."\"". "," . "\"".$sn_max_spent{$time_key}. "\"".");\n");
    }

    my $time_key;
    my $last_time = "";
    my %sections = ("100" => 0,"200" => 0,"300" => 0,"400" => 0,"500" => 0,"600" => 0,"700" => 0,"800" => 0,"900" => 0,"1000" => 0,"1100" => 0,"1200" => 0,"1300" => 0,"1400" => 0,"1500" => 0,"1600" => 0,"1700" => 0,"1800" => 0,"1900" => 0,"2000" => 0,"2100" => 0,"2200" => 0,"2300" => 0,"2400" => 0,"2500" => 0,"2600" => 0,"2700" => 0,"2800" => 0,"2900" => 0,"3000" => 0,"3001" => 0);
    
    foreach my $time_key (sort keys %snr_match)
    { 
        my @array = split(/:/,$time_key);
        my $time = $array[0];
        my $timesection = $array[1];
        
        if($last_time == "")
        {
            $sections{$timesection} = $snr_match{$time_key};  
        }
        elsif($last_time == $time)
        {
            $sections{$timesection} = $snr_match{$time_key};     
        }
        else
        {
            print("INSERT INTO " . $mysql_table3 . " (DATES,TYPES,C100,C200,C300,C400,C500,C600,C700,C800,C900,C1000,C1100,C1200,C1300,C1400,C1500,C1600,C1700,C1800,C1900,C2000,C2100,C2200,C2300,C2400,C2500,C2600,C2700,C2800,C2900,C3000,C3001) VALUES (" . $last_time . "," . "\""."SNR" ."\"". "," .  "\"". $sections{"100"} ."\"". "," . "\"". $sections{"200"} ."\"". ",". "\"". $sections{"300"} ."\"". ",". "\"". $sections{"400"} ."\"". ",". "\"". $sections{"500"} ."\"". ",". "\"". $sections{"600"} ."\"". ",". "\"". $sections{"700"} ."\"". ",". "\"". $sections{"800"} ."\"". ",". "\"". $sections{"900"} ."\"". ",". "\"". $sections{"1000"} ."\"". ",". "\"". $sections{"1100"} ."\"". ",". "\"". $sections{"1200"} ."\"". ",". "\"". $sections{"1300"} ."\"". ",". "\"". $sections{"1400"} ."\"". ",". "\"". $sections{"1500"} ."\"". ",". "\"". $sections{"1600"} ."\"". ",". "\"". $sections{"1700"} ."\"". ",". "\"". $sections{"1800"} ."\"". ",". "\"". $sections{"1900"} ."\"". ",". "\"". $sections{"2000"} ."\"". ",". "\"". $sections{"2100"} ."\"". ",". "\"". $sections{"2200"} ."\"". ",". "\"". $sections{"2300"} ."\"". ",". "\"". $sections{"2400"} ."\"". ",". "\"". $sections{"2500"} ."\"". ",".   "\"". $sections{"2600"} ."\"". ",".  "\"". $sections{"2700"} ."\"". ",".  "\"". $sections{"2800"} ."\"". ",".  "\"". $sections{"2900"} ."\"". ",". "\"". $sections{"3000"} ."\"". ",".  "\"". $sections{"3001"} ."\"". ");" . "\n");
            
            foreach my $sec (keys %sections)
            {   
                $sections{$sec} = 0;
            }
            $sections{$timesection} = $snr_match{$time_key};
        }

        $last_time = $time;
    } 
    if($last_time != "")
    {    
        print("INSERT INTO " . $mysql_table3 . " (DATES,TYPES,C100,C200,C300,C400,C500,C600,C700,C800,C900,C1000,C1100,C1200,C1300,C1400,C1500,C1600,C1700,C1800,C1900,C2000,C2100,C2200,C2300,C2400,C2500,C2600,C2700,C2800,C2900,C3000,C3001) VALUES (" . $last_time . "," . "\""."SNR" ."\"". "," .  "\"". $sections{"100"} ."\"". "," . "\"". $sections{"200"} ."\"". ",". "\"". $sections{"300"} ."\"". ",". "\"". $sections{"400"} ."\"". ",". "\"". $sections{"500"} ."\"". ",". "\"". $sections{"600"} ."\"". ",". "\"". $sections{"700"} ."\"". ",". "\"". $sections{"800"} ."\"". ",". "\"". $sections{"900"} ."\"". ",". "\"". $sections{"1000"} ."\"". ",". "\"". $sections{"1100"} ."\"". ",". "\"". $sections{"1200"} ."\"". ",". "\"". $sections{"1300"} ."\"". ",". "\"". $sections{"1400"} ."\"". ",". "\"". $sections{"1500"} ."\"". ",". "\"". $sections{"1600"} ."\"". ",". "\"". $sections{"1700"} ."\"". ",". "\"". $sections{"1800"} ."\"". ",". "\"". $sections{"1900"} ."\"". ",". "\"". $sections{"2000"} ."\"". ",". "\"". $sections{"2100"} ."\"". ",". "\"". $sections{"2200"} ."\"". ",". "\"". $sections{"2300"} ."\"". ",". "\"". $sections{"2400"} ."\"". ",". "\"". $sections{"2500"} ."\"". ",".   "\"". $sections{"2600"} ."\"". ",".  "\"". $sections{"2700"} ."\"". ",".  "\"". $sections{"2800"} ."\"". ",".  "\"". $sections{"2900"} ."\"". ",". "\"". $sections{"3000"} ."\"". ",".  "\"". $sections{"3001"} ."\"". ");" . "\n");
    }   

}

if($operation eq "ss")
{

    my $time_key;
    foreach $time_key(keys %time_array)
    {
        if($chatmem{$time_key} eq "")
        {
            $chatmem{$time_key} = 0;
        };
        if($ss_set_num{$time_key} eq "")
        {
            $ss_set_num{$time_key} = 0;
        };

        if($ss_total_spent{$time_key} eq "")
        {
            $ss_total_spent{$time_key} = 0;
        };
    
        if($ss_match{$time_key} eq "")
        {
            $ss_match{$time_key} = 0;
        };
    
        if($ss_max_spent{$time_key} eq "")
        {
            $ss_max_spent{$time_key} = 0;
        };
 
        if($ss_flow{$time_key} eq "")
        {
            $ss_flow{$time_key} = 0;
        };
 
        print("INSERT INTO " . $mysql_table1 . " (DATES,TYPES,DIME,VAL) VALUES (" . $time_key . "," . "\"". "SS" ."\"". "," . "\"" ."CHATMEM" ."\"". "," . "\"".$chatmem{$time_key}. "\"".");\n");
        print("INSERT INTO " . $mysql_table1 . " (DATES,TYPES,DIME,VAL) VALUES (" . $time_key . "," . "\"". "SS" ."\"". "," . "\"" ."SET_NUM" ."\"". "," . "\"".$ss_set_num{$time_key}. "\"".");\n");
        print("INSERT INTO " . $mysql_table1 . " (DATES,TYPES,DIME,VAL) VALUES (" . $time_key . "," . "\"". "SS" ."\"". "," . "\"" ."MATCH" ."\"". "," . "\"".$ss_match{$time_key}. "\"".");\n");
        print("INSERT INTO " . $mysql_table1 . " (DATES,TYPES,DIME,VAL) VALUES (" . $time_key . "," . "\"". "SS" ."\"". "," . "\"" ."FLOW" ."\"". "," . "\"".$ss_flow{$time_key}. "\"".");\n");
        print("INSERT INTO " . $mysql_table1 . " (DATES,TYPES,DIME,VAL) VALUES (" . $time_key . "," . "\"". "SS" ."\"". "," . "\"" ."TOTAL_SPENT" ."\"". "," . "\"".$ss_total_spent{$time_key}. "\"".");\n");
        print("INSERT INTO " . $mysql_table1 . " (DATES,TYPES,DIME,VAL) VALUES (" . $time_key . "," . "\"". "SS" ."\"". "," . "\"" ."MAX_SPENT" ."\"". "," . "\"".$ss_max_spent{$time_key}. "\"".");\n");
    }

    my $time_key;
    my $last_time = "";
    my %sections = ("100" => 0,"200" => 0,"300" => 0,"400" => 0,"500" => 0,"600" => 0,"700" => 0,"800" => 0,"900" => 0,"1000" => 0,"1100" => 0,"1200" => 0,"1300" => 0,"1400" => 0,"1500" => 0,"1600" => 0,"1700" => 0,"1800" => 0,"1900" => 0,"2000" => 0,"2100" => 0,"2200" => 0,"2300" => 0,"2400" => 0,"2500" => 0,"2600" => 0,"2700" => 0,"2800" => 0,"2900" => 0,"3000" => 0,"3001" => 0);
    
    
    foreach my $time_key (sort keys %ssr_match)
    { 
        my @array = split(/:/,$time_key);
        my $time = $array[0];
        my $timesection = $array[1];
        
        if($last_time == "")
        {
            $sections{$timesection} = $ssr_match{$time_key};  
        }
        elsif($last_time == $time)
        {
            $sections{$timesection} = $ssr_match{$time_key};     
        }
        else
        {
            print("INSERT INTO " . $mysql_table3 . " (DATES,TYPES,C100,C200,C300,C400,C500,C600,C700,C800,C900,C1000,C1100,C1200,C1300,C1400,C1500,C1600,C1700,C1800,C1900,C2000,C2100,C2200,C2300,C2400,C2500,C2600,C2700,C2800,C2900,C3000,C3001) VALUES (" . $last_time . "," . "\""."SSR" ."\"". "," .  "\"". $sections{"100"} ."\"". "," . "\"". $sections{"200"} ."\"". ",". "\"". $sections{"300"} ."\"". ",". "\"". $sections{"400"} ."\"". ",". "\"". $sections{"500"} ."\"". ",". "\"". $sections{"600"} ."\"". ",". "\"". $sections{"700"} ."\"". ",". "\"". $sections{"800"} ."\"". ",". "\"". $sections{"900"} ."\"". ",". "\"". $sections{"1000"} ."\"". ",". "\"". $sections{"1100"} ."\"". ",". "\"". $sections{"1200"} ."\"". ",". "\"". $sections{"1300"} ."\"". ",". "\"". $sections{"1400"} ."\"". ",". "\"". $sections{"1500"} ."\"". ",". "\"". $sections{"1600"} ."\"". ",". "\"". $sections{"1700"} ."\"". ",". "\"". $sections{"1800"} ."\"". ",". "\"". $sections{"1900"} ."\"". ",". "\"". $sections{"2000"} ."\"". ",". "\"". $sections{"2100"} ."\"". ",". "\"". $sections{"2200"} ."\"". ",". "\"". $sections{"2300"} ."\"". ",". "\"". $sections{"2400"} ."\"". ",". "\"". $sections{"2500"} ."\"". ",".   "\"". $sections{"2600"} ."\"". ",".  "\"". $sections{"2700"} ."\"". ",".  "\"". $sections{"2800"} ."\"". ",".  "\"". $sections{"2900"} ."\"". ",". "\"". $sections{"3000"} ."\"". ",".  "\"". $sections{"3001"} ."\"". ");" . "\n");
            
            foreach my $sec (keys %sections)
            {   
                $sections{$sec} = 0;
            }
            $sections{$timesection} = $ssr_match{$time_key};
        }

        $last_time = $time;
    } 
    
if($last_time != "")
{
    print("INSERT INTO " . $mysql_table3 . " (DATES,TYPES,C100,C200,C300,C400,C500,C600,C700,C800,C900,C1000,C1100,C1200,C1300,C1400,C1500,C1600,C1700,C1800,C1900,C2000,C2100,C2200,C2300,C2400,C2500,C2600,C2700,C2800,C2900,C3000,C3001) VALUES (" . $last_time . "," . "\""."SSR" ."\"". "," .  "\"". $sections{"100"} ."\"". "," . "\"". $sections{"200"} ."\"". ",". "\"". $sections{"300"} ."\"". ",". "\"". $sections{"400"} ."\"". ",". "\"". $sections{"500"} ."\"". ",". "\"". $sections{"600"} ."\"". ",". "\"". $sections{"700"} ."\"". ",". "\"". $sections{"800"} ."\"". ",". "\"". $sections{"900"} ."\"". ",". "\"". $sections{"1000"} ."\"". ",". "\"". $sections{"1100"} ."\"". ",". "\"". $sections{"1200"} ."\"". ",". "\"". $sections{"1300"} ."\"". ",". "\"". $sections{"1400"} ."\"". ",". "\"". $sections{"1500"} ."\"". ",". "\"". $sections{"1600"} ."\"". ",". "\"". $sections{"1700"} ."\"". ",". "\"". $sections{"1800"} ."\"". ",". "\"". $sections{"1900"} ."\"". ",". "\"". $sections{"2000"} ."\"". ",". "\"". $sections{"2100"} ."\"". ",". "\"". $sections{"2200"} ."\"". ",". "\"". $sections{"2300"} ."\"". ",". "\"". $sections{"2400"} ."\"". ",". "\"". $sections{"2500"} ."\"". ",".   "\"". $sections{"2600"} ."\"". ",".  "\"". $sections{"2700"} ."\"". ",".  "\"". $sections{"2800"} ."\"". ",".  "\"". $sections{"2900"} ."\"". ",". "\"". $sections{"3000"} ."\"". ",".  "\"". $sections{"3001"} ."\"". ");" . "\n");

}
   
    
    my $time_key;
    my $last_time = "";
    my %count = ("300" => 0,"301" => 0,"302" => 0,"303" => 0,"304" => 0,"305" => 0,"306" => 0,"307" => 0,"888100" => 0,"888101" => 0,"888009" => 0,"99" => 0,"999000" => 0);
    my %size = ("300" => 0,"301" => 0,"302" => 0,"303" => 0,"304" => 0,"305" => 0,"306" => 0,"307" => 0,"888100" => 0,"888101" => 0,"888009" => 0,"99" => 0,"999000" => 0);
    
    foreach my $time_key (sort keys %ssf_match)
    { 
        my @array = split(/:/,$time_key);
        my $time = $array[0];
        my $msg_type = $array[1];
        
        if($last_time == "")
        {
            $count{$msg_type} = $ssf_match{$time_key};  
            $size{$msg_type} = $ssf_flow{$time_key};  
        }
        elsif($last_time == $time)
        {
            $count{$msg_type} = $ssf_match{$time_key};  
            $size{$msg_type} = $ssf_flow{$time_key};  
        }
        else
        {
            print("INSERT INTO " . $mysql_table2 . " (DATES , TYPES , DIME , TEXT , VOC , IMG , MAPS , ANIMATION  , CUSTOMAPS , CARD , LOCATION , DISTEXT , DISIMG , DISACK , REACHACK , INVITE_VOC ) VALUES (" . $last_time . "," . "\""."SET" ."\"". "," .  "\"". "COUNT" ."\"". "," . "\"". $count{"300"} ."\"". "," . "\"". $count{"301"} ."\"". "," . "\"". $count{"302"} ."\"". "," . "\"". $count{"303"} ."\"". "," . "\"". $count{"304"} ."\"". "," . "\"". $count{"305"} ."\"". "," . "\"". $count{"306"} ."\"". "," . "\"". $count{"307"} ."\"". "," . "\"". $count{"888100"} ."\"". "," . "\"". $count{"888101"} ."\"". "," . "\"". $count{"888009"} ."\"". "," . "\"". $count{"99"} ."\"". "," . "\"". $count{"999000"} ."\"".  ");" . "\n");
            
            print("INSERT INTO " . $mysql_table2 . " (DATES , TYPES , DIME , TEXT , VOC , IMG , MAPS , ANIMATION  , CUSTOMAPS , CARD , LOCATION , DISTEXT , DISIMG , DISACK , REACHACK , INVITE_VOC ) VALUES (" . $last_time . "," . "\""."SET" ."\"". "," .  "\"". "SIZE" ."\"". "," . "\"". $size{"300"} ."\"". "," . "\"". $size{"301"} ."\"". "," . "\"". $size{"302"} ."\"". "," . "\"". $size{"303"} ."\"". "," . "\"". $size{"304"} ."\"". "," . "\"". $size{"305"} ."\"". "," . "\"". $size{"306"} ."\"". "," . "\"". $size{"307"} ."\"". "," . "\"". $size{"888100"} ."\"". "," . "\"". $size{"888101"} ."\"". "," . "\"". $size{"888009"} ."\"". "," . "\"". $size{"99"} ."\"". "," . "\"". $size{"999000"} ."\"".  ");" . "\n");
            
            foreach my $sec (keys %count)
            {   
                $count{$sec} = 0;
            }
            foreach my $sec (keys %count)
            {   
                $size{$sec} = 0;
            }
            
            $count{$msg_type} = $ssf_match{$time_key};  
            $size{$msg_type} = $ssf_flow{$time_key};  
        }

        $last_time = $time;
    } 
    
if($last_time != "")
{
    print("INSERT INTO " . $mysql_table2 . " (DATES , TYPES , DIME , TEXT , VOC , IMG , MAPS , ANIMATION  , CUSTOMAPS , CARD , LOCATION , DISTEXT , DISIMG , DISACK , REACHACK , INVITE_VOC ) VALUES (" . $last_time . "," . "\""."SET" ."\"". "," .  "\"". "COUNT" ."\"". "," . "\"". $count{"300"} ."\"". "," . "\"". $count{"301"} ."\"". "," . "\"". $count{"302"} ."\"". "," . "\"". $count{"303"} ."\"". "," . "\"". $count{"304"} ."\"". "," . "\"". $count{"305"} ."\"". "," . "\"". $count{"306"} ."\"". "," . "\"". $count{"307"} ."\"". "," . "\"". $count{"888100"} ."\"". "," . "\"". $count{"888101"} ."\"". "," . "\"". $count{"888009"} ."\"". "," . "\"". $count{"99"} ."\"". "," . "\"". $count{"999000"} ."\"".  ");" . "\n");
            
    print("INSERT INTO " . $mysql_table2 . " (DATES , TYPES , DIME , TEXT , VOC , IMG , MAPS , ANIMATION  , CUSTOMAPS , CARD , LOCATION , DISTEXT , DISIMG , DISACK , REACHACK , INVITE_VOC ) VALUES (" . $last_time . "," . "\""."SET" ."\"". "," .  "\"". "SIZE" ."\"". "," . "\"". $size{"300"} ."\"". "," . "\"". $size{"301"} ."\"". "," . "\"". $size{"302"} ."\"". "," . "\"". $size{"303"} ."\"". "," . "\"". $size{"304"} ."\"". "," . "\"". $size{"305"} ."\"". "," . "\"". $size{"306"} ."\"". "," . "\"". $size{"307"} ."\"". "," . "\"". $size{"888100"} ."\"". "," . "\"". $size{"888101"} ."\"". "," . "\"". $size{"888009"} ."\"". "," . "\"". $size{"99"} ."\"". "," . "\"". $size{"999000"} ."\"".  ");" . "\n");
}
}
if($operation eq "sg")
{

    my $time_key;
    foreach $time_key(keys %time_array)
    {
        if($sg_set_num{$time_key} eq "")
        {
            $sg_set_num{$time_key} = 0;
        };

        if($sg_launch{$time_key} eq "")
        {
            $sg_launch{$time_key} = 0;
        };
    
 
        if($sg_flow{$time_key} eq "")
        {
            $sg_flow{$time_key} = 0;
        };
 
        #print($time_key . "\tSG\t" . "NULL\t" . $sg_set_num{$time_key} . "\t" . "NULL" . "\t" . $sg_launch{$time_key} . "\t" . "NULL" . "\t" . $sg_flow{$time_key} . "\n");

        #print("INSERT INTO " . $mysql_table . " (STAT_TIME,TYPES,SET_NUM,TOTAL_SPAN,LAUNCH,MAX_SPENT,FLOW) VALUES (" . $time_key . "," . "SG" . "," . $sg_set_num{$time_key} . "," . "0" . "," . $sg_launch{$time_key} . "," . "0" . "," . $sg_flow{$time_key} . ");" . "\n"); 
        
        print("INSERT INTO " . $mysql_table1 . " (DATES,TYPES,DIME,VAL) VALUES (" . $time_key . "," . "\"". "SG" ."\"". "," . "\"" ."MATCH" ."\"". "," . "\"".$sg_set_num{$time_key}. "\"".");\n");
        print("INSERT INTO " . $mysql_table1 . " (DATES,TYPES,DIME,VAL) VALUES (" . $time_key . "," . "\"". "SG" ."\"". "," . "\"" ."GET_NUM" ."\"". "," . "\"".$sg_launch{$time_key}. "\"".");\n");
        print("INSERT INTO " . $mysql_table1 . " (DATES,TYPES,DIME,VAL) VALUES (" . $time_key . "," . "\"". "SG" ."\"". "," . "\"" ."FLOW" ."\"". "," . "\"".$sg_flow{$time_key}. "\"".");\n");

    }

    my $time_key;
    my $last_time = "";
    my %count = ("300" => 0,"301" => 0,"302" => 0,"303" => 0,"304" => 0,"305" => 0,"306" => 0,"307" => 0,"888100" => 0,"888101" => 0,"888009" => 0,"99" => 0,"999000" => 0,"999001" => 0);
    my %size =  ("300" => 0,"301" => 0,"302" => 0,"303" => 0,"304" => 0,"305" => 0,"306" => 0,"307" => 0,"888100" => 0,"888101" => 0,"888009" => 0,"99" => 0,"999000" => 0,"999001" => 0);
    
    foreach my $time_key (sort keys %sgf_launch)
    { 
        my @array = split(/:/,$time_key);
        my $time = $array[0];
        my $msg_type = $array[1];
        
        if($last_time == "")
        {
            $count{$msg_type} = $sgf_launch{$time_key};  
            $size{$msg_type} = $sgf_flow{$time_key};  
        }
        elsif($last_time == $time)
        {
            $count{$msg_type} = $sgf_launch{$time_key};  
            $size{$msg_type} = $sgf_flow{$time_key};  
        }
        else
        {
            print("INSERT INTO " . $mysql_table2 . " (DATES , TYPES , DIME , TEXT , VOC , IMG , MAPS , ANIMATION  , CUSTOMAPS , CARD , LOCATION , DISTEXT , DISIMG , DISACK , REACHACK , INVITE_VOC,INVITE_END ) VALUES (" . $last_time . "," . "\""."GET" ."\"". "," .  "\"". "COUNT" ."\"". "," . "\"". $count{"300"} ."\"". "," . "\"". $count{"301"} ."\"". "," . "\"". $count{"302"} ."\"". "," . "\"". $count{"303"} ."\"". "," . "\"". $count{"304"} ."\"". "," . "\"". $count{"305"} ."\"". "," . "\"". $count{"306"} ."\"". "," . "\"". $count{"307"} ."\"". "," . "\"". $count{"888100"} ."\"". "," . "\"". $count{"888101"} ."\"". "," . "\"". $count{"888009"} ."\"". "," . "\"". $count{"99"} ."\"". "," . "\"". $count{"999000"} ."\"". "," . "\"". $count{"999001"} ."\"".  ");" . "\n");
            
            print("INSERT INTO " . $mysql_table2 . " (DATES , TYPES , DIME , TEXT , VOC , IMG , MAPS , ANIMATION  , CUSTOMAPS , CARD , LOCATION , DISTEXT , DISIMG , DISACK , REACHACK , INVITE_VOC,INVITE_END ) VALUES (" . $last_time . "," . "\""."GET" ."\"". "," .  "\"". "SIZE" ."\"". "," . "\"". $size{"300"} ."\"". "," . "\"". $size{"301"} ."\"". "," . "\"". $size{"302"} ."\"". "," . "\"". $size{"303"} ."\"". "," . "\"". $size{"304"} ."\"". "," . "\"". $size{"305"} ."\"". "," . "\"". $size{"306"} ."\"". "," . "\"". $size{"307"} ."\"". "," . "\"". $size{"888100"} ."\"". "," . "\"". $size{"888101"} ."\"". "," . "\"". $size{"888009"} ."\"". "," . "\"". $size{"99"} ."\"". "," . "\"". $size{"999000"} ."\"". "," . "\"". $size{"999001"} ."\"".  ");" . "\n");
            
            foreach my $sec (keys %count)
            {   
                $count{$sec} = 0;
            }
            foreach my $sec (keys %count)
            {   
                $size{$sec} = 0;
            }
            
            $count{$msg_type} = $sgf_launch{$time_key};  
            $size{$msg_type} = $sgf_flow{$time_key};  
        }

        $last_time = $time;
    } 
    
if($last_time != "")
{
    print("INSERT INTO " . $mysql_table2 . " (DATES , TYPES , DIME , TEXT , VOC , IMG , MAPS , ANIMATION  , CUSTOMAPS , CARD , LOCATION , DISTEXT , DISIMG , DISACK , REACHACK , INVITE_VOC,INVITE_END ) VALUES (" . $last_time . "," . "\""."GET" ."\"". "," .  "\"". "COUNT" ."\"". "," . "\"". $count{"300"} ."\"". "," . "\"". $count{"301"} ."\"". "," . "\"". $count{"302"} ."\"". "," . "\"". $count{"303"} ."\"". "," . "\"". $count{"304"} ."\"". "," . "\"". $count{"305"} ."\"". "," . "\"". $count{"306"} ."\"". "," . "\"". $count{"307"} ."\"". "," . "\"". $count{"888100"} ."\"". "," . "\"". $count{"888101"} ."\"". "," . "\"". $count{"888009"} ."\"". "," . "\"". $count{"99"} ."\"". "," . "\"". $count{"999000"} ."\"". "," . "\"". $count{"999001"} ."\"".  ");" . "\n");
            
    print("INSERT INTO " . $mysql_table2 . " (DATES , TYPES , DIME , TEXT , VOC , IMG , MAPS , ANIMATION  , CUSTOMAPS , CARD , LOCATION , DISTEXT , DISIMG , DISACK , REACHACK , INVITE_VOC,INVITE_END ) VALUES (" . $last_time . "," . "\""."GET" ."\"". "," .  "\"". "SIZE" ."\"". "," . "\"". $size{"300"} ."\"". "," . "\"". $size{"301"} ."\"". "," . "\"". $size{"302"} ."\"". "," . "\"". $size{"303"} ."\"". "," . "\"". $size{"304"} ."\"". "," . "\"". $size{"305"} ."\"". "," . "\"". $size{"306"} ."\"". "," . "\"". $size{"307"} ."\"". "," . "\"". $size{"888100"} ."\"". "," . "\"". $size{"888101"} ."\"". "," . "\"". $size{"888009"} ."\"". "," . "\"". $size{"99"} ."\"". "," . "\"". $size{"999000"} ."\"". "," . "\"". $size{"999001"} ."\"".  ");" . "\n");

}

}

if($operation eq "pg")
{

    my $time_key;
    foreach $time_key(keys %time_array)
    {

        if($pg_total_spent{$time_key} eq "")
        {
            $pg_total_spent{$time_key} = 0;
        };
    
        if($pg_match{$time_key} eq "")
        {
            $pg_match{$time_key} = 0;
        };
    
        if($pg_max_spent{$time_key} eq "")
        {
            $pg_max_spent{$time_key} = 0;
        };
 
        #print($time_key . "\tPG\t" . "NULL\t" . "NULL" . "\t" . $pg_total_spent{$time_key} . "\t" . $pg_match{$time_key} . "\t" . $pg_max_spent{$time_key} . "\t" . "NULL" . "\n");

        #print("INSERT INTO " . $mysql_table . " (STAT_TIME,TYPES,SET_NUM,TOTAL_SPAN,LAUNCH,MAX_SPENT,FLOW) VALUES (" . $time_key . "," . "PG" . "," . "0" . "," . $pg_total_spent{$time_key} . "," . $pg_match{$time_key} . "," . $pg_max_spent{$time_key} . "," . "0" . ");" . "\n"); 
        print("INSERT INTO " . $mysql_table1 . " (DATES,TYPES,DIME,VAL) VALUES (" . $time_key . "," . "\"". "PG" ."\"". "," . "\"" ."MATCH" ."\"". "," . "\"".$pg_match{$time_key}. "\"".");\n");
        print("INSERT INTO " . $mysql_table1 . " (DATES,TYPES,DIME,VAL) VALUES (" . $time_key . "," . "\"". "PG" ."\"". "," . "\"" ."TOTAL_SPENT" ."\"". "," . "\"".$pg_total_spent{$time_key}. "\"".");\n");
        print("INSERT INTO " . $mysql_table1 . " (DATES,TYPES,DIME,VAL) VALUES (" . $time_key . "," . "\"". "PG" ."\"". "," . "\"" ."MAX_SPENT" ."\"". "," . "\"".$pg_max_spent{$time_key}. "\"".");\n");
    }

    my $time_key;
    my $last_time = "";
    my %sections = ("100" => 0,"200" => 0,"300" => 0,"400" => 0,"500" => 0,"600" => 0,"700" => 0,"800" => 0,"900" => 0,"1000" => 0,"1100" => 0,"1200" => 0,"1300" => 0,"1400" => 0,"1500" => 0,"1600" => 0,"1700" => 0,"1800" => 0,"1900" => 0,"2000" => 0,"2100" => 0,"2200" => 0,"2300" => 0,"2400" => 0,"2500" => 0,"2600" => 0,"2700" => 0,"2800" => 0,"2900" => 0,"3000" => 0,"3001" => 0);
    
    foreach my $time_key (sort keys %pgr_match)
    { 
        my @array = split(/:/,$time_key);
        my $time = $array[0];
        my $timesection = $array[1];
        
        if($last_time == "")
        {
            $sections{$timesection} = $pgr_match{$time_key};  
        }
        elsif($last_time == $time)
        {
            $sections{$timesection} = $pgr_match{$time_key};     
        }
        else
        {
            print("INSERT INTO " . $mysql_table3 . " (DATES,TYPES,C100,C200,C300,C400,C500,C600,C700,C800,C900,C1000,C1100,C1200,C1300,C1400,C1500,C1600,C1700,C1800,C1900,C2000,C2100,C2200,C2300,C2400,C2500,C2600,C2700,C2800,C2900,C3000,C3001) VALUES (" . $last_time . "," . "\""."PGR" ."\"". "," .  "\"". $sections{"100"} ."\"". "," . "\"". $sections{"200"} ."\"". ",". "\"". $sections{"300"} ."\"". ",". "\"". $sections{"400"} ."\"". ",". "\"". $sections{"500"} ."\"". ",". "\"". $sections{"600"} ."\"". ",". "\"". $sections{"700"} ."\"". ",". "\"". $sections{"800"} ."\"". ",". "\"". $sections{"900"} ."\"". ",". "\"". $sections{"1000"} ."\"". ",". "\"". $sections{"1100"} ."\"". ",". "\"". $sections{"1200"} ."\"". ",". "\"". $sections{"1300"} ."\"". ",". "\"". $sections{"1400"} ."\"". ",". "\"". $sections{"1500"} ."\"". ",". "\"". $sections{"1600"} ."\"". ",". "\"". $sections{"1700"} ."\"". ",". "\"". $sections{"1800"} ."\"". ",". "\"". $sections{"1900"} ."\"". ",". "\"". $sections{"2000"} ."\"". ",". "\"". $sections{"2100"} ."\"". ",". "\"". $sections{"2200"} ."\"". ",". "\"". $sections{"2300"} ."\"". ",". "\"". $sections{"2400"} ."\"". ",". "\"". $sections{"2500"} ."\"". ",".   "\"". $sections{"2600"} ."\"". ",".  "\"". $sections{"2700"} ."\"". ",".  "\"". $sections{"2800"} ."\"". ",".  "\"". $sections{"2900"} ."\"". ",". "\"". $sections{"3000"} ."\"". ",".  "\"". $sections{"3001"} ."\"". ");" . "\n");
            
            foreach my $sec (keys %sections)
            {   
                $sections{$sec} = 0;
            }
            $sections{$timesection} = $pgr_match{$time_key};
        }

        $last_time = $time;
    } 
    
if($last_time != "")
{
    print("INSERT INTO " . $mysql_table3 . " (DATES,TYPES,C100,C200,C300,C400,C500,C600,C700,C800,C900,C1000,C1100,C1200,C1300,C1400,C1500,C1600,C1700,C1800,C1900,C2000,C2100,C2200,C2300,C2400,C2500,C2600,C2700,C2800,C2900,C3000,C3001) VALUES (" . $last_time . "," . "\""."PGR" ."\"". "," .  "\"". $sections{"100"} ."\"". "," . "\"". $sections{"200"} ."\"". ",". "\"". $sections{"300"} ."\"". ",". "\"". $sections{"400"} ."\"". ",". "\"". $sections{"500"} ."\"". ",". "\"". $sections{"600"} ."\"". ",". "\"". $sections{"700"} ."\"". ",". "\"". $sections{"800"} ."\"". ",". "\"". $sections{"900"} ."\"". ",". "\"". $sections{"1000"} ."\"". ",". "\"". $sections{"1100"} ."\"". ",". "\"". $sections{"1200"} ."\"". ",". "\"". $sections{"1300"} ."\"". ",". "\"". $sections{"1400"} ."\"". ",". "\"". $sections{"1500"} ."\"". ",". "\"". $sections{"1600"} ."\"". ",". "\"". $sections{"1700"} ."\"". ",". "\"". $sections{"1800"} ."\"". ",". "\"". $sections{"1900"} ."\"". ",". "\"". $sections{"2000"} ."\"". ",". "\"". $sections{"2100"} ."\"". ",". "\"". $sections{"2200"} ."\"". ",". "\"". $sections{"2300"} ."\"". ",". "\"". $sections{"2400"} ."\"". ",". "\"". $sections{"2500"} ."\"". ",".   "\"". $sections{"2600"} ."\"". ",".  "\"". $sections{"2700"} ."\"". ",".  "\"". $sections{"2800"} ."\"". ",".  "\"". $sections{"2900"} ."\"". ",". "\"". $sections{"3000"} ."\"". ",".  "\"". $sections{"3001"} ."\"". ");" . "\n");
}


}


