#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $last_flag = "";
#$total 表示使用弹窗或者主程序人数
my $total;

my %kv;
my %utimes;
my %ptimes;
my %client_versions;
my $date;
my $flag = 0;
my $use_times = 0;
my $pop_times = 0;

my %timesarry =("p1"=>1,"p2"=>2,"p3"=>3,"p4"=>4,"p5"=>5,"p6"=>6,"p7"=>7,"p8"=>8,"p9"=>9,"p10"=>10,"p11"=>11,"p12"=>12,"p13"=>13,"p14"=>14,"p15"=>15,"p16"=>16,"p17"=>17,"p18"=>18,"p19"=>19,"p20"=>20,"p21"=>21,"p22"=>22,"p23"=>23,"p24"=>24,"p25"=>25,"p26"=>26,"p27"=>27,"p28"=>28,"p29"=>29,"p30"=>30);

while(<STDIN>)
{
    my $row = $_;
    chomp($row);
	
    my @r = split(/\t/, $row);
    my @t = split (/\|/,$r[0]);
	
    if($t[0] eq "STAT")
    {
        $date = $r[2];
        #$r[1] =~ s/\t//g;
        if(substr($t[1],0,3) eq "MAX")
        {
            if($r[1] > $kv{$t[1]})
            {
	    	$kv{$t[1]} = $r[1];	
            }
        }
        else
        {
            $kv{$t[1]} += $r[1];	
        }
		
        $flag = 0;
    }
    elsif($t[0] eq "PERUSETIME")
    {
        $date = $r[2];
        if(($last_flag ne $r[0])&&($last_flag ne "")&&($flag == 3))
        {
            foreach my $key (keys %timesarry) 
            {
                if($use_times == $timesarry{$key})
                {
                    $utimes{$key} += 1;
                }
            }
            if($use_times > 30)
            {
                $utimes{"p3x"} += 1;
            }
            $use_times = 0;
        }
        $use_times += $r[1];
        $flag = 3;
        $last_flag = $r[0];
    }
    elsif($t[0] eq "PERPOPTIME")
    {
        $date = $r[2];
	
        if(($last_flag ne $r[0])&&($last_flag ne "")&&($flag == 4))
        {
            foreach my $key (keys %timesarry) 
            {
                if($pop_times == $timesarry{$key})
                {
                    $ptimes{$key} += 1;
                }
            }
            $pop_times = 0;
        }
        if($pop_times > 30)
        {
            $ptimes{"p3x"} += 1;
        }
	
        $pop_times += $r[1];
        $flag = 4;
        $last_flag = $r[0];
    }
    elsif($t[0] eq "CID")
    {
        $date = $r[1];
        if(($last_flag ne $r[0])&&($last_flag ne "")&&($flag == 2))
        {
            $client_versions{$t[2]} += 1;
        }
		
        $last_flag = $r[0];
        $flag = 2;
    }
    else
    { 
        $date = $r[1];
        if(($last_flag ne $r[0])&&($last_flag ne "")&&($flag == 1))
        {
            $kv{$t[0]} += 1;
        }
        $last_flag = $r[0];
        $flag = 1;
    }
}

if($last_flag ne "")
{
    my @tt = split (/\|/,$last_flag);
    if($flag == 1)
    {
        $kv{$tt[0]} += 1;
    }
    if($flag == 2)
    {
        $client_versions{$tt[2]} += 1;
    }
    if($flag == 3)
    {
        foreach my $key (keys %timesarry) 
        {
            if($use_times == $timesarry{$key})
            {
                $utimes{$key} += 1;
            }

        }
        if($use_times > 30)
        {
            $utimes{"p3x"} += 1;
        }
	
    }
    if($flag == 4)
    {
        foreach my $key (keys %timesarry) 
        {
            if($pop_times == $timesarry{$key})
            {
                $ptimes{$key} += 1;
            }
        }
        if($pop_times > 30)
        {
            $ptimes{"p3x"} += 1;
        }
	
    }
}


for my $k (keys %kv)
{
    print($k."\t".$kv{$k}."\t".$date."\n");
}
#USETIME分次数统计
for my $k (keys %utimes)
{
    print("USETIMES"."\t".$k."\t".$utimes{$k}."\t".$date."\n");
}
#POPTIME分次数统计
for my $k (keys %ptimes)
{
    print("POPTIMES"."\t".$k."\t".$ptimes{$k}."\t".$date."\n");
}

for my $k (keys %client_versions)
{
    print("CID"."\t".$k."\t".$client_versions{$k}."\t".$date."\n");
}

# Last Version 
