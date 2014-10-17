#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $current_date = $ENV{'CURRENT_DATE'};
#my $current_date ='20140522';
my $last_online_jid = "";
my $online_time = 0;
my $online_number = 0;
#ChatOnline	1211  10
while(<STDIN>)
{
    my $row = $_;
    chomp($row);
		
    my @r = split(/\t/, $row);
    if($r[0] ne $last_online_jid && $last_online_jid ne "")
    {
        $online_number += 1;
    }
    $online_time += $r[1];
    $last_online_jid = $r[0];
}

    if($last_online_jid ne "")
    {
        $online_number += 1;
    }
print("CHATONLINE"."\t".$current_date."\t".$online_time."\t".$online_number."\n");
