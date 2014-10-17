#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my %distribute_set_num;
my %distribute_fail_num;
my %pushagent_fail_num;
while(<STDIN>)
{
    #distribute|set	1	19700101080000
    #distribute|fail	2	19700101080000
    #pushagent|set	1	19700101080000
    #pushagent|fail	3	19700101080000
    #nr_peer	3	201312231010
    #nr_group	4	201312241520
    my $row = $_;
    chomp($row);
	
    my @r = split(/\t/, $row);
    
    my $array_len = @r;
    if( $array_len != 3 )
    {
        print($row."\n");
        next;
    }
    if( $r[0] ne "nr_peer" && $r[0] ne "nr_group" )
    {
        
        my @ra = split(/\|/, $r[0]);
        if( $ra[0] eq "distribute" )
        {
            if( $ra[1] eq "set" )
            {
              $distribute_set_num{$r[2]} += $r[1];
            }
            elsif( $ra[1] eq "fail" )
            {
              $distribute_fail_num{$r[2]} += $r[1];
            }

        }
        elsif( $ra[0] eq "pushagent" )
        {
           if( $ra[1] eq "fail" )
           {
              $pushagent_fail_num{$r[2]} += $r[1];
           }
        }
    
    }
    else
    {
 	print($row."\n");
    }	
	
}
my $timekey; 
foreach $timekey(keys %distribute_set_num)
{
    print("nr_distribute"."\t"."set"."\t".$timekey."\t".$distribute_set_num{$timekey}."\n");
 
}
foreach $timekey(keys %distribute_fail_num)
{
    print("nr_distribute"."\t"."fail"."\t".$timekey."\t".$distribute_fail_num{$timekey}."\n");
}
foreach $timekey(keys %pushagent_fail_num)
{
    print("nr_pushagent"."\t"."fail"."\t".$timekey."\t".$pushagent_fail_num{$timekey}."\n");
}

