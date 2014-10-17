#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my %up_maxtime;
my %up_totalnum;
my %up_totaltime;
my %up_totalsize;
my %down_success_maxtime;
my %down_success_totalnum;
my %down_success_totaltime;
my %down_success_totalsize;
my %down_error_totalnum;
my @r;
while(<STDIN>)
{
    # upload|201312191120	1119699	119357
    # download|201312191120	1119699	119357
    my $row = $_;
    chomp($row);
    @r = split(/\t/, $row);
    my @devide = split(/\|/,$r[0]); 
    if( $devide[0] eq "upload")
    {
      if( $r[1] > $up_maxtime{$devide[1]} )
      {
        $up_maxtime{$devide[1]} = $r[1]; 
      }

      $up_totalnum{$devide[1]} += 1;
      $up_totaltime{$devide[1]} += $r[1];
      $up_totalsize{$devide[1]} += $r[2];
    }
    elsif( $devide[0] eq "download_success")
    {
      if( $r[1] > $down_success_maxtime{$devide[1]} )
      {
        $down_success_maxtime{$devide[1]} = $r[1]; 
      }

      $down_success_totalnum{$devide[1]} += 1;
      $down_success_totaltime{$devide[1]} += $r[1];
      $down_success_totalsize{$devide[1]} += $r[2];

    }
    elsif( $devide[0] eq "download_error")
    {
      $down_error_totalnum{$devide[1]} += 1;
    }
	
	
}
my $timekey; 
foreach $timekey(keys %up_totaltime)
{
    print("upload"."\t".$timekey."\t".$up_totaltime{$timekey}."\t".$up_totalsize{$timekey}."\t".$up_maxtime{$timekey}."\t".$up_totalnum{$timekey}."\n");
}
foreach $timekey(keys %down_success_totaltime)
{
    print("download_success"."\t".$timekey."\t".$down_success_totaltime{$timekey}."\t".$down_success_totalsize{$timekey}."\t".$down_success_maxtime{$timekey}."\t".$down_success_totalnum{$timekey}."\n");
}
foreach $timekey(keys %down_error_totalnum)
{
    print("download_error"."\t".$timekey."\t"."NULL"."\t"."NULL"."\t"."NULL"."\t".$down_error_totalnum{$timekey}."\n");
}
