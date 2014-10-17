#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

$| = 1;

while(<STDIN>)
{
        #201312231010|peer|3
	#201312231010|group|3
	#2013-12-20 18:55:10 [distribute] [PID=6261] [INFO ] [c_src/qlog_driver.cc:95] [info] [<0.164.0>:distribute_server:384] <action:"set"><sender_id:"4000000001"><sender_phonenumber:"99990000001"><receiver_id:"null"><group_id:"null"><service_id:"10000001"><sender_sn:"1387555730954"><msg_id:"null"><msg_type:"0"><timestamp:"1387536910962"><size:"1">
	
	#2013-12-20 18:55:10 [pushagent] [PID=4533] [INFO ] [c_src/qlog_driver.cc:95] [info] [<0.68.0>:pushagent_server:321] <action:"set"><sender_id:"4000000001"><sender_phonenumber:"99990000001"><receiver_id:"null"><group_id:"null"><service_id:"10000001"><sender_sn:"1387555730954"><msg_id:"null"><msg_type:"0"><timestamp:"1387536910963"><size:"1">
        my $row = $_;
	chomp($row);
	$row =~ s/\s//g;
		
	my @r = split(/\|/, $row,-1);
	if(@r == 3)
        {
	    if($r[1] eq "peer" )
	    {
	        print("nr_peer". "\t". $r[2]."\t".$r[0]. "\n");
	    }
	    elsif($r[1] eq "group")
	    {
	       print("nr_group". "\t". $r[2]."\t".$r[0]. "\n");
	    }
        }
        else
        {
           my %inf;
           my @ra = split(/\].*?</, $row);
           my @rb = split(/\[|\]/,$row);
           my $i = $ra[2];
           my @l = split(/><?/, $i);
           my $set_distribute;
           my $fail_distribute;
           my $fail_pushagent;

           for(my $a = 0; $a < @l; $a ++)
           {
	     $l[$a] =~ s/\"//g;
	     my @kv = split(/:/, $l[$a]);
	     $inf{$kv[0]} = $kv[1];
           }
           if( $rb[1] eq "distribute" )
           {
               print($rb[1]."|".$inf{"action"}."\t".$inf{"size"}."\t".substr( &rstamp(int($inf{"timestamp"}/1000)),0,11)."0"."\n");  
           }
           elsif( $rb[1] eq "pushagent" )
           {
               print($rb[1]."|".$inf{"action"}."\t".$inf{"size"}."\t".substr( &rstamp(int($inf{"timestamp"}/1000)),0,11)."0"."\n");  
           }
       }
	
}
sub stamp()
{
    my $t = shift;
    return mktime(substr($t,12,2),substr($t,10,2),substr($t,8,2),substr($t,6,2),substr($t,4,2)-1,substr($t,0,4)-1900);
}

sub rstamp()
{
    my $t = shift;
    return strftime("%Y%m%d%H%M%S",localtime($t));
}

