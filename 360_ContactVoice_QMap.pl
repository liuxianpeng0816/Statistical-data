#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $type;
my $now;

if((@ARGV == 2)&&(length($ARGV[0]) == 2)&&(length($ARGV[1]) == 10))
{
	$type = substr($ARGV[0],0,2);
	$now = substr($ARGV[1],0,10);
}
else
{
	print("Parameter Error! or Input Type (ss-set/set, sg-set/get, sn-set/notify) and yyyymmddhh ...\n");
	exit(0);
}

while(<STDIN>)
{
	my $row = $_;
	chomp($row);
	
	my @r = split(/\s/, $row);
	
	# $r[0]      $r[1]    $r[2]          $ra[1]
	# 2013-09-22 00:17:22 [msgrouter]...<action:"set"><sender_id:"1000511"><sender_phonenumber:"18665705636"><receiver_id:"1000150"><service_id:"10000000"><sender_sn:"18"><msg_id:"null"><msg_type:"0">
        # 2013-09-22 14:25:35 [msgrouter]...<action:"notify"><sender_id:"null"><sender_phonenumber:"13681161855"><receiver_id:"1000174"><service_id:"10000000"><sender_sn:"1379813278105"><msg_id:"null"><msg_type:"null">
        # 2013-09-22 10:50:00 [PEER_MSG]    <action:"set"><sender_id:"1000495"><sender_phonenumber:"null"><receiver_id:"1000165"><service_id:"10000000"><sender_sn:"92"><msg_id:"56"><msg_type:"0">
	# 2013-09-22 10:50:11 [PEER_MSG]    <action:"get"><sender_id:"1000010"><sender_phonenumber:"null"><receiver_id:"1000181"><service_id:"10000000"><sender_sn:"null"><msg_id:"79"><msg_type:"1000">
	
	$r[0] =~ s/-//g;
	$r[1] =~ s/://g;
	
	my $logt = $r[2];
	$logt =~ s/\[|\]//g;
	
	my @ra = split(/\].*?</, $row);	
	my %inf;
	
	my $i = "<".$ra[1];
	my @l = split(/>?<|><?/, $i);
	
	for(my $a = 1; $a < @l; $a ++)
	{
		$l[$a] =~ s/\"//g;
		my @kv = split(/:/, $l[$a]);
		$inf{$kv[0]} = $kv[1];
	}
	
	# 4000000003
	# 4000000099
	# 4005000000
	if((substr($inf{"sender_id"},0,3) eq "350")&&(length($inf{"sender_id"}) == 10)||($inf{"sender_id"} =~/[^0-9]+/))
	{
		next;
	}
	
	if((substr($inf{"receiver_id"},0,3) eq "350")&&(length($inf{"receiver_id"}) == 10)||($inf{"receiver_id"} =~/[^0-9]+/))
	{
		next;
	}
	if((substr($inf{"sender_id"},0,3) eq "400")&&(length($inf{"sender_id"}) == 10))
	{
		next;
	}
	
	if((substr($inf{"receiver_id"},0,3) eq "400")&&(length($inf{"receiver_id"}) == 10))
	{
		next;
	}
	
	if(($inf{"receiver_id"} eq "")||($inf{"receiver_id"} eq "null"))
	{
		next;
	}
	
	if(($inf{"service_id"} eq "")||($inf{"service_id"} eq "null"))
	{
		next;
	}
	
	if($inf{"service_id"} ne "10000000")
	{
		next;
	}
	
	if($inf{"msg_type"} eq "1000")
	{
		next;
	}
	
	if(($inf{"timestamp"} eq "")||($inf{"timestamp"} eq "null"))
	{
		$inf{"timestamp"} = &stamp($r[0].$r[1]) * 1000;
	}
	
	if(($inf{"size"} eq "")||($inf{"size"} eq "null"))
	{
		$inf{"size"} = 0;
	}
	
	my $key;
	
	if($type eq "sn")
	{		
		if(substr(&rstamp(int($inf{"timestamp"}/1000)),0,10) ne $now)
		{
			next;
		}
		
		if(($inf{"sender_phonenumber"} eq "")||($inf{"sender_phonenumber"} eq "null"))
		{
			next;
		}
		
		if(($inf{"sender_sn"} eq "")||($inf{"sender_sn"} eq "null"))
		{
			next;
		}
		
		if($logt ne "msgrouter")
		{
			next;
		}
		
		if(($inf{"action"} eq "get")||($inf{"action"} eq "prepare"))
		{
			next;
		}
		
		$key = $inf{"sender_phonenumber"}."_".$inf{"sender_sn"}."_".$inf{"service_id"}."_".$inf{"receiver_id"};
		
		print($key."\t".$inf{"timestamp"}."\t".$logt."\t".$inf{"action"}."\t".$inf{"msg_type"}."\t".$inf{"msg_id"}."\t".$inf{"size"}."\n");
	}
	
	if($type eq "ss")
	{
		if(substr(&rstamp(int($inf{"timestamp"}/1000)),0,10) ne $now)
		{
			next;
		}
		
		if(($inf{"sender_id"} eq "")||($inf{"sender_id"} eq "null"))
		{
			next;
		}
		
		if(($inf{"sender_sn"} eq "")||($inf{"sender_sn"} eq "null"))
		{
			next;
		}
		
		if(($logt ne "msgrouter")&&($logt ne "PEER_MSG"))
		{
			next;
		}
		
		if(($inf{"action"} eq "get")||($inf{"action"} eq "notify")||($inf{"action"} eq "prepare"))
		{
			next;
		}
		
		$key = $inf{"sender_id"}."_".$inf{"sender_sn"}."_".$inf{"service_id"}."_".$inf{"receiver_id"};
		
		print($key."\t".$inf{"timestamp"}."\t".$logt."\t".$inf{"action"}."\t".$inf{"msg_type"}."\t".$inf{"msg_id"}."\t".$inf{"size"}."\n");
	}
	
	if($type eq "sg")
	{
		if((substr(&rstamp(int($inf{"timestamp"}/1000)),0,10) ne $now)&&($inf{"action"} eq "get"))
		{
			next;
		}
		
		if(($inf{"sender_id"} eq "")||($inf{"sender_id"} eq "null"))
		{
			next;
		}
		
		if(($inf{"sender_sn"} eq "")||($inf{"sender_sn"} eq "null"))
		{
			next;
		}
		
		if(($inf{"msg_id"} eq "")||($inf{"msg_id"} eq "null"))
		{
			next;
		}
		
		if($logt ne "PEER_MSG")
		{
			next;
		}
		
		$key = $inf{"sender_id"}."_".$inf{"msg_id"}."_".$inf{"service_id"}."_".$inf{"receiver_id"};
		
		print($key."\t".$inf{"timestamp"}."\t".$logt."\t".$inf{"action"}."\t".$inf{"msg_type"}."\t".$inf{"sender_sn"}."\t".$inf{"size"}."\n");
	}
	
	if($type eq "pg")
	{
		if(substr(&rstamp(int($inf{"timestamp"}/1000)),0,10) ne $now)
		{
			next;
		}
		
		if(($inf{"sender_sn"} eq "")||($inf{"sender_sn"} eq "null"))
		{
			next;
		}
		
		if(($logt ne "msgrouter")&&($logt ne "PEER_MSG"))
		{
			next;
		}
		
		if(($inf{"action"} ne "get")&&($inf{"action"} ne "prepare"))
		{
			next;
		}
		
		$key = $inf{"sender_sn"}."_".$inf{"service_id"}."_".$inf{"receiver_id"};
		
		print($key."\t".$inf{"timestamp"}."\t".$logt."\t".$inf{"action"}."\t".$inf{"msg_type"}."\t".$inf{"msg_id"}."\t".$inf{"size"}."\n");
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

# Last Version 2013-9-26 18:52
