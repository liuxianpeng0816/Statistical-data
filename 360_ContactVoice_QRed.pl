#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $t;

if((@ARGV == 1)&&(length($ARGV[0]) == 2))
{
	$t = substr($ARGV[0],0);
}
else
{
	print("Parameter Error! or Input Type! (ss-set/set, sg-set/get, sn-set/notify) ...\n");
	exit(0);
}

my $lastr = "";

my $t1 = "NULL";
my $t2 = "NULL";

my $mt = "NULL";
my $mid = "NULL";
my $des = 0;

my $in = 0;
my $out = 0;

while(<STDIN>)
{
	my $row = $_;
	chomp($row);
	
	my @r = split(/\s/, $row);
	
	# 0                                                                     1     2              3          4           5                   6
	# (senderid/senderphonenumber_)sendersn/msgid_serviceid_receiverid      date  log_type       action     msg_type    msg_id/sender_sn    size
	
	if(($r[0] ne $lastr)&&($lastr ne ""))
	{
		if(($in <= 1)&&($out <= 1))
		{
			if(($t1 ne "NULL")&&($t2 ne "NULL"))
			{
				my $err = "";
				if(($t2 - $t1) < -20)
				{
					$err = "\tER1";
				}
				
				print("FI\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$mid."\t".$des.$err."\n");
			}
			else
			{
				print("UN\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$mid."\t".$des."\n");
			}
		}
		else
		{
			print("ER\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$mid."\t".$des."\tER2\n");
		}
		
		$t1 = "NULL";
		$t2 = "NULL";
		$mt = "NULL";
		$mid = "NULL";
		$des = 0;
		
		$in = 0;
		$out = 0;
	}
	
	if($t eq "ss")
	{
		if(($r[2] eq "msgrouter")&&($r[3] eq "set"))
		{
			$t1 = $r[1];
			$in += 1;
		}
		
		if(($r[2] eq "PEER_MSG")&&($r[3] eq "set"))
		{
			$t2 = $r[1];
			$out += 1;
		}
	
		if(($r[6] ne "null")&&($r[6] ne "")&&($r[6] > 0))
		{
			$des = $r[6];
		}
	}
	
	if($t eq "sg")
	{
		if(($r[2] eq "PEER_MSG")&&($r[3] eq "set"))
		{
			$t1 = $r[1];
			$in += 1;
		}
		
		if(($r[2] eq "PEER_MSG")&&($r[3] eq "get"))
		{
			$t2 = $r[1];
			$out += 1;
		}
	
		if(($r[6] ne "null")&&($r[6] ne "")&&($r[6] > 0))
		{
			$des = $r[6];
		}
	}
	
	if($t eq "sn")
	{
		if(($r[2] eq "msgrouter")&&($r[3] eq "set"))
		{
			$t1 = $r[1];
			$in += 1;
		}
		
		if(($r[2] eq "msgrouter")&&($r[3] eq "notify"))
		{
			$t2 = $r[1];
			$out += 1;
		}
	
		#if(($r[6] ne "null")&&($r[6] ne "")&&($r[6] > 0))
		#{
		#	$des = $r[6];
		#}
		
		$des = 0;
	}
	
	if($t eq "pg")
	{
		if(($r[2] eq "msgrouter")&&($r[3] eq "prepare"))
		{
			$t1 = $r[1];
			$in += 1;
		}
		
		if(($r[2] eq "msgrouter")&&($r[3] eq "get"))
		{
			$t2 = $r[1];
			$out += 1;
		}
		
		if(($r[2] eq "PEER_MSG")&&($r[3] eq "get"))
		{
			$des += 1;
		}
	}
	
	if(($r[4] ne "null")&&($r[4] ne ""))
	{
		$mt = $r[4];
	}
	
	if(($r[5] ne "null")&&($r[5] ne ""))
	{
		$mid = $r[5];
	}
	
	$lastr = $r[0];
}

if(($in <= 1)&&($out <= 1))
{
	if(($t1 ne "NULL")&&($t2 ne "NULL"))
	{
		my $err = "";
		if(($t2 - $t1) < -20)
		{
			$err = "\tER1";
		}
		
		print("FI\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$mid."\t".$des.$err."\n");
	}
	else
	{
		print("UN\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$mid."\t".$des."\n");
	}
}
else
{
	print("ER\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$mid."\t".$des."\tER2\n");
}

sub rstamp()
{
	my $t = shift;
	
	if($t =~ /^\d+$/)
	{
		return strftime("%Y%m%d%H%M%S",localtime($t));
	}
	else
	{
		return $t;
	}
}

# Last Version 2013-9-27 16:25
