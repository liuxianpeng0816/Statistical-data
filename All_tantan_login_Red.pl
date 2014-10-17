#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;


my $current_date = $ENV{'CURRENT_DATE'};
#my $current_date = "20140913";

my %client_version;
my %client_version_member;
my $login_number = 0;
my $push_number = 0;
my $login_member = 0;
my $last_login = "";
my $last_login_version = "";

#mmlogin|13281621705
while(<STDIN>)
{
	my $row = $_;
	chomp($row);
	my @flag = split(/\t/, $row);

	if($flag[0] eq "PUSH")
	{
		#if($flag[1] ne $last_login && $last_login ne "")
		#{
		#	$login_member += 1;	
		#}
		#$last_login = $flag[1];		
		$push_number += $flag[1];	
	}

}
#if($last_login ne "")
#{
#	$login_member += 1;	
#}

print("PUSH"."\t".$push_number."\t".$current_date."\n");

