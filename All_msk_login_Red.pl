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
my $login_member = 0;
my $last_login = "";
my $last_login_version = "";

#mmlogin|13281621705
while(<STDIN>)
{
	my $row = $_;
	chomp($row);
	my @r = split(/\s+/, $row);
	my @flag = split(/\|/, $r[0]);

	if($flag[0] eq "mmlogin")
	{
		if($flag[1] ne $last_login && $last_login ne "")
		{
			$login_member += 1;	
		}
		$last_login = $flag[1];		
	}
	elsif($flag[0] eq "mmversion")
	{
		if($flag[2] ne $last_login && $last_login ne "")
		{
			$client_version_member{$last_login_version} += 1;	
		}
		$last_login_version = $flag[1];		
		$last_login = $flag[2];		
	}
	elsif($flag[0] eq "loginumber")
        {
		$login_number += $flag[1];
	}
        else 
	{
		$client_version{$r[0]} += $r[1];
	}

}
if($last_login ne "")
{
	$login_member += 1;	
	$client_version_member{$last_login_version} += 1;	
}

print("LOGIN"."\t".$login_member."\t".$login_number."\t".$current_date."\n");

foreach my $key(keys %client_version)
{
	print("CLIENT_VERSION"."\t".$key."\t".$client_version{$key}."\t".$current_date."\n");
}
foreach my $key(keys %client_version_member)
{
	print("VERSION_MEMBER"."\t".$key."\t".$client_version_member{$key}."\t".$current_date."\n");
}
