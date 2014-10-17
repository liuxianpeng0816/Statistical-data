#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;


my $current_date = $ENV{'CURRENT_DATE'};
#my $current_date = "20140824";

my $single_send_number = 0;
my $single_success_number = 0;
my $single_count_number = 0;
my $single_count_time = 0;

my $group_send_number = 0;
my $group_success_number = 0;
my $group_count_number = 0;
my $group_count_time = 0;

my $single_firstlog_source = "";
my $single_first_sn = "";
my $single_first_time = "";
my $single_last_time = "";

my $group_firstlog_source = "";
my $group_first_sn = "";
my $group_first_time = "";
my $group_last_time = "";

my $login_number = 0;
my $last_login = "";

#single|68670944        distribute      1407985198
#single|68670944        msg     	1407985198

#group|68670944         distribute      1407985198
#group|68670944         msg     	1407985198

while(<STDIN>)
{
	my $row = $_;
	chomp($row);
	my @r = split(/\s+/, $row);
	my @flag = split(/\|/, $r[0]);

	if($flag[0] eq "single")
	{
		if($r[0] eq $single_first_sn && $r[1] ne $single_firstlog_source )
		{
			$single_count_number += 1;
			$single_last_time = $r[2];
			$single_count_time += abs($single_last_time - $single_first_time);
		}

		if($flag[1] ne "")
		{
			$single_first_sn = $r[0];    
			$single_firstlog_source = $r[1];
			$single_first_time = $r[2];
		}
	}
	elsif($flag[0] eq "group")
	{
		
		if($r[0] eq $group_first_sn && $r[1] ne $group_firstlog_source )
		{
			$group_count_number += 1;
			$group_last_time = $r[2];
			$group_count_time += abs($group_last_time - $group_first_time);

		}

		if($flag[1] ne "")
		{
			$group_first_sn = $r[0];    
			$group_firstlog_source = $r[1];
			$group_first_time = $r[2];
		}
	}
	elsif($flag[0] eq "weimilogin")
	{
		if($flag[1] ne $last_login)
		{
			$login_number += 1;	
		}
		$last_login = $flag[1];		
	}
	elsif($r[0] eq "MSGSINGLE")
	{
		$single_send_number += $r[1];
		$single_success_number += $r[2];
	}
	elsif($r[0] eq "MSGROUP")
	{
		$group_send_number += $r[1];
		$group_success_number += $r[2];
	} 

}

print("SINGLE"."\t".$single_send_number."\t".$single_success_number."\t".$single_count_time."\t".$current_date."\n");
print("GROUP"."\t".$group_send_number."\t".$group_success_number."\t".$group_count_time."\t".$current_date."\n");
print("LOGIN"."\t".$login_number."\t"."NULL"."\t"."NULL"."\t".$current_date."\n");
