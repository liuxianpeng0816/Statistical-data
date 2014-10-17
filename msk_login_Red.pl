#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;


my $current_date = $ENV{'CURRENT_DATE'};
#my $current_date = "20140824";


my $login_number = 0;
my $login_member = 0;
my $last_login = "";

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
	elsif($flag[0] eq "loginumber")
        {
		$login_number += $flag[1];
	}


}
if($last_login ne "")
{
	$login_member += 1;	
}

print("LOGIN"."\t".$login_member."\t".$login_number."\t".$current_date."\n");
