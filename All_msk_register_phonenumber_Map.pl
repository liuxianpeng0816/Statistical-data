#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

$| = 1;


my $user_register_flag = "mb";

while(<STDIN>)
{
        my $row = $_;
	chomp($row);
	if(($row =~ m/$user_register_flag/))
	{
		my @devide = split(/\s+/, $row);
		print($devide[3]."\n");	
	}

}
