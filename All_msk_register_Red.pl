#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $current_date = $ENV{'CURRENT_DATE'};
my $user_register_number = 0;
my $server_register_number = 0;
# user_reg	1
# server_reg	0
while(<STDIN>)
{
    my $row = $_;
    chomp($row);
		

    my @r = split(/\t/, $row);
    if($r[0] eq "USER_REG")
    {
    	$user_register_number += $r[1];
    }
    elsif($r[0] eq "SERVER_REG")
    {
    	$server_register_number += $r[1];
    }
    
}

print("USER_REG"."\t".$user_register_number."\t".$current_date."\n");
print("SERVER_REG"."\t".$server_register_number."\t".$current_date."\n");
