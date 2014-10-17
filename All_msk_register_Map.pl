#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

$| = 1;


my $user_register_flag = "user down register successful";
my $server_register_flag = "backyard register successful";	
my $user_register_number = 0;
my $server_register_number = 0;
#[2014-06-24 09:33:10.409][INFO][DownregController.php:389][DownregController::getAction] user down register successful:Array

while(<STDIN>)
{
        my $row = $_;
	chomp($row);
	if($row =~ m/$user_register_flag/)
	{
		$user_register_number += 1;
	}
	elsif($row =~ m/$server_register_flag/)
	{
		$server_register_number += 1;
	}

}
print("USER_REG"."\t".$user_register_number."\n");
print("SERVER_REG"."\t".$server_register_number."\n");
