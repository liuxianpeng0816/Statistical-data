#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $register_num = 0;
my $register_flag = "rAction success";

while(<STDIN>)
{
    my $row = $_;
    chomp($row);
    if($row =~ m/$register_flag/)
    {
        $register_num++;
    }
	
}

print("CHATROOM"."|".$register_num."\n");
