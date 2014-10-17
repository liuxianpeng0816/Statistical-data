#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $current_date = $ENV{'CURRENT_DATE'};
#my $current_date ='20140122';
my $register_num = 0;

while(<STDIN>)
{
    my $row = $_;
    chomp($row);
    $row =~ s/\s//g;
		
    my @r = split(/\|/, $row);
    $register_num += $r[1];
}

print("CHATROOM"."\t".$current_date."\t".$register_num."\n");


