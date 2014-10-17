#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $current_date = $ENV{'CURRENT_DATE'};
#my $current_date ='20131103';
my $new_user_num = 0;

while(<STDIN>)
{
    my $row = $_;
    chomp($row);
    #已经在预处理过程中去重 这里不用考虑去重
    $new_user_num += 1;
}

print("NEW_USER\t".$current_date."\t".$new_user_num."\n");


