#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

while(<STDIN>)
{
    my $row = $_;
    chomp($row);
		
    print($row."\n");
}
