#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my %savesn_hash;
my $count_number = 0;
my $count_time = 0;
#my $current_date = $ENV{'CURRENT_DATE'};
my $current_date = "20140814";

unless (open (savekey, "./WeiMiSn.20140814"))
{
    die ("cannot open input file file1/n");
}
else
{
    while(<savekey>)
    {
        my $row = $_;
        chomp($row);
        my @r = split(/\t/, $row);

        $savesn_hash{$r[0]} = $r[2];
    }
}

while(<STDIN>)
{
	my $row = $_;
	chomp($row);

	my @r = split(/\t/, $row);

	foreach my $key(keys %savesn_hash)
	{
		if($key eq $r[0])
		{
			$count_number += 1; 
			$count_time += $savesn_hash{$key} -$r[2]; 
		}
	}
}
print("WEIMI"."\t".$count_number."\t".$count_time."\n");
