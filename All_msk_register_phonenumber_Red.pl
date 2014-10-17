#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my %savereply_number;
my $current_date = $ENV{'CURRENT_DATE'};
#my $current_date = "20140808";
my $reply_register_number = 0;

unless (open (savereply_number, "./ReplyPhoneNumber"))
{
    die ("cannot open input file file1/n");
}
else
{
    while(<savereply_number>)
    {
        my $row = $_;
        chomp($row);
        my @r = split(/\s+/, $row);

        $savereply_number{$r[0]} = $r[0];
    }
}
while(<STDIN>)
{
	my $row = $_;
	chomp($row);

	my @r = split(/\t/, $row);

	foreach my $key(keys %savereply_number)
	{
		if($r[0] eq $key)
		{
			$reply_register_number += 1;
		}
	}
}

print("ReplyRegister"."\t".$reply_register_number."\t".$current_date."\n");
