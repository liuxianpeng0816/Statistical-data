#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my %savekey_hash;
my $current_date = $ENV{'CURRENT_DATE'};

unless (open (savekey, "./ShareMessageSaveKey.$current_date"))
{
    die ("cannot open input file file1/n");
}
else
{
    while(<savekey>)
    {
        my $row = $_;
        chomp($row);
        my @r = split(/\s+/, $row);

        $savekey_hash{$r[1]} = $r[0];
    }
}

my $sendmessage_count = 0; 
my $viewmessage_count = 0;

#viewkey  WT3urlMB
#viewkey  KZiyP4Nm
#sendmessage     3 

while(<STDIN>)
{
	my $row = $_;
	chomp($row);
	my @r;
	@r = split(/\s+/, $row);
	if($r[0] eq "sendmessage")
	{
		$sendmessage_count += $r[1];
	}
	elsif($r[0] eq "viewkey")
	{
		foreach my $key(keys %savekey_hash)
		{
			if($key eq $r[1])
			{
				$viewmessage_count += 1; 
			}
		}
	}

}
    
$current_date = $current_date."0000";

print("sendmessage"."\t".$sendmessage_count."\t".$current_date."\n");
print("viewmessage"."\t".$viewmessage_count."\t".$current_date."\n");
