#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $current_date = $ENV{'CURRENT_DATE'};
#my $current_date ='2014062409';
my $mskreply_number = 0;
my $mskreply_member = 0;
my $last_senderid ="";
while(<STDIN>)
{
    my $row = $_;
    chomp($row);
		

    my @r = split(/\t/, $row);
    if($r[0] eq "MSKREPLY")
    {
    	$mskreply_number += $r[1];
    }
    else
    {
    	if(($last_senderid ne "")&&($r[0] ne $last_senderid))	
	{
		$mskreply_member += 1;
	}

	$last_senderid = $r[0];
	
    }
    
}
if($last_senderid ne "")
{
	$mskreply_member += 1;
}
print("MSKREPLY_MEMBER"."\t".$mskreply_member."\t".$current_date."\n");
print("MSKREPLY_NUMBER"."\t".$mskreply_number."\t".$current_date."\n");
