#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $current_date = $ENV{'CURRENT_DATE'};
#my $current_date ='2014051215';
my $create_num = 0;
my $heart_num = 0;
my $comment_num = 0;
my $create_mem = 0;
my $heart_mem = 0;
my $comment_mem = 0;
my $last_create = "";
my $last_heart = "";
my $last_comment = "";

#Count|1|2|1
#create|500000008
#comment|500000013
#heart|500000008

while(<STDIN>)
{
    my $row = $_;
    chomp($row);
		
    my @r = split(/\|/, $row);
    if($r[0] eq "Count")
    {
	$create_num += $r[1];
	$heart_num += $r[2];
	$comment_num += $r[3];
    }
    elsif($r[0] eq "create")
    {
	if($r[1] ne $last_create && $last_create ne "")
	{
		$create_mem += 1;
	}

	$last_create = $r[1];	
    }
    elsif($r[0] eq "heart")
    {
	if($r[1] ne $last_heart && $last_heart ne "")
	{
		$heart_mem += 1;
	}
	
	$last_heart = $r[1];	
    }
    elsif($r[0] eq "comment")
    {
	if($r[1] ne $last_comment && $last_create ne "")
	{
		$comment_mem += 1;
	}

	$last_comment = $r[1];	
    }
}

if($last_create ne "")
{
	$create_mem += 1;
}
if($last_heart ne "")
{
	$heart_mem += 1;
}
if($last_comment ne "")
{
	$comment_mem += 1;
}

print("MEMBER"."\t".$current_date."\t".$create_mem."\t".$heart_mem."\t".$comment_mem."\n");
print("NUMBER"."\t".$current_date."\t".$create_num."\t".$heart_num."\t".$comment_num."\n");

