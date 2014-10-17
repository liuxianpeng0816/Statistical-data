#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $current_date = $ENV{'CURRENT_DATE'};
#my $current_date = "20140717";
my %relation_count;

while(<STDIN>)
{
    my $row = $_;
    chomp($row);
		

    my @r = split(/\t/, $row);
    if($r[0] eq "PHONE_COUNT")
    {
	    $relation_count{"PHONE_COUNT"} += $r[1];
    }
    elsif($r[0] eq "REG_COUNT")
    {
	    $relation_count{"REG_COUNT"} += $r[1];
    }
    elsif($r[0] eq "AVEREG_COUNT")
    {
	    $relation_count{"AVEREG_COUNT"} += $r[1];
    }
    elsif($r[0] eq "REG_THREE")
    {
	    $relation_count{"REG_THREE"} += $r[1];
    }
    elsif($r[0] eq "SUBSCRIBE_ZERO")
    {
	    $relation_count{"SUBSCRIBE_ZERO"} += $r[1];
    }
    elsif($r[0] eq "REG_ZERO")
    {
	    $relation_count{"REG_ZERO"} += $r[1];
    }
    elsif($r[0] eq "SUBSCRIBE_COUNT")
    {
	    $relation_count{"SUBSCRIBE_COUNT"} += $r[1];
    }
    elsif($r[0] eq "AVE_SUBSCRIBE")
    {
	    $relation_count{"AVE_SUBSCRIBE"} += $r[1];
    }
    elsif($r[0] eq "SUBSCRIBE_MEMBER")
    {
	    $relation_count{"SUBSCRIBE_MEMBER"} += $r[1];
    }
}

foreach my $key(keys %relation_count)
{
	print($key."\t".$relation_count{$key}."\t".$current_date."\n");
}
