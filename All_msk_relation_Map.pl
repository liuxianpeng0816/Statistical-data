#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

$| = 1;


my $total_phone_count_flag = "totalphonecount";
my $total_regfriends_count_flag = "totalregfriendscount";
my $average_regfriends_count_flag = "averageregfriendscount";
my $reg_friends_count_standard1_flag = "regfriend>3count";
my $subscribe_count_standard_flag = "subscribe>0count";
my $reg_friends_count_standard2_flag = "regfriend>0count";
my $total_second_subscribe_count_flag = "totalsecondsubscribecount";
my $average_second_subscribe_count_flag = "averagesecondsubscribe";
my $second_subscribe_member_flag = "secondsubscribe>0count";
my %relation_count;
while(<STDIN>)
{
        my $row = $_;
	chomp($row);
    	$row =~ s/\s//g;
        my @r = split(/:/, $row);
	if($row =~ m/$total_phone_count_flag/)
	{
		$relation_count{"PHONE_COUNT"} += $r[1];
	}
	elsif($row =~ m/$total_regfriends_count_flag/)
	{
		$relation_count{"REG_COUNT"} += $r[1];
	}
	elsif($row =~ m/$average_regfriends_count_flag/)
	{
		$relation_count{"AVEREG_COUNT"} += $r[1];
	}
	elsif($row =~ m/$reg_friends_count_standard1_flag/)
	{
		$relation_count{"REG_THREE"} += $r[1];
	}
	elsif($row =~ m/$subscribe_count_standard_flag/ && $row=~ m/$second_subscribe_member_flag/)
	{
		$relation_count{"SUBSCRIBE_MEMBER"} += $r[1];
	}
	elsif($row =~ m/$reg_friends_count_standard2_flag/)
	{
		$relation_count{"REG_ZERO"} += $r[1];
	}
	elsif($row =~ m/$total_second_subscribe_count_flag/)
	{
		$relation_count{"SUBSCRIBE_COUNT"} += $r[1];
	}
	elsif($row =~ m/$average_second_subscribe_count_flag/)
	{
		$relation_count{"AVE_SUBSCRIBE"} += $r[1];
	}
	elsif($row =~ m/$subscribe_count_standard_flag/)
	{
		$relation_count{"SUBSCRIBE_ZERO"} += $r[1];
	}
}

foreach my $key(keys %relation_count)
{
	print($key."\t".$relation_count{$key}."\n");
}

