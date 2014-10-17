#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $current_date = $ENV{'CURRENT_DATE'};
$current_date = $current_date."0000";
#my $current_date = "201406040000";

my %message_member;
my $count_sendmsg = 0;
my $count_recvmsg = 0;
my @r;
my $lastkey = "";
while(<STDIN>)
{
    my $row = $_;
    chomp($row);
    @r = split(/\t/, $row);
    my $array_len = @r;
    my @devide = split(/\|/,$r[0]); 
    if($array_len eq 1 )
    {
	    if( $devide[0] eq "send")
	    {
		    if( $devide[1] ne $lastkey )
		    {
			    $message_member{"sendmessage_member"} += 1;
		    }
		    $lastkey = $devide[1];
	    }
	    elsif( $devide[0] eq "get")
	    {
		    if( $devide[1] ne $lastkey )
		    {
			    $message_member{"getmessage_member"} += 1;
		    }
		    $lastkey = $devide[1];

	    }
    }
    elsif($array_len eq 3 )
    {
    	$count_sendmsg += $r[1];
    	$count_recvmsg += $r[2];
    }	

}
my $key; 
foreach $key(keys %message_member)
{
    print($key."\t".$message_member{$key}."\t".$current_date."\n");
}

print("MessageCount"."\t".$count_sendmsg."\t".$count_recvmsg."\t".$current_date."\n");
