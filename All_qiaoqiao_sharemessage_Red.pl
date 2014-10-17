#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $current_date = $ENV{'CURRENT_DATE'};

my $sendmessage_count = 0; 
my $receiver_num = 0; 
my $lastkey = "";
while(<STDIN>)
{
    my $row = $_;
    chomp($row);
    my @r;
    @r = split(/\t/, $row);
    my $array_len = @r;
    if($array_len eq 2 )
    {
	    if( $r[0] ne $lastkey )
	    {
		    $sendmessage_count += 1;
	    }
	    $lastkey = $r[0];
    }
    elsif($array_len eq 3 )
    {
	    if( $r[0] eq "receiver_num" )
	    {
		    $receiver_num += $r[1];
	    }
    }	

}
    
print("sendmessage"."\t".$sendmessage_count."\t".$current_date."\n");
print("receiver"."\t".$receiver_num."\t".$current_date."\n");
