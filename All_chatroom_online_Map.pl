#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $online_time = 0;
my $online_jid ="";
#[warning] [<0.6791.0>:session_server:1196] [2014-08-12 04:15:55] stat info >> user: 6430805 3000 "pc" keep online time(s): 1695
while(<STDIN>)
{
    my $row = $_;
    chomp($row);

    my @r = split(/:/,$row);
    $online_time = $r[6];
    my @j = split(/\s/,$r[5]);
    $online_jid = $j[1];

    if($online_jid =~ /^\d+$/)	
    {
	print($online_jid."\t".$online_time."\n");
    }
}
