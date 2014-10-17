#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $date = $ENV{'CURRENT_DATE'};
my $last_vocid = "";
my $last_sendid = "";
my $voc_usr_num = 0;
my $peer_usr_num = 0;

while(<STDIN>)
{
    my $row = $_;
    chomp($row);
    my @jid = split(/\|/,$row);
	
    if(($last_vocid ne $jid[1])&&($last_vocid ne ""))
    {
        $voc_usr_num++;
    }
    if($jid[0] eq "VOC")	
    {
        $last_vocid = $jid[1];
    }
    elsif($jid[0] eq "PEER")
    {
        if(($last_sendid ne $jid[1])&&($last_sendid ne ""))
        {
            $peer_usr_num++;
        }

        $last_sendid = $jid[1];    
    }
	
}
if($last_vocid ne "")
{
    $voc_usr_num++;
}
if($last_sendid ne "")
{
    $peer_usr_num++;
}

print($date."\t"."VOCNUM\t" . $voc_usr_num . "\n");
print($date."\t"."CHATMEM\t" . $peer_usr_num . "\n");
