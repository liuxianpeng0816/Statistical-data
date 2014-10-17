#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;


while(<STDIN>)
{
        my $row = $_;
        chomp($row);

        my @r = split(/\].*?</, $row);	
        my %inf;
        my $i = $r[2];
        my @l = split(/> ?<?/, $i);
	
	      
        for(my $a = 0; $a < @l; $a ++)
        {
	    $l[$a] =~ s/\"//g;
	    my @kv = split(/:/, $l[$a]);
	    $inf{$kv[0]} = $kv[1];
        }
	
            if($inf{"action"} eq "set") 
            {
                if(($inf{"sender_id"} eq "")||((substr($inf{"sender_id"},0,3) eq "350")&&(length($inf{"sender_id"}) == 10))||($inf{"sender_id"} =~/[^0-9]+/))
                {
                    next;
                }
                if(($inf{"sender_id"} eq "")||((substr($inf{"sender_id"},0,3) eq "400")&&(length($inf{"sender_id"}) == 10)))
                {
                    next;
                }
                if($inf{"sender_phonenumber"} eq "")
                {
                    next;
                }
                if($inf{"receiver_id"} eq "")
                {
                    next;
                }
                if($inf{"room_id"} eq "")
                {
                    next;
                }
                if($inf{"service_id"} eq "")
                {
                    next;
                }
                if($inf{"sender_sn"} eq "")
                {
                    next;
	        }
                if($inf{"msg_type"} eq "")
                {
                    next;
                }
                if($inf{"msg_type"} == 102)
                {
                    print($inf{"room_id"}."|".$inf{"sender_id"}."\n");
                }

        }
}

