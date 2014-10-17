#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

while(<STDIN>)
{
        my $row = $_;
        chomp($row);
        my @ra;
        my @l;
	      
        @ra = split(/<action:|<act:/, $row);
        @l = split(/>?<|><?/, "action:".$ra[1]);
        my %inf;
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
                print($row."\n");
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
            if($inf{"msg_id"} eq "")
            {
                next;
            }
            if($inf{"msg_type"} eq "")
            {
                next;
            }
            if($inf{"timestamp"} eq "")
            {
                next;
            }
            if($inf{"size"} eq "")
            {
                next;
            }
        }
                

        if(($inf{"sender_id"} ne "")&&($inf{"action"} eq "set")&&($inf{"msg_type"} == 0||$inf{"msg_type"} == 1||$inf{"msg_type"} == 2))	
        {
            print($inf{"action"}."|".$inf{"sender_id"}."\t"."chatdb"."\t".$inf{"sender_id"}."|".$inf{"sender_sn"}."\t".$inf{"timestamp"}."\n");
            print($inf{"action"}."\t".$inf{"msg_type"}."\t"."chatdb"."\t".$inf{"sender_id"}."|".$inf{"sender_sn"}."\t".$inf{"size"}."\t".$inf{"timestamp"}."\n");
            print("active_chatroom"."|".$inf{"room_id"}."\t".$inf{"timestamp"}."\n");
        }

}
# Last Version  2013-9-17 17:44
