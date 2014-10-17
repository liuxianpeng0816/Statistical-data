#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

while(<STDIN>)
{
    my $row = $_;
    chomp($row);
    my @r = split(/\s/, $row);
    my @ra;
    my @l;
    my $size = 0;
    my $date;
	
	
    $r[0] =~ s/-//g;
    $r[1] =~ s/://g;
    $r[2] =~ s/\[|\]//g;
    $date = $r[0];
	
	
    if($r[2] eq "groupdb_msg_log")
    {
	      
        @ra = split(/<action:|<act:/, $row);
        @l = split(/>?<|><?/, "action:".$ra[1]);
        my %inf;
        for(my $a = 0; $a < @l; $a ++)
        {
            $l[$a] =~ s/\"//g;
            my @kv = split(/:/, $l[$a]);
            $inf{$kv[0]} = $kv[1];
        }
	
        if($inf{"action"} eq "create_group") 
        {
            if(($inf{"creater_jid"} eq "")||((substr($inf{"creater_jid"},0,3) eq "350")&&(length($inf{"creater_jid"}) == 10))||($inf{"creater_jid"} =~/[^0-9]+/))
            {
                next;
            }
            if(($inf{"creater_jid"} eq "")||((substr($inf{"creater_jid"},0,3) eq "400")&&(length($inf{"creater_jid"}) == 10)))
            {
                next;
            }
        }
        if($inf{"action"} eq "destroy_group") 
        {
            if(($inf{"creater_jid"} eq "")||((substr($inf{"creater_jid"},0,3) eq "350")&&(length($inf{"creater_jid"}) == 10))||($inf{"creater_jid"} =~/[^0-9]+/))
            {
                next;
            }
            if(($inf{"creater_jid"} eq "")||((substr($inf{"creater_jid"},0,3) eq "400")&&(length($inf{"creater_jid"}) == 10)))
            {
                next;
            }

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

            if($inf{"group_id"} eq "")
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
        }
                

        #if(($inf{"sender_id"} ne "")&&($inf{"action"} eq "set"))	
        if(($inf{"sender_id"} ne "")&&($inf{"action"} eq "set")&&($inf{"msg_type"} == 300||$inf{"msg_type"} == 301||$inf{"msg_type"} == 302||$inf{"msg_type"} == 303||$inf{"msg_type"} == 306||$inf{"msg_type"} == 307||$inf{"msg_type"} ==888100 ||$inf{"msg_type"} == 888101))	
        {
            print($inf{"action"}."|".$inf{"sender_id"}."\t"."groupdb"."\t".$inf{"sender_id"}."|".$inf{"sender_sn"}."\t".$date."\n");
            print($inf{"action"}."\t".$inf{"msg_type"}."\t"."groupdb"."\t".$inf{"sender_id"}."|".$inf{"sender_sn"}."\t".$date."\n");
        }
        if(($inf{"creater_jid"} ne "")&&($inf{"action"} eq "create_group"))
        {
            print($inf{"action"}."\t".$inf{"creater_jid"}."|".$inf{"group_id"}."\t"."groupdb"."\t".$date."\n");
        }
        if(($inf{"creater_jid"} ne "")&&($inf{"action"} eq "destroy_group"))
        {
            print($inf{"action"}."\t".$inf{"creater_jid"}."|".$inf{"group_id"}."\t"."groupdb"."\t".$date."\n");
        }
    }
    if($r[2] eq "group_server")
    {
        @ra = split(/<action:/, $row);
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
            if(( $inf{"group_id"} eq "" ) || ( $inf{"group_id"} eq "[]" ))
            {
                next;
            }
            if($inf{"service_id"} eq "")
            {
                next;
            }
            if(($inf{"sender_sn"} eq "") || (substr($inf{"sender_sn"},0,4) eq "#Ref"))
            {
                next;
			}
            if($inf{"msg_type"} eq "")
            {
                next;
            }
            if(($inf{"msg_type"} == 301) && ($inf{"size"} ne "") && ($inf{"size"} ne "null") && ($inf{"size"} != 0 ))
            {
                $size = $inf{"size"};
                print("group"."|".$inf{"sender_id"}."|".$inf{"sender_sn"}."\t".$size."\t".$date."\n");
            }
            #inorder to cut msg_type = 100 102 111 
 
            if($inf{"msg_type"} eq "100" || $inf{"msg_type"} eq "102" || $inf{"msg_type"} eq "111")
            {
                next;
            }
            print("active_group"."|".$inf{"group_id"}."\t".$date."\n");

        }
                
		
    }
}

# Last Version  2013-9-17 17:44
