#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;


my $date ="";
my $lastsetid_db="";
my $lastkey="";
my $vocsize = 0;
my $chatmember = 0;
my $totalmsg = 0;
my $active_chatroom = 0;
my $lastchatroomid = "";
my $total_chatroom_text = 0;
my $total_chatroom_voc = 0;
my $total_chatroom_img = 0;
my $total_chatroom_maps = 0;
my $total_chatroom_card = 0;
my $total_chatroom_location = 0;
my $total_chatroom_distext = 0;
my $total_chatroom_disimg = 0;

my %msgtype=("chatroom_text"=>0,"chatroom_voc"=>1,"chatroom_img"=>2);

sub stamp()
{
    my $t = shift;
    return mktime(substr($t,12,2),substr($t,10,2),substr($t,8,2),substr($t,6,2),substr($t,4,2)-1,substr($t,0,4)-1900);
}

sub rstamp()
{
    my $t = shift;
    return strftime("%Y%m%d%H%M%S",localtime($t));
}

while(<STDIN>)
{
    my $row = $_;
    chomp($row);
    my @r = split(/\t/, $row);
    my @l = split(/\|/, $r[0]);
    if($r[2] eq "chatdb")
    {    
        $date = substr(&rstamp(int($r[4]/1000)),0,8);
        if($r[0] eq "set")
        {
            if($msgtype{"chatroom_text"} == $r[1])
            {
                $total_chatroom_text += 1;
            }
            if($msgtype{"chatroom_voc"} == $r[1])
            {
                $total_chatroom_voc += 1;
                $vocsize += $r[4];
            }
            if($msgtype{"chatroom_img"} == $r[1])
            {
                $total_chatroom_img += 1;
            }
        }
		
		
    }
		
    if(($l[0] eq "set")&&($r[1] eq "chatdb"))
    {
        $date = substr(&rstamp(int($r[3]/1000)),0,8);
		
        if(($l[1] ne $lastsetid_db)&&($lastsetid_db ne ""))
        {
            $chatmember += 1;
        }
        #if(($r[2] ne $lastkey)&&($lastkey ne ""))
        #{
            $totalmsg += 1;
        #}
		
        $lastsetid_db = $l[1];
        $lastkey = $r[2];
    }
    if($l[0] eq "active_chatroom")
    {  
        $date = substr(&rstamp(int($r[1]/1000)),0,8);
        if(($r[0] ne $lastchatroomid) && ($lastchatroomid ne "") )
        {
            $active_chatroom += 1;
        }
		
        $lastchatroomid = $r[0];
		
    }
                
}
    if($lastsetid_db ne "")
    {
        $chatmember += 1;
    }
    if($lastchatroomid ne "")
    {
        $active_chatroom += 1;
    }
    #if($lastkey ne "")
    #{
        #$totalmsg += 1;
    #}
    print ("ChatroomInfo"."|".$total_chatroom_text."|".$total_chatroom_voc."|".$total_chatroom_img."|".$totalmsg."|".$chatmember."|".$active_chatroom."|".$vocsize."|".$date."\n");

# Last Version 2013-9-17 15:19 360_ContactVoice_GPvCfg.pl
