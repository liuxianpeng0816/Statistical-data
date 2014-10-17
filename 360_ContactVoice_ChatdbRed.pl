#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $date = $ENV{'CURRENT_DATE'};
#my $date = "20140310";
my %gamename_hash;
unless (open (Roomid_Game, "./Roomid-Game1"))
{
    die ("cannot open input file file1/n");
}
else
{
    while(<Roomid_Game>)
    {
        my $row = $_;
        chomp($row);
        my @r = split(/\s+/, $row);

        $gamename_hash{$r[0]} = $r[1];
    }
}

my $lastsetid_db ="";
my $each_lastsetid_db ="";
my $last_roomid ="";
my $play_number = 0;
my $last_usrid = "";     
my $lastroomkey ="";
my $vocsize = 0;
my $chatmember = 0;
my $eachchatmember = 0;
my $totalmsg = 0;
my %each_chatmember ;
my %each_playmember ;
my %each_totalmsg ;
my $eachmsgcount = 0;   
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
    my $array_len = @r;
    if($array_len > 2)
    {
	    my @l = split(/\|/, $r[0]);
	    if($r[2] eq "chatdb")
	    {    
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

	    if($r[3] eq "chatdb")
	    {

		    if($r[0] ne $lastsetid_db && $lastsetid_db ne "" )
		    {
			    $chatmember ++;
		    }

		    $lastsetid_db = $r[0];

             
	    }
	    if($r[1] eq "chatdb")
	    {

		    if($l[0] ne $lastroomkey && $lastroomkey ne "")
		    {
			    $each_chatmember{$lastroomkey} += $eachchatmember;
			    $each_totalmsg{$lastroomkey} += $eachmsgcount;

			    $eachmsgcount = 0;
			    $eachchatmember = 0;
			    $lastroomkey = "";
			    $each_lastsetid_db = "";     
		    }

		    if($l[1] ne $each_lastsetid_db )
		    {
			    #$chatmember ++; 
			    $eachchatmember ++; 
		    }

		    $eachmsgcount ++;   
		    $totalmsg += 1; 
		    $lastroomkey = $l[0];
		    $each_lastsetid_db = $l[1];

             
	    }
	    if($r[1] eq "active_chatroom")
	    {  
		    if(($r[0] ne $lastchatroomid) && ($lastchatroomid ne "") )
		    {
			    $active_chatroom += 1;
		    }

		    $lastchatroomid = $r[0];

	    }
    }
    elsif($array_len == 2)
    {

	    my @l = split(/\|/, $r[0]);
	    if($l[0] ne $last_roomid && $last_roomid ne "")
	    {
		    $each_playmember{$last_roomid} += $play_number;
		    $play_number = 0;
		    $last_roomid = "";
		    $last_usrid = "";     
	    }

	    if($l[1] ne $last_usrid )
	    {
		    $play_number ++; 
	    }    
	    $last_roomid = $l[0];
	    $last_usrid = $l[1];
    }
}



if($last_roomid ne "")
{

        #every chatroom's chat member
	$each_playmember{$last_roomid} += $play_number;
}

if($each_lastsetid_db ne "")
{
        $each_chatmember{$lastroomkey} += $eachchatmember;
        $each_totalmsg{$lastroomkey} += $eachmsgcount;
}
if($lastsetid_db ne "")
{
	$chatmember += 1;
}
if($lastchatroomid ne "")
{
	$active_chatroom += 1;
}

my $chatroomid;
foreach $chatroomid (keys %each_playmember)
{
        if ($each_playmember{$chatroomid} eq "")
        {
            $each_playmember{$chatroomid} = 0;
        }
        if ($each_chatmember{$chatroomid} eq "")
        {
            $each_chatmember{$chatroomid} = 0;
        }
        if ($each_totalmsg{$chatroomid} eq "")
        {
            $each_totalmsg{$chatroomid} = 0;
        }
	print ("EachChatroomInfo"."|".$gamename_hash{$chatroomid}."|".$each_playmember{$chatroomid}."|".$each_chatmember{$chatroomid}."|".$each_totalmsg{$chatroomid}."|".$date."\n");
}

print ("ChatroomInfo"."|".$total_chatroom_text."|".$total_chatroom_voc."|".$total_chatroom_img."|".$totalmsg."|".$chatmember."|".$active_chatroom."|".$vocsize."|".$date."\n");
# Last Version 2013-9-17 15:19 360_ContactVoice_GPvCfg.pl
