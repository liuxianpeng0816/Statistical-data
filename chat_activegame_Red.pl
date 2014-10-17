#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

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
my $current_date = $ENV{'CURRENT_DATE'};
#my $current_date ='20140305';


my $gamename = "UNKNOW";
my $active_usr_num = 0;
my $last_roomid = "";
my $last_usrid = "";
my $play_number = 0;
my %room_player;
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
    my @r = split(/\|/,$row);

    if(($r[0] ne $last_roomid) && ($last_roomid ne ""))
    {
        $room_player{$last_roomid} += $play_number;
        $play_number = 0;
        $last_roomid = "";
        $last_usrid = "";     
    }

    if($r[1] ne $last_usrid )
    {
        $play_number ++;

    }    
    $last_roomid = $r[0];
    $last_usrid = $r[1];
}
if($last_roomid ne "")
{
    $room_player{$last_roomid} += $play_number;
}
my $room_id;
foreach $room_id(keys %room_player)
{
    print($room_id."\t".$current_date."\t".$room_player{$room_id}."\t".$gamename_hash{$room_id}."\n");
}
