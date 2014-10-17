#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;


my $date ="";
my $voc_group_id=0;
my $voc_groupdb_id=0;
my $lastsetid_db="";
my $lastkey="";
my $lastsetid="";
my $lastgroupid="";
my $lastsize = 0;
my $nowsetid="";
my $chatmember = 0;
my $totalvocsize = 0;
my $size = 0;
my $vocsize =0;
my $newgroup = 0;
my $active_group = 0;
my $destorygroup =0;
my $totalmsg =0;
my $total_group_text = 0;
my $total_group_voc = 0;
my $total_group_img = 0;
my $total_group_maps = 0;
my $total_group_card = 0;
my $total_group_location = 0;
my $total_group_distext = 0;
my $total_group_disimg = 0;

my %msgtype=("group_text"=>300,"group_voc"=>301,"group_img"=>302,"group_maps"=>303,"group_card"=>306,"group_location"=>307,"group_distext"=>888100,"group_disimg"=>888101);


while(<STDIN>)
{
    my $row = $_;
    chomp($row);
    my @r = split(/\t/, $row);
    my @l = split(/\|/, $r[0]);
    #群信息统计
    if($r[2] eq "groupdb")
    {    
        $date=$r[4];
        if($r[0] eq "set")
        {
            if($msgtype{"group_text"}==$r[1])
            {
                $total_group_text+=1;
            }
            if($msgtype{"group_voc"}==$r[1])
            {
                $total_group_voc+=1;
            }
            if($msgtype{"group_maps"}==$r[1])
            {
                $total_group_maps+=1;
            }
            if($msgtype{"group_img"}==$r[1])
            {
                $total_group_img+=1;
            }
            if($msgtype{"group_card"}==$r[1])
            {
                $total_group_card+=1;
            }
            if($msgtype{"group_location"}==$r[1])
            {
                $total_group_location+=1;
            }
            if($msgtype{"group_distext"}==$r[1])
            {
                $total_group_distext+=1;
            }
            if($msgtype{"group_disimg"}==$r[1])
            {
                $total_group_disimg+=1;
            }
        }
		
        elsif($r[0] eq "create_group")
        {
            $newgroup+=1;
        }
        elsif($r[0] eq "destroy_group")
        {
            $destorygroup+=1;
        }
		
    }
		
    if(($l[0] eq "set")&&($r[1] eq "groupdb"))
    {
        $date =$r[3];
		
        if(($l[1] ne $lastsetid_db)&&($lastsetid_db ne ""))
        {
            $chatmember+=1;
        }
        if(($r[2] ne $lastkey)&&($lastkey ne ""))
        {
            $totalmsg+=1;
        }
		
        $lastsetid_db=$l[1];
        $lastkey=$r[2];
    }
    if($l[0] eq "group")
    {	
        $date =$r[2];

        if(($r[0] ne $lastsetid) && ($lastsetid ne "") )
        {
            $vocsize += $lastsize;
        }
		
        $lastsetid = $r[0];

        if($l[0] eq "group")
        {
            $lastsize = $r[1];
        }
		
    }
    if($l[0] eq "active_group")
    {	
        $date =$r[1];
        if(($r[0] ne $lastgroupid) && ($lastgroupid ne "") )
        {
            $active_group += 1;
        }
		
        $lastgroupid = $r[0];
		
    }
                
}
    if($lastsetid_db ne "")
    {
        $chatmember += 1;
    }
    if($lastgroupid ne "")
    {
        $active_group += 1;
    }
    if($lastkey ne "")
    {
        $totalmsg += 1;
    }
    $vocsize += $lastsize;
    print ("GroupInfo"."|".$total_group_text."|".$total_group_voc."|".$total_group_img."|".$total_group_maps."|".$total_group_card."|".$total_group_location."|".$total_group_distext."|".$total_group_disimg."|".$newgroup."|".$totalmsg."|".$chatmember."|".$destorygroup."|".$vocsize."|".$active_group."|".$date."\n");

# Last Version 2013-9-17 15:19 360_ContactVoice_GPvCfg.pl
