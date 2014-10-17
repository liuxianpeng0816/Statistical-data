#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $current_date = $ENV{'CURRENT_DATE'};
#my $current_date = "20140808";

my %post_count; 
my %post_member; 
my %dialog_number; 
my %dialog_member; 
my %content_number;
my %content_member; 
my %privatemessage_number; 
my %privatemessage_member; 
my $active_member = 0;
my @r;
my $lastkey = "";
my $last_activekey = "";
my $lastjid = "";
while(<STDIN>)
{
    my $row = $_;
    chomp($row);
    @r = split(/\|/, $row);
    my $array_len = @r;
    #my @devide = split(/\|/,$r[0]);
    if($array_len eq 1)
    {
        if($r[0] ne $last_activekey && $last_activekey ne "")
        {
        	$active_member += 1;
        }
    	$last_activekey = $r[0];
    } 
    elsif($array_len eq 2 )
    {
	    if( $r[0] eq "CREATEWISH")
	    {
		    if( $r[1] ne $lastkey )
		    {
			    $post_member{"CREATEWISH_MEM"} += 1;
		    }
		    $lastkey = $r[1];
	    }
	    elsif( $r[0] eq "CREATESUBJECT")
	    {
		    if( $r[1] ne $lastkey )
		    {
			    $post_member{"CREATESUBJECT_MEM"} += 1;
		    }
		    $lastkey = $r[1];

	    }
	    elsif( $r[0] eq "SENDTEXT")
	    {
		    if( $r[1] ne $lastkey )
		    {
			    $content_member{"TEXT_MEM"} += 1;
		    }
		    $lastkey = $r[1];

	    }
	    elsif( $r[0] eq "SENDVOICE")
	    {
		    if( $r[1] ne $lastkey )
		    {
			    $content_member{"VOICE_MEM"} += 1;
		    }
		    $lastkey = $r[1];

	    }
	    elsif( $r[0] eq "SENDIMAGE")
	    {
		    if( $r[1] ne $lastkey )
		    {
			    $content_member{"IMAGE_MEM"} += 1;
		    }
		    $lastkey = $r[1];

	    }
	    elsif( $r[0] eq "DIALOGMEM")
	    {
		    if( $r[1] ne $lastkey )
		    {
			    $dialog_member{"DIALOG_MEM"} += 1;
		    }
		    $lastkey = $r[1];

	    }
	    elsif( $r[0] eq "DIALOGNUM")
	    {
	    	$dialog_number{"DIALOG_NUM"} += $r[1]; 
	    }
	    elsif( $r[0] eq "SENDASSISTANT")
	    {
		    if( $r[1] ne $lastkey )
		    {
			    $content_member{"ASSISTANT_MEM"} += 1;
		    }
		    $lastkey = $r[1];

	    }
	    elsif( $r[0] eq "ANONYMOUSCHAT")
	    {
		    if( $r[1] ne $lastkey )
		    {
			    $privatemessage_member{"ANONYMOUS_MEM"} += 1;
		    }
		    $lastkey = $r[1];

	    }
	    elsif( $r[0] eq "WISHUNMATCHED")
	    {
		    if( $r[1] ne $lastkey )
		    {
			    $privatemessage_member{"WISH_UNMATCH_MEM"} += 1;
		    }
		    $lastkey = $r[1];

	    }
	    elsif( $r[0] eq "WISHPUBLIC")
	    {
		    if( $r[1] ne $lastkey )
		    {
			    $privatemessage_member{"WISH_PUBLIC_MEM"} += 1;
		    }
		    $lastkey = $r[1];

	    }
	    elsif( $r[0] eq "DYNAMICMESSAGE")
	    {
		    if( $r[1] ne $lastkey )
		    {
			    $privatemessage_member{"DYNAMIC_MEM"} += 1;
		    }
		    $lastkey = $r[1];

	    }
    }
    elsif($array_len eq 3 )
    {
	    if($r[0] eq "WISHSUBJECT")	
	    {
		    $post_count{"CREATEWISH_NUM"} += $r[1];
		    $post_count{"CREATESUBJECT_NUM"} += $r[2];
	    }
    }	
    elsif($array_len eq 5 )
    {
	    if($r[0] eq "CONTENTCOUNT")	
	    {
		    $content_number{"TEXT_NUM"} += $r[1];
		    $content_number{"VOICE_NUM"} += $r[2];
		    $content_number{"IMAGE_NUM"} += $r[3];
		    $content_number{"ASSISTANT_NUM"} += $r[4];
	    }
	    elsif($r[0] eq "PRIVATEMESSAGE")
	    {

		    $privatemessage_number{"ANONYMOUS_NUM"} += $r[1]; 
		    $privatemessage_number{"WISH_UNMATCHED_NUM"} += $r[2]; 
		    $privatemessage_number{"WISH_PUBLIC_NUM"} += $r[3]; 
		    $privatemessage_number{"DYNAMIC_MESSAGE_NUM"} += $r[4]; 
	    }
    }	

}
if($last_activekey ne "")
{
	$active_member += 1;
}
my $key; 
foreach $key(keys %post_count)
{
    print($key."\t".$post_count{$key}."\t".$current_date."\n");
}
foreach $key(keys %post_member)
{
    print($key."\t".$post_member{$key}."\t".$current_date."\n");
}
foreach $key(keys %content_number)
{
    print($key."\t".$content_number{$key}."\t".$current_date."\n");
}
foreach $key(keys %content_member)
{
    print($key."\t".$content_member{$key}."\t".$current_date."\n");
}
foreach $key(keys %dialog_number)
{
    print($key."\t".$dialog_number{$key}."\t".$current_date."\n");
}
foreach $key(keys %dialog_member)
{
    print($key."\t".$dialog_member{$key}."\t".$current_date."\n");
}
foreach $key(keys %privatemessage_number)
{
    print($key."\t".$privatemessage_number{$key}."\t".$current_date."\n");
}
foreach $key(keys %privatemessage_member)
{
    print($key."\t".$privatemessage_member{$key}."\t".$current_date."\n");
}
print("ACTIVE"."\t".$active_member."\t".$current_date."\n");
