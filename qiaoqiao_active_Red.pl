#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $current_date = $ENV{'CURRENT_DATE'};
$current_date = $current_date."0000";

my %post_count; 
my %post_member; 
my $active_member = 0;
my @r;
my $lastkey = "";
my $lastjid = "";
while(<STDIN>)
{
    my $row = $_;
    chomp($row);
    @r = split(/\t/, $row);
    my $array_len = @r;
    my @devide = split(/\|/,$r[0]); 
    if($array_len eq 1 )
    {
	    if( $devide[0] eq "postmessage")
	    {
		    if( $r[0] ne $lastkey )
		    {
			    $post_member{"postmessage_member"} += 1;
		    }
		    $lastkey = $r[0];
	    }
	    elsif( $devide[0] eq "browselist")
	    {
		    if( $r[0] ne $lastkey )
		    {
			    $post_member{"browselist_member"} += 1;
		    }
		    $lastkey = $r[0];

	    }
	    elsif( $devide[0] eq "browsedetail")
	    {
		    if( $r[0] ne $lastkey )
		    {
			    $post_member{"browsedetail_member"} += 1;
		    }
		    $lastkey = $r[0];
	    }
	    elsif( $devide[0] eq "browsereply")
	    {
		    if( $r[0] ne $lastkey )
		    {
			    $post_member{"browsereply_member"} += 1;
		    }
		    $lastkey = $r[0];
	    }
	    elsif( $devide[0] eq "likemessage")
	    {
		    if( $r[0] ne $lastkey )
		    {
			    $post_member{"likemessage_member"} += 1;
		    }
		    $lastkey = $r[0];
	    }
	    elsif( $devide[0] eq "likereply")
	    {
		    if( $r[0] ne $lastkey )
		    {
			    $post_member{"likereply_member"} += 1;
		    }
		    $lastkey = $r[0];
	    }
	    elsif( $devide[0] eq "reply")
	    {
		    if( $r[0] ne $lastkey )
		    {
			    $post_member{"reply_member"} += 1;
		    }
		    $lastkey = $r[0];
	    }
	    elsif( $devide[0] eq "subscribe")
	    {
		    if( $r[0] ne $lastkey )
		    {
			    $post_member{"subscribe_member"} += 1;
		    }
		    $lastkey = $r[0];
	    }
	    elsif( $devide[0] eq "feedback")
	    {
		    if( $r[0] ne $lastkey )
		    {
			    $post_member{"feedback_member"} += 1;
		    }
		    $lastkey = $r[0];
	    }
	    elsif( $devide[0] eq "report")
	    {
		    if( $r[0] ne $lastkey )
		    {
			    $post_member{"report_member"} += 1;
		    }
		    $lastkey = $r[0];
	    }
	    elsif( $devide[0] eq "remove")
	    {
		    if( $r[0] ne $lastkey )
		    {
			    $post_member{"remove_member"} += 1;
		    }
		    $lastkey = $r[0];
	    }
    }
    elsif($array_len eq 2 )
    {
	    if( $r[0] ne $lastjid )
	    {
		    $active_member += 1;
	    }
	    $lastjid = $r[0];
    }	
    elsif($array_len > 2 )
    {


	    $post_count{"postmessage"} += $r[1];
	    $post_count{"browselist"} += $r[2];
	    $post_count{"browsedetail"} += $r[3];
	    $post_count{"browsereply"} += $r[4];
	    $post_count{"likemessage"} += $r[5];
	    $post_count{"likereply"} += $r[6];
	    $post_count{"reply"} += $r[7];
	    $post_count{"subscribe"} += $r[8];
	    $post_count{"feedback"} += $r[9];
	    $post_count{"report"} += $r[10];
	    $post_count{"remove"} += $r[11];
    }

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

print("active"."\t".$active_member."\t".$current_date."\n");
