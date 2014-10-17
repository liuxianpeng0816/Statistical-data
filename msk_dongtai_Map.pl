#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

$| = 1;

my $create_num = 0;
my $heart_num = 0;
my $comment_num = 0;
my $create_flag = "do_createsubjectreq";
my $heart_flag = "do_heartsubjectreq";
my $comment_flag = "do_createcommentreq";

#2014-07-03 15:56:39 651, do_createsubjectreq: addsubject: user:500000008, sid:gMkUtwy1UwAACQAA
#2014-07-03 15:56:58 986, do_heartsubjectreq: likesubject: user:500000013, sid:gMkUtwy1UwAACQAA, like:1
#2014-07-03 15:57:06 1075, do_createcommentreq: addcomment: user:500000013, cid:gMkU0gy1UwAACgAA, sid:gMkUtwy1UwAACQAA
while(<STDIN>)
{
        my $row = $_;
	chomp($row);
	$row =~ s/\s//g;
	my @r = split(/,/, $row);
	my @devide = split(/\:/, $r[1]);
	
	if($row =~ m/$create_flag/)
	{
		$create_num += 1;
		print("create"."|".$devide[3]."\n");
	}
	elsif($row =~ m/$heart_flag/)
	{
		$heart_num += 1;
		print("heart"."|".$devide[3]."\n");
	}
	elsif($row =~ m/$comment_flag/)
	{
		$comment_num += 1;
		print("comment"."|".$devide[3]."\n");
	}

}
print("Count"."|".$create_num."|".$heart_num."|".$comment_num."\n");
