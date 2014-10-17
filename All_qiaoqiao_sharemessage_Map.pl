#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

$| = 1;

my $receiver_num = 0;
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
    my $datarow = $_;
    chomp($datarow);

    #[info] [<0.1577.0>:cmd_share_subject:179] <action:"share"><subject_id:"1000000434_LQAAF6hgUwA=1"><sender_id:"1000000434"><receiver_num:1><timestamp:"1398843439713">
	
    my @r = split(/\].*?</, $datarow);	
    my %inf;

    my @l = split(/><?/, $r[2]);
    for(my $a = 0; $a < @l; $a ++)
    {
        $l[$a] =~ s/\"//g;
        my @kv = split(/:/, $l[$a]);
        $inf{$kv[0]} = $kv[1];
    }

    print($inf{"sender_id"}."\t".substr(&rstamp(int($inf{"timestamp"}/1000)), 0, 10)."0000"."\n");
    print("receiver_num"."\t".$inf{"receiver_num"}."\t".substr(&rstamp(int($inf{"timestamp"}/1000)), 0, 10)."0000"."\n");

}
