#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $current_date = $ENV{'CURRENT_DATE'};
#my $current_date ='20131104';
my $seconds = 0;

my %remain_count;
my $register_date = "";
my $online_date = "";
my $last_id = "";

sub datesc
{
    my $red = shift;
    my $time = $seconds + ($red * 86400);
    my ($sec,$min,$hour,$day,$mon,$year) = (localtime $time)[0..5];
    $year += 1900;
    $mon += 1;
    if($mon<10)
    {
       $mon="0$mon"
    }
    if($day<10)
    {
       $day="0$day"
    }
    my $datesc = "$year$mon$day";
    return $datesc;
}

if(length($current_date) == 8)
{
    $seconds = mktime(0,0,0,substr($current_date,6,2),substr($current_date,4,2)-1,substr($current_date,0,4)-1900);
}
else
{
    print("current_date ERR \n");
    exit(0);
}

$remain_count{&datesc(-1)} = 0;
$remain_count{&datesc(-2)} = 0;
$remain_count{&datesc(-3)} = 0;
$remain_count{&datesc(-4)} = 0;
$remain_count{&datesc(-5)} = 0;
$remain_count{&datesc(-6)} = 0;
$remain_count{&datesc(-7)} = 0;

#Map Output Format
#r0[0] r0[1]       r[1]
#jid_1/2           日期


while(<STDIN>)
{
    my $row = $_;
    chomp($row);
	
    my @r = split(/\t/, $row);
    my @r0 = split(/_/, $r[0]);
	
    if(($last_id ne $r0[0])&&($last_id ne ""))
    {
        if(($register_date != "")&&($online_date != "")&&(length($register_date) == 8)&&(length($online_date) == 8))
        {
            $remain_count{$register_date} += 1;
        }
		
        $register_date = "";
        $online_date = "";
    }
	
    if(($r0[1] == 1)&&(length($r[1]) == 8))
    {
        $register_date = $r[1];
    }
	
    if(($r0[1] == 2)&&(length($r[1]) == 8))
    {
        $online_date = $r[1];
    }
			
    $last_id = $r0[0];
}

if(($register_date != "")&&($online_date != "")&&(length($register_date) == 8)&&(length($online_date) == 8))
{
    $remain_count{$register_date} += 1;
}

my $output;
foreach my $registerdate (sort {$b<=>$a} keys %remain_count)
{  
    $output .= "\t" . $remain_count{$registerdate};
}  

print($current_date . $output . "\n");

























