#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $current_date = $ENV{'CURRENT_DATE'};
#my $current_date ='20140126';


my $active_usr_num = 0;
my $last_id = "";
my $total_use_time;
my $firstlogin_flag = 0;
my $firstlogin_time = "";
my $lastlogin_time = "";
my $login_time = 0;
my $logout_time = 0;
my $login_num = 0;
my $logout_num = 0;
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
    my @r1 = split(/\|/, $r[0]);
    my @r2 = split(/\|/, $r[1]);
    #active user	
    if(($last_id ne $r1[0])&&($last_id ne ""))
    {
        $active_usr_num ++;
        if(($login_num == $logout_num) && ($logout_time > $login_time))
        {
            $total_use_time += $logout_time - $login_time;
        }
        #elsif($login_num - $logout_num == 1)
        elsif(($login_num - $logout_num == 1) && ($logout_time > $login_time + $lastlogin_time))
        {

            $total_use_time += $logout_time - $login_time - $lastlogin_time;
        }
        else
        {
            $total_use_time += 0;
        }
            $login_time = 0;
            $login_num  = 0;
            $logout_time = 0;
            $logout_num  = 0;

            $firstlogin_flag = 0;
    }

    #active user use time
    if($r1[0] eq $last_id && $r2[1] == 2 && &stamp($r1[1]) > $firstlogin_time)
    {
        $logout_time += &stamp($r1[1]);
        $logout_num++;
    }
    if($r2[1] == 0 )
    {
        $login_time += &stamp($r1[1]);	
        $login_num++;
        $lastlogin_time = &stamp($r1[1]);
        if($firstlogin_flag == 0)
        {
            $firstlogin_time = &stamp($r1[1]);
            $firstlogin_flag = 1;
        } 
	
    }

    $last_id = $r1[0];
	
}
if($last_id ne "")
{
    $active_usr_num ++;

    if($login_num == $logout_num)
    {
        $total_use_time += $logout_time - $login_time;
    }
    elsif($login_num - $logout_num == 1)
    {
        $total_use_time += $logout_time - $login_time - $lastlogin_time;
    }
    else
    {
        $total_use_time += 0;
    }
}

print("ACTIVE\t".$current_date."\t".$active_usr_num."\t".$total_use_time."\n");

