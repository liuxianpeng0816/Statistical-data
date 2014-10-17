#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $current_date = $ENV{'CURRENT_DATE'};

my $active_usr_num = 0;
my $uniq_active_usr_num = 0;
my $last_key = "";
my $last_id = "";
my $total_use_time;
my $firstlogin_flag = 0;
my $firstlogin_time = "";
my $firstlogout_flag = 0;
my $firstlogout_time = "";
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
        #active user	
	if(@r == 3)
	{
		if(($last_key ne $r[0])&&($last_key ne ""))
		{
			$active_usr_num ++;
			if(($login_num == $logout_num) && ($logout_time > $login_time))
			{
				$total_use_time += $logout_time - $login_time;
				if($firstlogout_time ne "")
				{
					$total_use_time += 14400 - ( &stamp($current_date."00") - $firstlogout_time );
				}
			}
			elsif(($login_num - $logout_num == 1) && ($logout_time > $login_time + $lastlogin_time))
			{

				$total_use_time += $logout_time - $login_time + &stamp($current_date."00") - $lastlogin_time;
				if($firstlogout_time ne "")
				{
					$total_use_time += 14400 - ( &stamp($current_date."00") - $firstlogout_time );
				}
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
			$firstlogout_flag = 0;
		}

                #active user use time
		if($r[0] eq $last_key && $r[1] == 103 && $r[2] > $firstlogin_time)
		{
			$logout_time += int($r[2]/1000);
			$logout_num++;
		}
		if($r[1] == 102)
		{
			$login_time += int($r[2]/1000);	
			$login_num++;
			$lastlogin_time = int($r[2]/1000);
			if($firstlogin_flag == 0)
			{
				$firstlogin_time = int($r[2]/1000);
				$firstlogin_flag = 1;
			} 

		}
		elsif($r[1] == 103 && $r[0] ne $last_key)
		{
			if($firstlogout_flag == 0)
			{
				$firstlogout_time = int($r[2]/1000);
				$firstlogout_flag = 1;
			} 

		}

		$last_key = $r[0];
	}
	if(@r == 1)
	{
		if(($last_id ne $r[0])&&($last_id ne ""))
		{
			$uniq_active_usr_num ++;
		}
		$last_id = $r[0];
	}

}
if($last_key ne "")
{

	$active_usr_num ++;

	if(($login_num == $logout_num) && ($logout_time > $login_time))
	{
		$total_use_time += $logout_time - $login_time;
		if($firstlogout_time ne "")
		{
			$total_use_time += 14400 - ( &stamp($current_date."00") - $firstlogout_time );
		}
	}
	elsif(($login_num - $logout_num == 1) && ($logout_time > $login_time + $lastlogin_time))
	{

		$total_use_time += $logout_time - $login_time + &stamp($current_date."00") - $lastlogin_time;
		if($firstlogout_time ne "")
		{
			$total_use_time += 14400 - ( &stamp($current_date."00") - $firstlogout_time );
		}
	}
	else
	{
		$total_use_time += 0;
	}
}

if($last_id ne "")
{
	$uniq_active_usr_num ++;
}
print("ACTIVE\t".$current_date."\t".$active_usr_num."\t".$uniq_active_usr_num."\t".$total_use_time."\n");

