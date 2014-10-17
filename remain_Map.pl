#! /usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

while(<STDIN>)
{
    my $row = $_;
    chomp($row);
    $row =~ s/\s//g;
		
    my @r = split(/\|/, $row,-1);
	
    # Register Log Format
    # 0            1             2      3   4            5            6    7           8          9        10       11       12         13     14
    # 短信申请时间|客户端GET时间|手机号|JID|短信注册标识|注册成功标识|机型|GET网络模式|短信网关IP|客户端IP|系统类型|系统版本|客户端版本|分辨率|注册渠道
	
    if((@r == 15)&&(length($r[1]) == 14)&&(length($r[2]) == 11)&&(length($r[4]) == 1)&&(length($r[5]) == 1))
    {
        #Map Output Format
        #JID_1		注册日期yyyymmdd
        if(($r[5] == 1)&&($r[3] != "")&&($r[3] != "UNKNOW"))
        {
            print($r[3]."_"."1\t".substr($r[1],0,8)."\n");
        }
    }
		
    # User Log Input
    # 0    1      2   3        4        5  6          7        8    9      10
    # 时间|手机号|JID|操作类型|网络模式|IP|客户端版本|OS及版本|机型|分辨率|渠道 
	
    if((@r == 11)&&(length($r[1]) == 11)&&(length($r[3]) == 1)&&($r[3] == 0))
    {
        if((substr($r[2],0,3) eq "350")&&(length($r[2]) == 10))
        {
            next;
        }
        if((substr($r[2],0,3) eq "400")&&(length($r[2]) == 10))
        {
            next;
        }
		
        if((substr($r[1],0,1) ne "1")&&(length($r[1]) != 11))
        {
            next;
        }
		
        # Log Output
        # 2         0
        # JID_2		日期yyyymmdd	
		
        print($r[2]."_"."2\t".substr($r[0],0,8)."\n");
    }

}

