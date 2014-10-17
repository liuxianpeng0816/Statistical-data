#!/usr/bin/perl
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
        #JID
        if(($r[5] == 1)&&($r[3] != "")&&($r[3] != "UNKNOW"))
        {
            print($r[3] . "\n");
        }
        #print($r[3]."_".$r[1]."\t".$r[2]."|".$r[4]."|".$r[5]."|".$r[6]."|".$r[7]."|".$r[8]."|".$r[9]."|".$r[10]."+".$r[11]."|".$r[12]."|".$r[13]."|".$r[14]."\n");
    }
	
}

