#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my %area_hash;
unless (open (mobile_cn, "./mobile-cn"))
{
    die ("cannot open input file file1/n");
}
else
{
    while(<mobile_cn>)
    {
        my $row = $_;
        chomp($row);
        my @r = split(/\s/, $row);

        $area_hash{$r[0]} = $r[1];
    }
}

while(<STDIN>)
{
	my $row = $_;
	chomp($row);
	$row =~ s/\s//g;
		
	my @r = split(/\|/, $row,-1);
	
	# User Log Input
	# 0    1      2   3        4        5  6          7        8    9      10
	# 时间|手机号|JID|操作类型|网络模式|IP|客户端版本|OS及版本|机型|分辨率|渠道 
	
	#if((@r == 11)&&(length($r[1]) == 11)&&(length($r[3]) == 1)&&($r[3] == 0))
	if((@r == 12)&&(length($r[1]) == 11)&&(length($r[3]) == 1)&&($r[3] == 0))
	{
	    if((substr($r[2],0,3) eq "350")&&(length($r[2]) == 10)||($r[2] =~/[^0-9]+/))
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
		
	    my $area = "UNKNOW";
	    if(exists($area_hash{substr($r[1],0,7)}))
	    {
	       $area = $area_hash{substr($r[1],0,7)};
	    }
	    # Log Output
	    # 2        1      3        4      5  6          7         8     9     10                0
	    # JID	手机号|操作类型|网络模式|IP|客户端版本|OS及版本|机型|分辨率|渠道|用户所在地区|日期
		
	    #print($r[2]."_".$r[0]."_0"."\t".$r[1]."|".$r[3]."|".$r[4]."|".$r[5]."|".$r[6]."|".$r[7]."|".$r[8]."|".$r[9]."|".$r[10]."|".$area."\n");
	    print($r[2]."\t".$r[1]."|".$r[3]."|".$r[4]."|".$r[5]."|".$r[6]."|".$r[7]."|".lc($r[8])."|".$r[9]."|".$r[10]."|".$area."|".$r[0]."\n");
	}
	else
	{
	    if((@r == 12)&&(length($r[1]) == 11)&&(length($r[3]) == 1))
	    {
	        print("ER\t".$row."\n");
	    }
	}

	
}

