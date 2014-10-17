#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

$| = 1;

my $mskdownload_flag = "msk.apk";
my $mskdownload_total_number = 0;

my $xy_download_page = "f=hd.xy";
my $xy_firstpage_flag = "GET /hd/xy/xy.html";
my $xy_secondpage_flag = "GET /hd/hd_biaobai";
my $xy_thirdpage_flag = "GET /hd/hd_result";

my $zq_download_page = "f=hd.zq";
my $zq_firstpage_flag = "GET /hd/zq/start/zq.html";
my $zq_secondpage_flag = "GET /hd/zq_send";
my $zq_thirdpage_flag = "GET /hd/zq_bless_view";

my $sy_download_page = "f=hd.msklm";
my $sy_firstpage_flag = "GET /hd/msklm/game.html";

my $app_flag = "isapp=1";

my %mskdownload_page_download;
my %open_firstpage_totalnumber;
my %open_secondpage_totalnumber;
my %open_thirdpage_totalnumber;
my %open_firstpage_appnumber;
my %open_secondpage_appnumber;
my %open_thirdpage_appnumber;

while(<STDIN>)
{
        my $row = $_;
        chomp($row);
        if($row =~ m/$mskdownload_flag/)
        {
		if($row =~ m/$xy_download_page/)
		{
			$mskdownload_page_download{"HD_XY"} += 1;
		}
		elsif($row =~ m/$zq_download_page/)
		{	
			$mskdownload_page_download{"HD_ZQ"} += 1;
		}
		elsif($row =~ m/$sy_download_page/)
		{	
			$mskdownload_page_download{"HD_SY"} += 1;
		}
                $mskdownload_total_number += 1;
        }
        #表白活动
	elsif($row =~ m/$xy_firstpage_flag/)
	{
		if($row =~ m/$app_flag/)
		{
			$open_firstpage_appnumber{"XY_FIRSTAPP"} += 1;		
		}
		$open_firstpage_totalnumber{"XY_FIRSTOTAL"} += 1;
	}
	elsif($row =~ m/$xy_secondpage_flag/)
	{
		if($row =~ m/$app_flag/)
		{
			$open_secondpage_appnumber{"XY_SECONDAPP"} += 1;		
		}
		$open_secondpage_totalnumber{"XY_SECONDTOTAL"} += 1;
	}
	elsif($row =~ m/$xy_thirdpage_flag/)
	{
		if($row =~ m/$app_flag/)
		{
			$open_thirdpage_appnumber{"XY_THIRDAPP"} += 1;		
		}
		$open_thirdpage_totalnumber{"XY_THIRDTOTAL"} += 1;
	}
        #中秋活动
	elsif($row =~ m/$zq_firstpage_flag/)
	{
		if($row =~ m/$app_flag/)
		{
			$open_firstpage_appnumber{"ZQ_FIRSTAPP"} += 1;		
		}
		$open_firstpage_totalnumber{"ZQ_FIRSTOTAL"} += 1;
	}
	elsif($row =~ m/$zq_secondpage_flag/)
	{
		if($row =~ m/$app_flag/)
		{
			$open_secondpage_appnumber{"ZQ_SECONDAPP"} += 1;		
		}
		$open_secondpage_totalnumber{"ZQ_SECONDTOTAL"} += 1;
	}
	elsif($row =~ m/$zq_thirdpage_flag/)
	{
		if($row =~ m/$app_flag/)
		{
			$open_thirdpage_appnumber{"ZQ_THIRDAPP"} += 1;		
		}
		$open_thirdpage_totalnumber{"ZQ_THIRDTOTAL"} += 1;
	}
        #十一活动
	elsif($row =~ m/$sy_firstpage_flag/)
	{
		if($row =~ m/$app_flag/)
		{
			$open_firstpage_appnumber{"SY_FIRSTAPP"} += 1;		
		}
		$open_firstpage_totalnumber{"SY_FIRSTOTAL"} += 1;
	}

}
print("MSK_DOWNLOAD"."\t".$mskdownload_total_number."\n");
foreach my $key(keys %mskdownload_page_download)
{
	print("PAGE_DOWNLOAD"."\t".$key."\t".$mskdownload_page_download{$key}."\n");
}
foreach my $key(keys %open_firstpage_appnumber)
{
	print("FIRST_APP"."\t".$key."\t".$open_firstpage_appnumber{$key}."\n");
}
foreach my $key(keys %open_secondpage_appnumber)
{
	print("SECOND_APP"."\t".$key."\t".$open_secondpage_appnumber{$key}."\n");
}
foreach my $key(keys %open_thirdpage_appnumber)
{
	print("THIRD_APP"."\t".$key."\t".$open_thirdpage_appnumber{$key}."\n");
}
foreach my $key(keys %open_firstpage_totalnumber)
{
	print("FIRST_TOTAL"."\t".$key."\t".$open_firstpage_totalnumber{$key}."\n");
}
foreach my $key(keys %open_secondpage_totalnumber)
{
	print("SECOND_TOTAL"."\t".$key."\t".$open_secondpage_totalnumber{$key}."\n");
}
foreach my $key(keys %open_thirdpage_totalnumber)
{
	print("THIRD_TOTAL"."\t".$key."\t".$open_thirdpage_totalnumber{$key}."\n");
}
