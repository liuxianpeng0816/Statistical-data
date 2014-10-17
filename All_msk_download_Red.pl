#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $current_date = $ENV{'CURRENT_DATE'};
#my $current_date = "20140904";

my $mskdownload_total_number = 0;

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
		

    my @r = split(/\t/, $row);
    if($r[0] eq "MSK_DOWNLOAD")
    {
    	$mskdownload_total_number += $r[1];
    }
    elsif($r[0] eq "PAGE_DOWNLOAD")
    {
	if($r[1] eq "HD_XY")
	{
		$mskdownload_page_download{"HD_XY"}+=$r[2];
	}
	elsif($r[1] eq "HD_ZQ")
	{
		$mskdownload_page_download{"HD_ZQ"}+=$r[2];
	}	
	elsif($r[1] eq "HD_SY")
	{
		$mskdownload_page_download{"HD_SY"}+=$r[2];
	}	
    }
    elsif($r[0] eq "FIRST_APP")
    {
	    if($r[1] eq "XY_FIRSTAPP")
	    {
		    $open_firstpage_appnumber{"XY_FIRSTAPP"} += $r[2];
	    }
	    elsif($r[1] eq "ZQ_FIRSTAPP")
	    {
		    $open_firstpage_appnumber{"ZQ_FIRSTAPP"} += $r[2]; 
	    }
	    elsif($r[1] eq "SY_FIRSTAPP")
	    {
		    $open_firstpage_appnumber{"SY_FIRSTAPP"} += $r[2]; 
	    }
    }
    elsif($r[0] eq "SECOND_APP")
    {
	    if($r[1] eq "XY_SECONDAPP")
	    {
		    $open_secondpage_appnumber{"XY_SECONDAPP"} += $r[2]; 
	    }
	    elsif($r[1] eq "ZQ_SECONDAPP")
	    {
		    $open_secondpage_appnumber{"ZQ_SECONDAPP"} += $r[2]; 
	    }	
    }
    elsif($r[0] eq "THIRD_APP")
    {
	    if($r[1] eq "XY_THIRDAPP")
	    {
		    $open_thirdpage_appnumber{"XY_THIRDAPP"} += $r[2]; 
	    }
	    elsif($r[1] eq "ZQ_THIRDAPP")
	    {
		    $open_thirdpage_appnumber{"ZQ_THIRDAPP"} += $r[2]; 
	    }	
    }
    elsif($r[0] eq "FIRST_TOTAL")
    {
	    if($r[1] eq "XY_FIRSTOTAL")
	    {
		    $open_firstpage_totalnumber{"XY_FIRSTOTAL"} += $r[2];
	    }
	    elsif($r[1] eq "ZQ_FIRSTOTAL")
	    {
		    $open_firstpage_totalnumber{"ZQ_FIRSTOTAL"} += $r[2]; 
	    }
	    elsif($r[1] eq "SY_FIRSTOTAL")
	    {
		    $open_firstpage_totalnumber{"SY_FIRSTOTAL"} += $r[2]; 
	    }
    }
    elsif($r[0] eq "SECOND_TOTAL")
    {
	    if($r[1] eq "XY_SECONDTOTAL")
	    {
		    $open_secondpage_totalnumber{"XY_SECONDTOTAL"} += $r[2]; 
	    }
	    elsif($r[1] eq "ZQ_SECONDTOTAL")
	    {
		    $open_secondpage_totalnumber{"ZQ_SECONDTOTAL"} += $r[2]; 
	    }	
    }
    elsif($r[0] eq "THIRD_TOTAL")
    {
	    if($r[1] eq "XY_THIRDTOTAL")
	    {
		    $open_thirdpage_totalnumber{"XY_THIRDTOTAL"} += $r[2]; 
	    }
	    elsif($r[1] eq "ZQ_THIRDTOTAL")
	    {
		    $open_thirdpage_totalnumber{"ZQ_THIRDTOTAL"} += $r[2]; 
	    }	
    }
    
}

print("MSK_DOWNLOAD"."\t"."ALL"."\t".$mskdownload_total_number."\t".$current_date."\n");
foreach my $key(keys %mskdownload_page_download)
{
	print("MSK_DOWNLOAD"."\t".$key."\t".$mskdownload_page_download{$key}."\t".$current_date."\n");
}
foreach my $key(keys %open_firstpage_appnumber)
{
	print("FIRST_APP"."\t".$key."\t".$open_firstpage_appnumber{$key}."\t".$current_date."\n");
}
foreach my $key(keys %open_secondpage_appnumber)
{
	print("SECOND_APP"."\t".$key."\t".$open_secondpage_appnumber{$key}."\t".$current_date."\n");
}
foreach my $key(keys %open_thirdpage_appnumber)
{
	print("THIRD_APP"."\t".$key."\t".$open_thirdpage_appnumber{$key}."\t".$current_date."\n");
}
foreach my $key(keys %open_firstpage_totalnumber)
{
	print("FIRST_TOTAL"."\t".$key."\t".$open_firstpage_totalnumber{$key}."\t".$current_date."\n");
}
foreach my $key(keys %open_secondpage_totalnumber)
{
	print("SECOND_TOTAL"."\t".$key."\t".$open_secondpage_totalnumber{$key}."\t".$current_date."\n");
}
foreach my $key(keys %open_thirdpage_totalnumber)
{
	print("THIRD_TOTAL"."\t".$key."\t".$open_thirdpage_totalnumber{$key}."\t".$current_date."\n");
}
