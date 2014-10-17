#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;


my $mysql_table;

if(@ARGV == 1)
{    
    $mysql_table = $ARGV[0];
}
else
{
    print("Parameter Error! or Input mysql_table name\n");
    exit(0);
}

print("CREATE TABLE IF NOT EXISTS " . $mysql_table . " (DATES date, TYPES char(20),DIME varchar(20),VAL bigint,  constraint PK_".$mysql_table." primary key clustered (DATES,TYPES,DIME)) ;" . "\n"); 

my $date;

my %mskdownload;
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
    my $array_len = @r;
    if($array_len != 4)
    {
        next;
    }
	
    my $type = $r[0];
    $date = $r[3];

    if($type eq "MSK_DOWNLOAD")
    {
         if($r[1] eq "ALL")
	 {
		$mskdownload{"MSKDOWNLOAD_ALL"} += $r[2];
         }
         elsif($r[1] eq "HD_XY")
	 {
		$mskdownload{"HD_XY_DOWNLOAD"}+= $r[2];
         }
         elsif($r[1] eq "HD_ZQ")
         {
		$mskdownload{"HD_ZQ_DOWNLOAD"}+= $r[2]; 
	 }
         elsif($r[1] eq "HD_SY")
         {
		$mskdownload{"HD_SY_DOWNLOAD"}+= $r[2]; 
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


foreach my $key(keys %mskdownload)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," ."\"$key\"". "," . "\"ALL\"" . "," . $mskdownload{$key}. ");\n"); 
}

foreach my $key(keys %open_firstpage_appnumber)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," ."\"$key\"". "," . "\"APP\"" . "," . $open_firstpage_appnumber{$key}. ");\n"); 
}

foreach my $key(keys %open_secondpage_appnumber)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," ."\"$key\"". "," . "\"APP\"" . "," . $open_secondpage_appnumber{$key}. ");\n"); 
}

foreach my $key(keys %open_thirdpage_appnumber)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," ."\"$key\"". "," . "\"APP\"" . "," . $open_thirdpage_appnumber{$key}. ");\n"); 
}

foreach my $key(keys %open_firstpage_totalnumber)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," ."\"$key\"". "," . "\"TOTAL\"" . "," . $open_firstpage_totalnumber{$key}. ");\n"); 
}

foreach my $key(keys %open_secondpage_totalnumber)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," ."\"$key\"". "," . "\"TOTAL\"" . "," . $open_secondpage_totalnumber{$key}. ");\n"); 
}

foreach my $key(keys %open_thirdpage_totalnumber)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," ."\"$key\"". "," . "\"TOTAL\"" . "," . $open_thirdpage_totalnumber{$key}. ");\n"); 
}

print("INSERT INTO " . $mysql_table . " SELECT \"". $date ."\",\"MSKDOWNLOAD_TOTAL\", \"ALL\" ,sum(VAL) FROM ". $mysql_table ." WHERE TYPES=\"MSKDOWNLOAD\" or TYPES=\"MSKDOWNLOAD_ALL\" AND DATES <= \"".$date."\";\n");
 


