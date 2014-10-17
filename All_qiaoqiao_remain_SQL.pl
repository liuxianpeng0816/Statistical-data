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

print("CREATE TABLE IF NOT EXISTS " . $mysql_table . " (DATES date, remain_day_1 integer, remain_day_2 integer, remain_day_3 integer, remain_day_4 integer, remain_day_5 integer, remain_day_6 integer,remain_day_7 integer, constraint PK_USER_".$mysql_table." primary key clustered (DATES)) ;" . "\n"); 

my $date;
my %remain_day_1;
my %remain_day_2;
my %remain_day_3;
my %remain_day_4;
my %remain_day_5;
my %remain_day_6;
my %remain_day_7;

while(<STDIN>)
{
    my $row = $_;
    chomp($row);
	
    my @data_array = split(/\t/, $row);
    my $array_len = @data_array;
    
    if($array_len != 8)
    {
        next;
    }
	
    $date = $data_array[0];
	
    if(($date != "")&&(length($date) == 8))
    {
        $remain_day_1{$date} += $data_array[1];
        $remain_day_2{$date} += $data_array[2];
        $remain_day_3{$date} += $data_array[3];
        $remain_day_4{$date} += $data_array[4];
        $remain_day_5{$date} += $data_array[5];
        $remain_day_6{$date} += $data_array[6];
        $remain_day_7{$date} += $data_array[7];
		
    }

}

print("INSERT INTO " . $mysql_table . " (DATES, remain_day_1, remain_day_2, remain_day_3, remain_day_4, remain_day_5, remain_day_6,remain_day_7) VALUES (" . $date . "," . $remain_day_1{$date} . "," . $remain_day_2{$date}. "," . $remain_day_3{$date}. "," . $remain_day_4{$date}. "," . $remain_day_5{$date}. "," . $remain_day_6{$date}. "," . $remain_day_7{$date} . ");\n"); 

