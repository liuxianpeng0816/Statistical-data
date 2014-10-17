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

print("CREATE TABLE IF NOT EXISTS " . $mysql_table . " (DATES date, TYPES char(10),DIME varchar(16),VAL bigint(20),  constraint PK_".$mysql_table." primary key clustered (DATES,TYPES,DIME)) ;" . "\n"); 

my $date;
my %online_time;
my %online_number;

while(<STDIN>)
{
    my $row = $_;
    chomp($row);
    #CHATONLINE	20140122	1211  10	
    my @data_array = split(/\t/, $row);
    my $array_len = @data_array;

    
    if($array_len != 4)
    {
        next;
    }
	
    my $type = $data_array[0];
    $date = $data_array[1];
    my $online_time = $data_array[2];
    my $online_number = $data_array[3];
	
    if(($date != "")&&(length($date) == 8))
    {
        $online_time{"ONLINE"} += $online_time;
        $online_number{"ONLINE"} += $online_number;
    }
}

foreach my $key(keys %online_time)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," . "\"ONLINETIME\"" . "," . "\"ALL\"" . "," . $online_time{$key}. ");\n"); 
}
foreach my $key(keys %online_number)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," . "\"NUMBER\"" . "," . "\"ALL\"" . "," . $online_number{$key}. ");\n"); 
}








