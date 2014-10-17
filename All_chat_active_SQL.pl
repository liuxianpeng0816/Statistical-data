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
    print("Parameter Error! or Input mysql_table mysql_table2\n");
    exit(0);
}

print("CREATE TABLE IF NOT EXISTS " . $mysql_table . " (DATES date, TYPES char(10),DIME varchar(64),VAL bigint(20),constraint PK_".$mysql_table." primary key clustered (DATES,TYPES,DIME)) ;" . "\n"); 

my $date;
my $active_use_num;
my $uniq_active_use_num;
my $active_use_time;

while(<STDIN>)
{
    my $row = $_;
    chomp($row);
	
    my @data_array = split(/\t/, $row);
    my $array_len = @data_array;
    $date = $data_array[1];
    
    if($array_len == 5)
    {
        $active_use_num += $data_array[2]; 
        $uniq_active_use_num += $data_array[3]; 
        $active_use_time += $data_array[4];
        next;
    }
    else	
    {
        next;
    }
	
}

    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," . "\"ACTIVE\"" . "," . "\"USER\"" . "," . $active_use_num. ");\n"); 
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," . "\"ACTIVE\"" . "," . "\"UNIQ\"" . "," . $uniq_active_use_num. ");\n"); 
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," . "\"ACTIVE\"" . "," . "\"TIME\"" . "," . $active_use_time. ");\n"); 

