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
my %outputs;

while(<STDIN>)
{
    my $row = $_;
    chomp($row);
	
    my @data_array = split(/\t/, $row);
    my $array_len = @data_array;

    # 0  		1        2
    #NEW_USER	日期	100   
    
    if($array_len != 3)
    {
        next;
    }
	
    my $type = $data_array[0];
    $date = $data_array[1];
    my $new_usr_count = $data_array[2];
	
    if(($date != "")&&(length($date) == 8))
    {
        $outputs{"NEW"} += $new_usr_count;
    }
}

foreach my $output(keys %outputs)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," . "\"NEW\"" . "," . "\"ALL\"" . "," . $outputs{$output}. ");\n"); 
}








