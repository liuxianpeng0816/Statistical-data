#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $mysql_table1;
my $mysql_table2;

if(@ARGV == 2)
{    
    $mysql_table1 = $ARGV[0];
    $mysql_table2 = $ARGV[1];
}
else
{
    print("Parameter Error! or Input mysql_table1 mysql_table2\n");
    exit(0);
}

print("CREATE TABLE IF NOT EXISTS " . $mysql_table1 . " (DATES date, TYPES char(10),DIME varchar(64),VAL bigint(20),constraint PK_".$mysql_table1." primary key clustered (DATES,TYPES,DIME)) ;" . "\n"); 
print("CREATE TABLE IF NOT EXISTS " . $mysql_table2 . " (DATES date, TYPES char(10),DIME varchar(64),VAL bigint(20),constraint PK_".$mysql_table2." primary key clustered (DATES,TYPES,DIME)) ;" . "\n"); 

my $date;
my %active_outputs;
my %net_outputs;
my %client_outputs;
my %model_outputs;
my %chan_outputs;
my %area_outputs;

while(<STDIN>)
{
    my $row = $_;
    chomp($row);
	
    my @data_array = split(/\t/, $row);
    my $array_len = @data_array;
 
    
    if(($array_len != 3)&&($array_len != 4))
    {
        next;
    }
	
    my $type = $data_array[0];
    $date = $data_array[1];
		
    if($array_len == 3)
    {
        my $active_usr_num = $data_array[2]; 
        if(($date != "")&&(length($date) == 8)&&($type == "ACTIVE"))
        {
            $active_outputs{$type} += $active_usr_num;
        }
        next;
    }
	
    if($array_len == 4)
    {
        my $keyword = $data_array[2]; 
        my $val = $data_array[3]; 
        if(($date != "")&&(length($date) == 8))
        {
            if($type eq "NETWORK")
            {
                $net_outputs{$keyword} += $val;
            }
            elsif($type eq "CLIENT")
            {
                $client_outputs{$keyword} += $val;
            }
            elsif($type eq "MODEL")
            {
                $model_outputs{$keyword} += $val;
            }
            elsif($type eq "CHAN")
            {
                $chan_outputs{$keyword} += $val;
            }
            elsif($type eq "AREA")
            {
                $area_outputs{$keyword} += $val;
            }
		
        }
		
		
    }
	
}

foreach my $output(keys %active_outputs)
{
    print("INSERT INTO " . $mysql_table1 . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," . "\"ACTIVE\"" . "," . "\"ALL\"" . "," . $active_outputs{$output}. ");\n"); 
}


foreach my $output(keys %net_outputs)
{
    print("INSERT INTO " . $mysql_table2 . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," . "\"NETWORK\"" . "," . "\"".$output ."\"". "," . $net_outputs{$output}. ");\n"); 
}

foreach my $output(keys %client_outputs)
{
    print("INSERT INTO " . $mysql_table2 . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," . "\"CLIENT\"" . "," . "\"".$output . "\""."," . $client_outputs{$output}. ");\n"); 
}

foreach my $output(keys %model_outputs)
{
    print("INSERT INTO " . $mysql_table2 . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," . "\"MODEL\"" . "," ."\"". $output . "\""."," . $model_outputs{$output}. ");\n"); 
}

foreach my $output(keys %chan_outputs)
{
    print("INSERT INTO " . $mysql_table2 . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," . "\"CHAN\"" . "," . "\"".$output . "\""."," .$chan_outputs{$output}. ");\n"); 
}

foreach my $output(keys %area_outputs)
{
    print("INSERT INTO " . $mysql_table2 . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," . "\"AREA\"" . "," . "\"".$output . "\""."," . $area_outputs{$output}. ");\n"); 
}


