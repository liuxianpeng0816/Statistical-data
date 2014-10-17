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
my %weimi_login;
my %weimi_single;
my %weimi_group;

while(<STDIN>)
{
    my $row = $_;
    chomp($row);
	
    my @data_array = split(/\t/, $row);
    my $array_len = @data_array;

    if($array_len != 5)
    {
        next;
    }
	
    my $type = $data_array[0];
    $date = $data_array[4];

    if($data_array[0] eq "LOGIN")
    {
    	    $weimi_login{"LOGIN"} += $data_array[1];
    }
    elsif($data_array[0] eq "SINGLE")
    {
	    $weimi_single{"SINGLESEND"} += $data_array[1];
	    $weimi_single{"SINGLESUCCESS"} += $data_array[2];
	    $weimi_single{"SINGLECOUNT"} += $data_array[3];
    }
    else
    {
	    $weimi_group{"GROUPSEND"} += $data_array[1];
	    $weimi_group{"GROUPSUCCESS"} += $data_array[2];
	    $weimi_group{"GROUPCOUNT"} += $data_array[3];
    }
    
}
foreach my $key(keys %weimi_login)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," ."\"$key\"". "," . "\"ALL\"" . "," . $weimi_login{$key}. ");\n"); 
}
foreach my $key(keys %weimi_single)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," ."\"$key\"". "," . "\"ALL\"" . "," . $weimi_single{$key}. ");\n"); 
}
foreach my $key(keys %weimi_group)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," ."\"$key\"". "," . "\"ALL\"" . "," . $weimi_group{$key}. ");\n"); 
}
