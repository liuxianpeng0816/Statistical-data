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

print("CREATE TABLE IF NOT EXISTS " . $mysql_table . " (DATES datetime, TYPES char(20),DIME varchar(20),VAL bigint,  constraint PK_".$mysql_table." primary key clustered (DATES,TYPES,DIME)) ;" . "\n"); 

my $sendmessage_count; 
my $viewmessage_count; 
my $receiver_num; 
my $date;

while(<STDIN>)
{
    my $row = $_;
    chomp($row);

    #sendmessage	2	20140430150000

    my @data_array = split(/\t/, $row);
    my $array_len = @data_array;

    if($array_len != 3)
    {
        next;
    }
	
    my $type = $data_array[0];
    $date = $data_array[2];

    if( $data_array[0] eq "sendmessage")
    {
	    $sendmessage_count += $data_array[1];
    }
    elsif($data_array[0] eq "viewmessage")
    {
	    $viewmessage_count += $data_array[1];
    }
}
print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," ."\"SHARE\"". "," . "\"ALL\"" . "," . $sendmessage_count. ");\n"); 
print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," ."\"VIEW\"". "," . "\"ALL\"" . "," . $viewmessage_count. ");\n"); 







