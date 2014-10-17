#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $mysql_table;
my $date;
my %data1;
my %data2;

if(@ARGV == 1)
{    
    $mysql_table = $ARGV[0];
}
else
{
    print("Parameter Error! or Input mysql_table name\n");
    exit(0);
}
print("CREATE TABLE IF NOT EXISTS " . $mysql_table . " (DATES date not null,TYPES char(15) not null,DIME varchar(64) not null,VAL  bigint(20) ,constraint PK_".$mysql_table." primary key clustered (DATES,TYPES,DIME));" . "\n"); 


while(<STDIN>)
{
    my $row = $_;
    chomp($row);
	
    my @r = split(/\t/, $row);
	
    my $length = @r;
	
    if(($length != 3)&&($length != 4))
    {
        next;
    }
	
    if($length == 3)
    {
        $date = $r[2];
        my $types = $r[0];
        my $val = $r[1];
        $data1{$types} += $val;
		
    }
	
    if($length == 4)
    {
        $date = $r[3];
        my $types = $r[0];
        my $val = $r[2];
        my $dime = $r[1];
        $data2{$types.":".$dime} += $val;
		
    }
	
}

foreach my $key(keys %data1)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," . "\"". $key ."\"". "," . "\"" ."ALL" ."\"". "," . "\"".$data1{$key}. "\"".");\n"); 
}


foreach my $key(keys %data2)
{
    my @array=split(/:/,$key);
    my $types_output = $array[0];
    my $dime_output = $array[1];
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," . "\"". $types_output."\"". "," . "\"".$dime_output."\"". "," . "\"".$data2{$key}. "\"".");\n"); 
}

print("INSERT INTO " . $mysql_table . " SELECT \"". $date ."\",\"TOTAL_GSMS\", \"ALL\" ,sum(VAL) FROM ". $mysql_table ." WHERE TYPES=\"GSMS\" AND DATES <= \"".$date."\";\n"); 
