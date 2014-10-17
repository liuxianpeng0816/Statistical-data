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
my %relation_count;

while(<STDIN>)
{
    my $row = $_;
    chomp($row);
	
    my @data_array = split(/\t/, $row);
    my $array_len = @data_array;
    if($array_len != 3)
    {
        next;
    }
	
    my $type = $data_array[0];
    $date = $data_array[2];

    if($type eq "PHONE_COUNT")
    {
	    $relation_count{"PHONE_COUNT"} += $data_array[1];
    }
    elsif($type eq "REG_COUNT")
    {
	    $relation_count{"REG_COUNT"} += $data_array[1];
    }
    elsif($type eq "AVEREG_COUNT")
    {
	    $relation_count{"AVEREG_COUNT"} += $data_array[1];
    }
    elsif($type eq "REG_THREE")
    {
	    $relation_count{"REG_THREE"} += $data_array[1];
    }
    elsif($type eq "SUBSCRIBE_ZERO")
    {
	    $relation_count{"SUBSCRIBE_ZERO"} += $data_array[1];
    }
    elsif($type eq "REG_ZERO")
    {
	    $relation_count{"REG_ZERO"} += $data_array[1];
    }
    elsif($type eq "SUBSCRIBE_COUNT")
    {
	    $relation_count{"SUBSCRIBE_COUNT"} += $data_array[1];
    }
    elsif($type eq "AVE_SUBSCRIBE")
    {
	    $relation_count{"AVE_SUBSCRIBE"} += $data_array[1];
    }
    elsif($type eq "SUBSCRIBE_MEMBER")
    {
	    $relation_count{"SUBSCRIBE_MEMBER"} += $data_array[1];
    }
}
foreach my $key(keys %relation_count)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . ","  . "\"".$key."\"" .",". "\"ALL\"" . "," . $relation_count{$key}. ");\n"); 

}
