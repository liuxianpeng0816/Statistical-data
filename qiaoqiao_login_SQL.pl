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

my $date;
my %message_count;
my %message_member;

while(<STDIN>)
{
    my $row = $_;
    chomp($row);
	
    my @data_array = split(/\t/, $row);
    my $array_len = @data_array;
    #getmessage_member	1487	201406040000
    #MessageCount	23	10471	201406040000
    #sendmessage_member	14	201406040000
    #if($array_len != 3)
    #{
    #    next;
    #}
	
    my $type = $data_array[0];
    if($array_len == 3)
    {
    	$date = $data_array[2];
    }
    elsif($array_len == 4)
    {
    	$date = $data_array[3];
    }

    if( $data_array[0] eq "getmessage_member")
    {
	    $message_member{"GETMEM"} += $data_array[1];
    }
    elsif( $data_array[0] eq "sendmessage_member")
    {
	    $message_member{"SENDMEM"} += $data_array[1];
    }
    elsif( $data_array[0] eq "MessageCount")
    {
	    $message_count{"SENDNUM"} += $data_array[1];
	    $message_count{"GETNUM"} += $data_array[2];
    }
}
foreach my $key(keys %message_member)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," ."\"$key\"". "," . "\"ALL\"" . "," . $message_member{$key}. ");\n"); 


}
foreach my $key(keys %message_count)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," ."\"$key\"" . "," . "\"ALL\"" . "," . $message_count{$key}. ");\n"); 
}








