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
my %mm_login;
my %client_version;
my %client_version_member;

while(<STDIN>)
{
    my $row = $_;
    chomp($row);
	
    my @data_array = split(/\t/, $row);
    my $array_len = @data_array;

    if($array_len != 4)
    {
        next;
    }
	
    my $type = $data_array[0];
    $date = $data_array[3];

    if($data_array[0] eq "LOGIN")
    {
    	    $mm_login{"LOGIN_MEM"} += $data_array[1];
    	    $mm_login{"LOGIN_NUM"} += $data_array[2];
    }
    elsif($data_array[0] eq "VERSION_MEMBER")
    {
	    if($data_array[1] ne "")
	    {
		$client_version_member{$data_array[1]} += $data_array[2];	
	    }
    }
    elsif($data_array[0] eq "CLIENT_VERSION")
    {
	    $client_version{$data_array[1]} += $data_array[2];	
    }
    
}
foreach my $key(keys %mm_login)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," ."\"$key\"". "," . "\"ALL\"" . "," . $mm_login{$key}. ");\n"); 
}
foreach my $key(keys %client_version_member)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," ."\"$key\"". "," . "\"VERSION_MEMBER\"" . "," . $client_version_member{$key}. ");\n"); 
}
foreach my $key(keys %client_version)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," ."\"$key\"". "," . "\"CLIENT_VERSION\"" . "," . $client_version{$key}. ");\n"); 
}
