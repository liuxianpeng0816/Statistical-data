#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $mysql_table;
my $date;
my %vocusrs;
my %peerusrs;

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
	
    if($length != 3)
    {
        next;
    }
	
    if($length == 3)
    {
        $date = $r[0];
        my $types = $r[1];
        my $val = $r[2];
        if($types eq "VOCNUM")
        {
            $vocusrs{$types} += $val;
        }
        elsif($types eq "CHATMEM")
        {
            $peerusrs{$types} += $val;
        }
    }
	
}

foreach my $vocusr(keys %vocusrs)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," . "\"". $vocusr ."\"". "," . "\"" ."ALL" ."\"". "," . "\"".$vocusrs{$vocusr}. "\"".");\n"); 
}
foreach my $peerusr(keys %peerusrs)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," . "\"". $peerusr ."\"". "," . "\"" ."ALL" ."\"". "," . "\"".$peerusrs{$peerusr}. "\"".");\n"); 
}


