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
my %replyoutputs;

while(<STDIN>)
{
    my $row = $_;
    chomp($row);

    my @data_array = split(/\t/, $row);
    my $array_len = @data_array;

    #MSKREPLY_MEMBER 1       2014062409
    #MSKREPLY_NUMBER 3       2014062409

    if($array_len != 3)
    {
        next;
    }
	
    my $type = $data_array[0];
    $date = $data_array[2];
    if($type eq "MSKREPLY_MEMBER")
    {
	$replyoutputs{"MSKREPLY_MEMBER"} += $data_array[1];
    }
    elsif($type eq "MSKREPLY_NUMBER")
    {
	$replyoutputs{"MSKREPLY_NUMBER"} += $data_array[1];
    }
}
$date = $date."0000";

foreach my $output(keys %replyoutputs)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," . "\"MSKREPLY\"" . "," . "\"".$output."\"" . "," . $replyoutputs{$output}. ");\n"); 

}
