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
my %regoutputs;
my %loginoutputs;
my %channel_count;
my $new_usr_count = 0;
my $login_usr_count = 0;

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
    $date = $data_array[1];
    if($type eq "QIAOQIAO-REG")
    {
    	my $new_usr_count = $data_array[2];
	
    	if(($date != "")&&(length($date) == 8))
    	{
        	$regoutputs{"NEW"} += $new_usr_count;
    	}
    }
    elsif($type eq "QIAOQIAO-LOGIN")
    {
    	my $login_usr_count = $data_array[2];
	
    	if(($date != "")&&(length($date) == 8))
    	{
        	$loginoutputs{"LOGIN"} += $login_usr_count;
    	}
    }
    else
    {
       my $channel_number = $data_array[2];

       $channel_count{$type} += $channel_number;     
    }
}
foreach my $output(keys %regoutputs)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," . "\"NEW\"" . "," . "\"ALL\"" . "," . $regoutputs{$output}. ");\n"); 

}
foreach my $output(keys %loginoutputs)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," . "\"LOGIN\"" . "," . "\"ALL\"" . "," . $loginoutputs{$output}. ");\n"); 
}
foreach my $output(keys %channel_count)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," . "\"CHANNEL\"" . "," . "\"".$output."\"" . "," . $channel_count{$output}. ");\n"); 
}








