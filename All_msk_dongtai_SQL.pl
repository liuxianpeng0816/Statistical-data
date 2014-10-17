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
my %memoutputs;
my %numoutputs;

while(<STDIN>)
{
    my $row = $_;
    chomp($row);
    #MEMBER  2014051215      1       2       1
    #NUMBER  2014051215      1       2       1	
    my @data_array = split(/\t/, $row);
    my $array_len = @data_array;
    if($array_len != 5)
    {
        next;
    }
	
    my $type = $data_array[0];
    $date = $data_array[1];
    if($type eq "MEMBER")
    {
         $memoutputs{"CREATEDONGTAI_MEM"} += $data_array[2];
	 $memoutputs{"HEARTDONGTAI_MEM"} += $data_array[3];
	 $memoutputs{"COMMENTDONGTAI_MEM"} += $data_array[4];

    }
    elsif($type eq "NUMBER")
    {
         $numoutputs{"CREATEDONGTAI_NUM"} += $data_array[2];
	 $numoutputs{"HEARTDONGTAI_NUM"} += $data_array[3];
	 $numoutputs{"COMMENTDONGTAI_NUM"} += $data_array[4];
	
    }
}
foreach my $output(keys %memoutputs)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," . "\"$output\"" . "," . "\"ALL\"" . "," . $memoutputs{$output}. ");\n"); 

}
foreach my $output(keys %numoutputs)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," . "\"$output\"" . "," . "\"ALL\"" . "," . $numoutputs{$output}. ");\n"); 
}


    print("INSERT INTO " . $mysql_table . " SELECT \"". $date ."\",\"CREATEDONGTAI_TOTAL\", \"ALL\" ,sum(VAL) FROM ". $mysql_table ." WHERE TYPES=\"CREATEDONGTAI_NUM\" AND DATES <= \"".$date."\";\n");






