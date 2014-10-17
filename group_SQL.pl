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

my $total_group_text = 0;
my $total_group_voc = 0;
my $total_group_maps = 0;
my $total_group_img = 0;
my $total_group_card = 0;
my $total_group_location = 0;
my $total_group_distext = 0;
my $total_group_disimg = 0;
my $newgroup = 0;
my $active_group = 0;
my $totalmsg = 0;
my $chatmember = 0;
my $destorygroup = 0;
my $vocsize = 0;
my $date = "";


print("CREATE TABLE IF NOT EXISTS " . $mysql_table . " (DATES date  not null,TYPES char(20),DIME char(20),VAL integer  null,constraint PK_".$mysql_table." primary key clustered (DATES, TYPES, DIME)) ;" . "\n"); 

while(<STDIN>)
{
    my $row = $_;
    chomp($row);
		
    my @data_array = split(/\|/, $row);
    my $array_len = @data_array;

    # 0  		1        2
    #NEW_USER	日期	100   
		
    if($array_len != 16)
    {
        next;
    }
	
    $total_group_text += $data_array[1];
    $total_group_voc += $data_array[2];
    $total_group_img += $data_array[3];
    $total_group_maps += $data_array[4];
    $total_group_card += $data_array[5];
    $total_group_location += $data_array[6];
    $total_group_distext += $data_array[7];
    $total_group_disimg += $data_array[8];
    $newgroup += $data_array[9];
    $totalmsg += $data_array[10];
    $chatmember += $data_array[11];
    $destorygroup += $data_array[12];
    $vocsize += $data_array[13];
    $active_group += $data_array[14];
    $date = $data_array[15];
    $date =~ s/\s//g;	
		
}

if(($date != "")&&(length($date) == 8))
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . ","  . "\"MSGTYPE\"" . "," ."\"TEXT\"".",".$total_group_text. ");\n"); 
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . ","  . "\"MSGTYPE\"" . "," ."\"VOC\"".",". $total_group_voc. ");\n");
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . ","  . "\"MSGTYPE\"" . "," . "\"MAPS\"".",".$total_group_maps. ");\n");
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . ","  . "\"MSGTYPE\"" . "," . "\"IMG\"".",".$total_group_img. ");\n");
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . ","  . "\"MSGTYPE\"" . "," . "\"CARD\"".",".$total_group_card. ");\n");
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . ","  . "\"MSGTYPE\"" . "," ."\"LOCATION\"".",".$total_group_location. ");\n");
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . ","  . "\"MSGTYPE\"" . "," ."\"DISTEXT\"".",".$total_group_distext. ");\n");
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . ","  . "\"MSGTYPE\"" . "," ."\"DISIMG\"".",".$total_group_disimg. ");\n");
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . ","  . "\"MSGTYPE\"" .",". "\"VOCSIZE\""."," . $vocsize. ");\n");
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . ","  . "\"NEW\"" . "," ."\"ALL\"".",".$newgroup. ");\n");
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . ","  . "\"ACTIVE\"" . "," ."\"ALL\"".",".$active_group. ");\n");
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . ","  . "\"MSG\"" .",". "\"ALL\""."," . $totalmsg. ");\n");
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . ","  . "\"CHATMEM\"" .",". "\"ALL\""."," . $chatmember. ");\n");
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . ","  . "\"DESTORY\"" .",". "\"ALL\""."," . $destorygroup. ");\n");


    print("INSERT INTO " . $mysql_table . " SELECT \"". $date ."\",\"MSGTYPE\", \"TOTALIMG\", sum(VAL) FROM ".$mysql_table ." WHERE types =\"MSGTYPE\" AND DIME = \"IMG\" AND DATES <= \"".$date."\";\n"); 
    print("INSERT INTO " . $mysql_table . " SELECT \"". $date ."\",\"MSGTYPE\", \"TOTALVOC\", sum(VAL) FROM ".$mysql_table ." WHERE types =\"MSGTYPE\" AND DIME = \"VOC\" AND DATES <= \"".$date."\";\n"); 
    print("INSERT INTO " . $mysql_table . " SELECT \"". $date ."\",\"MSGTYPE\", \"TOTALVOCSIZE\", sum(VAL) FROM ".$mysql_table ." WHERE types =\"MSGTYPE\" AND DIME = \"VOCSIZE\" AND DATES <= \"".$date."\";\n");
    #print("INSERT INTO " . $mysql_table . " SELECT \"". $date ."\",\"TOTAL_GSMS\", \"ALL\" ,sum(VAL) FROM ". $mysql_table ." WHERE TYPES=\"GSMS\" AND DATES <= ".$date.";\n"); 
}

