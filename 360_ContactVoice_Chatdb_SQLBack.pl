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

my $total_chat_text = 0;
my $total_chat_voc = 0;
my $total_chat_img = 0;
my $newchat = 0;
my $active_chatroom = 0;
my $totalmsg = 0;
my $chatmember = 0;
my $vocsize = 0;
my $date = "";


print("CREATE TABLE IF NOT EXISTS " . $mysql_table . " (DATES date  not null,TYPES char(20),DIME char(20),VAL integer  null,constraint PK_".$mysql_table." primary key clustered (DATES, TYPES, DIME)) ;" . "\n"); 

while(<STDIN>)
{
    my $row = $_;
    chomp($row);
		
    my @data_array = split(/\|/, $row);
    my $array_len = @data_array;

		
    if($array_len != 9)
    {
        next;
    }
	
    $total_chat_text += $data_array[1];
    $total_chat_voc += $data_array[2];
    $total_chat_img += $data_array[3];
    $totalmsg += $data_array[4];
    $chatmember += $data_array[5];
    $active_chatroom += $data_array[6];
    $vocsize += $data_array[7];
    $date = $data_array[8];
    $date =~ s/\s//g;	
		
}

if(($date != "")&&(length($date) == 8))
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . ","  . "\"MSGTYPE\"" . "," ."\"TEXT\"".",".$total_chat_text. ");\n"); 
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . ","  . "\"MSGTYPE\"" . "," ."\"VOC\"".",". $total_chat_voc. ");\n");
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . ","  . "\"MSGTYPE\"" . "," . "\"IMG\"".",".$total_chat_img. ");\n");
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . ","  . "\"MSGTYPE\"" . "," ."\"VOCSIZE\"".",".$vocsize. ");\n");
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . ","  . "\"MSG\"" .",". "\"ALL\""."," . $totalmsg. ");\n");
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . ","  . "\"CHATMEM\"" .",". "\"ALL\""."," . $chatmember. ");\n");
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . ","  . "\"ACTIVE\"" . "," ."\"ALL\"".",".$active_chatroom. ");\n");


    print("INSERT INTO " . $mysql_table . " SELECT \"". $date ."\",\"MSGTYPE\", \"TOTALTEXT\", sum(VAL) FROM ".$mysql_table ." WHERE types =\"MSGTYPE\" AND DIME = \"TEXT\" AND DATES <= \"".$date."\";\n"); 
    print("INSERT INTO " . $mysql_table . " SELECT \"". $date ."\",\"MSGTYPE\", \"TOTALIMG\", sum(VAL) FROM ".$mysql_table ." WHERE types =\"MSGTYPE\" AND DIME = \"IMG\" AND DATES <= \"".$date."\";\n"); 
    print("INSERT INTO " . $mysql_table . " SELECT \"". $date ."\",\"MSGTYPE\", \"TOTALVOC\", sum(VAL) FROM ".$mysql_table ." WHERE types =\"MSGTYPE\" AND DIME = \"VOC\" AND DATES <= \"".$date."\";\n"); 
    print("INSERT INTO " . $mysql_table . " SELECT \"". $date ."\",\"MSGTYPE\", \"TOTALVOCSIZE\", sum(VAL) FROM ".$mysql_table ." WHERE types =\"MSGTYPE\" AND DIME = \"VOCSIZE\" AND DATES <= \"".$date."\";\n");
    #print("INSERT INTO " . $mysql_table . " SELECT \"". $date ."\",\"TOTAL_GSMS\", \"ALL\" ,sum(VAL) FROM ". $mysql_table ." WHERE TYPES=\"GSMS\" AND DATES <= ".$date.";\n"); 
}

