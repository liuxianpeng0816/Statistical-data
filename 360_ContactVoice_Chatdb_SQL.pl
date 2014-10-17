#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $mysql_table1;
my $mysql_table2;

if(@ARGV == 2)
{    
    $mysql_table1 = $ARGV[0];
    $mysql_table2 = $ARGV[1];
}
else
{
    print("Parameter Error! or Input mysql_table name\n");
    exit(0);
}

my %each_playmember;
my %each_chatmember;
my %each_totalmsg;

my $total_chat_text = 0;
my $total_chat_voc = 0;
my $total_chat_img = 0;
my $newchat = 0;
my $active_chatroom = 0;
my $totalmsg = 0;
my $chatmember = 0;
my $vocsize = 0;
my $date = "";


print("CREATE TABLE IF NOT EXISTS " . $mysql_table1 . " (DATES datetime  not null,TYPES char(20),DIME char(20),VAL integer  null,constraint PK_".$mysql_table1." primary key clustered (DATES, TYPES, DIME)) ;" . "\n"); 
print("CREATE TABLE IF NOT EXISTS " . $mysql_table2 . " (DATES datetime  not null,TYPES char(20),DIME char(40),PLAYVAL integer  null,CHATVAL integer  null,MSGVAL integer  null,constraint PK_".$mysql_table2." primary key clustered (DATES, TYPES,DIME)) ;" . "\n"); 

while(<STDIN>)
{
	my $row = $_;
	chomp($row);

	my @data_array = split(/\|/, $row);
	my $array_len = @data_array;

	if($array_len == 9)
	{
		$total_chat_text += $data_array[1];
		$total_chat_voc += $data_array[2];
		$total_chat_img += $data_array[3];
		$totalmsg += $data_array[4];
		$chatmember += $data_array[5];
		$active_chatroom += $data_array[6];
		$vocsize += $data_array[7];
		$date = $data_array[8];
		$date =~ s/\s//g;
                $date = $date."0000";	
	}
	elsif($array_len == 6)
	{
		
                my $each_gamename = $data_array[1];
                $each_playmember{$each_gamename} += $data_array[2];
		$each_chatmember{$each_gamename} += $data_array[3];
		$each_totalmsg{$each_gamename} += $data_array[4];

		$date = $data_array[5];
		$date =~ s/\s//g;	
                $date = $date."0000";	
	}


}

if( $date != "" )
{
    print("INSERT INTO " . $mysql_table1 . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . ","  . "\"MSGTYPE\"" . "," ."\"TEXT\"".",".$total_chat_text. ");\n"); 
    print("INSERT INTO " . $mysql_table1 . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . ","  . "\"MSGTYPE\"" . "," ."\"VOC\"".",". $total_chat_voc. ");\n");
    print("INSERT INTO " . $mysql_table1 . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . ","  . "\"MSGTYPE\"" . "," . "\"IMG\"".",".$total_chat_img. ");\n");
    print("INSERT INTO " . $mysql_table1 . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . ","  . "\"MSGTYPE\"" . "," ."\"VOCSIZE\"".",".$vocsize. ");\n");
    print("INSERT INTO " . $mysql_table1 . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . ","  . "\"MSG\"" .",". "\"ALL\""."," . $totalmsg. ");\n");
    print("INSERT INTO " . $mysql_table1 . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . ","  . "\"CHATMEM\"" .",". "\"ALL\""."," . $chatmember. ");\n");
    print("INSERT INTO " . $mysql_table1 . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . ","  . "\"ACTIVE\"" . "," ."\"ALL\"".",".$active_chatroom. ");\n");


    print("INSERT INTO " . $mysql_table1 . " SELECT \"". $date ."\",\"MSGTYPE\", \"TOTALTEXT\", sum(VAL) FROM ".$mysql_table1 ." WHERE types =\"MSGTYPE\" AND DIME = \"TEXT\" AND DATES <= \"".$date."\";\n"); 
    print("INSERT INTO " . $mysql_table1 . " SELECT \"". $date ."\",\"MSGTYPE\", \"TOTALIMG\", sum(VAL) FROM ".$mysql_table1 ." WHERE types =\"MSGTYPE\" AND DIME = \"IMG\" AND DATES <= \"".$date."\";\n"); 
    print("INSERT INTO " . $mysql_table1 . " SELECT \"". $date ."\",\"MSGTYPE\", \"TOTALVOC\", sum(VAL) FROM ".$mysql_table1 ." WHERE types =\"MSGTYPE\" AND DIME = \"VOC\" AND DATES <= \"".$date."\";\n"); 
    print("INSERT INTO " . $mysql_table1 . " SELECT \"". $date ."\",\"MSGTYPE\", \"TOTALVOCSIZE\", sum(VAL) FROM ".$mysql_table1 ." WHERE types =\"MSGTYPE\" AND DIME = \"VOCSIZE\" AND DATES <= \"".$date."\";\n");
    #print("INSERT INTO " . $mysql_table1 . " SELECT \"". $date ."\",\"TOTAL_GSMS\", \"ALL\" ,sum(VAL) FROM ". $mysql_table1 ." WHERE TYPES=\"GSMS\" AND DATES <= ".$date.";\n"); 
}
my $gamename;
foreach $gamename (keys %each_playmember)
{
    print("INSERT INTO " . $mysql_table2 . " (DATES,TYPES,DIME,PLAYVAL,CHATVAL,MSGVAL) VALUES (" . $date . ","  . "\"GAMEINFO\"".","."\"$gamename\"" . "," .$each_playmember{$gamename}.",".$each_chatmember{$gamename}.",".$each_totalmsg{$gamename}. ");\n"); 
}
