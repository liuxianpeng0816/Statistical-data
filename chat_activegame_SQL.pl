#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $date;
my %active_room_player;
my %active_game_name;
my $room_id;
my $mysql_table;

if(@ARGV == 1)
{    
    $mysql_table = $ARGV[0];
}
else
{
    print("Parameter Error! or Input mysql_table mysql_table2\n");
    exit(0);
}

#print("CREATE TABLE IF NOT EXISTS " . $mysql_table . " (DATES date, TYPES char(10),DIME varchar(64),VAL bigint(20),constraint PK_".$mysql_table." primary key clustered (DATES,TYPES,DIME)) ;" . "\n");
 
print("CREATE TABLE IF NOT EXISTS " . $mysql_table . " (DATES datetime, TYPES char(10), DIME char(64), VAL bigint(20) ,constraint PK_".$mysql_table." primary key clustered (DATES,TYPES,DIME)) ;" . "\n");

while(<STDIN>)
{
    my $row = $_;
    chomp($row);
	
    my @data_array = split(/\t/, $row);
    my $array_len = @data_array;
    $room_id = $data_array[0];
    $date = $data_array[1];
    $date = $date."0000";
    
    if($array_len == 4)
    {
        $active_room_player{$room_id} += $data_array[2];
        $active_game_name{$room_id} = $data_array[3]; 
        next;
    }
    else	
    {
        next;
    }
	
}
foreach $room_id(keys %active_room_player)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," . "\"GAME\"" . "," . "\"".$active_game_name{$room_id} . "\""."," . $active_room_player{$room_id}. ");\n"); 
    #print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," . "\"". "GAME" ."\"". "," . "\"".$active_game_name{$room_id} . "\"". "," . "\"".$active_room_player{$room_id}. "\"".");\n");
}

