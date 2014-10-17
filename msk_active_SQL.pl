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
my %post_member;
my %post_count;
my %dialog_number;
my %dialog_member;
my %content_number;
my %content_member;
my %privatemessage_number;
my %privatemessage_member;
my $active_member;

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
    $date = $data_array[2];

    if($data_array[0] eq "CREATEWISH_MEM")
    {
	    $post_member{"CREATEWISH_MEM"} += $data_array[1];
    }
    elsif($data_array[0] eq "CREATESUBJECT_MEM")
    {
	    $post_member{"CREATESUBJECT_MEM"} += $data_array[1];
    }
    elsif($data_array[0] eq "CREATEWISH_NUM")
    {
	    $post_count{"CREATEWISH_NUM"} += $data_array[1];
    }
    elsif($data_array[0] eq "CREATESUBJECT_NUM")
    {
	    $post_count{"CREATESUBJECT_NUM"} += $data_array[1];
    }
    elsif($data_array[0] eq "TEXT_NUM")
    {
	    $content_number{"TEXT_NUM"} += $data_array[1];
    }
    elsif($data_array[0] eq "VOICE_NUM")
    {
	    $content_number{"VOICE_NUM"} += $data_array[1];
    }
    elsif($data_array[0] eq "IMAGE_NUM")
    {
	    $content_number{"IMAGE_NUM"} += $data_array[1];
    }
    elsif($data_array[0] eq "TEXT_MEM")
    {
	    $content_member{"TEXT_MEM"} += $data_array[1];
    }
    elsif($data_array[0] eq "VOICE_MEM")
    {
	    $content_member{"VOICE_MEM"} += $data_array[1];
    }
    elsif($data_array[0] eq "IMAGE_MEM")
    {
	    $content_member{"IMAGE_MEM"} += $data_array[1];
    }
    elsif($data_array[0] eq "ASSISTANT_MEM")
    {
	    $content_member{"ASSISTANT_MEM"} += $data_array[1];
    }
    elsif($data_array[0] eq "ASSISTANT_NUM")
    {
	    $content_number{"ASSISTANT_NUM"} += $data_array[1];
    }
    elsif($data_array[0] eq "DIALOG_MEM")
    {
	    $dialog_member{"DIALOG_MEM"} += $data_array[1];
    }
    elsif($data_array[0] eq "DIALOG_NUM")
    {
	    $dialog_number{"DIALOG_NUM"} += $data_array[1];
    }
    elsif($data_array[0] eq "ANONYMOUS_NUM")
    {
	    $privatemessage_number{"ANONYMOUS_NUM"} += $data_array[1];
    }
    elsif($data_array[0] eq "ANONYMOUS_MEM")
    {
	    $privatemessage_member{"ANONYMOUS_MEM"} += $data_array[1];
    }
    elsif($data_array[0] eq "WISH_UNMATCHED_NUM")
    {
	    $privatemessage_number{"WISH_UNMATCH_NUM"} += $data_array[1];
    }
    elsif($data_array[0] eq "WISH_UNMATCH_MEM")
    {
	    $privatemessage_member{"WISH_UNMATCH_MEM"} += $data_array[1];
    }
    elsif($data_array[0] eq "WISH_PUBLIC_NUM")
    {
	    $privatemessage_number{"WISH_PUBLIC_NUM"} += $data_array[1];
    }
    elsif($data_array[0] eq "WISH_PUBLIC_MEM")
    {
	    $privatemessage_member{"WISH_PUBLIC_MEM"} += $data_array[1];
    }
    elsif($data_array[0] eq "DYNAMIC_MESSAGE_NUM")
    {
	    $privatemessage_number{"DYNAMIC_NUM"} += $data_array[1];
    }
    elsif($data_array[0] eq "DYNAMIC_MEM")
    {
	    $privatemessage_member{"DYNAMIC_MEM"} += $data_array[1];
    }
    elsif($data_array[0] eq "ACTIVE")
    {
    	    $active_member += $data_array[1];
    }
}
foreach my $key(keys %post_member)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," ."\"$key\"". "," . "\"ALL\"" . "," . $post_member{$key}. ");\n"); 
}
foreach my $key(keys %post_count)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," ."\"$key\"" . "," . "\"ALL\"" . "," . $post_count{$key}. ");\n"); 
}
foreach my $key(keys %content_number)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," ."\"$key\"" . "," . "\"ALL\"" . "," . $content_number{$key}. ");\n"); 
}
foreach my $key(keys %content_member)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," ."\"$key\"" . "," . "\"ALL\"" . "," . $content_member{$key}. ");\n"); 
}
foreach my $key(keys %dialog_member)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," ."\"$key\"" . "," . "\"ALL\"" . "," . $dialog_member{$key}. ");\n"); 
}
foreach my $key(keys %dialog_number)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," ."\"$key\"" . "," . "\"ALL\"" . "," . $dialog_number{$key}. ");\n"); 
}

foreach my $key(keys %privatemessage_member)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," ."\"$key\"" . "," . "\"ALL\"" . "," . $privatemessage_member{$key}. ");\n"); 
}
foreach my $key(keys %privatemessage_number)
{
    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," ."\"$key\"" . "," . "\"ALL\"" . "," . $privatemessage_number{$key}. ");\n"); 
}

    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," . "\"ACTIVE\"" . "," . "\"USER\"" . "," . $active_member. ");\n"); 






