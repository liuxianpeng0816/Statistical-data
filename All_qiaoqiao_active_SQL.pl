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
my %post_count;
my %post_member;
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

    if( $data_array[0] eq "postmessage_member")
    {
	    $post_member{"POSTMESSAGE_MEM"} += $data_array[1];
    }
    elsif( $data_array[0] eq "browselist_member")
    {
	    $post_member{"BROWSELIST_MEM"} += $data_array[1];
    }
    elsif( $data_array[0] eq "browsedetail_member")
    {
	    $post_member{"BROWSEDETAIL_MEM"} += $data_array[1];
    }
    elsif( $data_array[0] eq "browsereply_member")
    {
	    $post_member{"BROWSEREPLY_MEM"} += $data_array[1];
    }
    elsif( $data_array[0] eq "likemessage_member")
    {
	    $post_member{"LIKEMESSAGE_MEM"} += $data_array[1];
    }
    elsif( $data_array[0] eq "likereply_member")
    {
	    $post_member{"LIKEREPLY_MEM"} += $data_array[1];
    }
    elsif( $data_array[0] eq "reply_member")
    {
	    $post_member{"REPLY_MEM"} += $data_array[1];
    }
    elsif( $data_array[0] eq "subscribe_member")
    {
	    $post_member{"SUBSCRIBE_MEM"} += $data_array[1];
    }
    elsif( $data_array[0] eq "feedback_member")
    {
	    $post_member{"FEEDBACK_MEM"} += $data_array[1];
    }
    elsif( $data_array[0] eq "report_member")
    {
	    $post_member{"REPORT_MEM"} += $data_array[1];
    }
    elsif( $data_array[0] eq "remove_member")
    {
	    $post_member{"REMOVE_MEM"} += $data_array[1];
    }

    if( $data_array[0] eq "postmessage")
    {
	    $post_member{"POSTMESSAGE"} += $data_array[1];
    }
    elsif( $data_array[0] eq "browselist")
    {
	    $post_member{"BROWSELIST"} += $data_array[1];

    }
    elsif( $data_array[0] eq "browsedetail")
    {
	    $post_member{"BROWSEDETAIL"} += $data_array[1];
    }
    elsif( $data_array[0] eq "browsereply")
    {
	    $post_member{"BROWSEREPLY"} += $data_array[1];
    }
    elsif( $data_array[0] eq "likemessage")
    {
	    $post_member{"LIKEMESSAGE"} += $data_array[1];
    }
    elsif( $data_array[0] eq "likereply")
    {
	    $post_member{"LIKEREPLY"} += $data_array[1];
    }
    elsif( $data_array[0] eq "reply")
    {
	    $post_member{"REPLY"} += $data_array[1];
    }
    elsif( $data_array[0] eq "subscribe")
    {
	    $post_member{"SUBSCRIBE"} += $data_array[1];
    }
    elsif( $data_array[0] eq "feedback")
    {
	    $post_member{"FEEDBACK"} += $data_array[1];
    }
    elsif( $data_array[0] eq "report")
    {
	    $post_member{"REPORT"} += $data_array[1];
    }
    elsif( $data_array[0] eq "remove")
    {
	    $post_member{"REMOVE"} += $data_array[1];
    }

    if( $data_array[0] eq "active")
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


    print("INSERT INTO " . $mysql_table . " (DATES,TYPES,DIME,VAL) VALUES (" . $date . "," . "\"ACTIVE\"" . "," . "\"USER\"" . "," . $active_member. ");\n"); 






