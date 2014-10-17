#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

$| = 1;

my $t1_file_name_pro = $ENV{'INPUT_TYPE1'};
my $cur_path_file_name = $ENV{'map_input_file'};
my $curr_hour = $ENV{'INPUT_HOUR'};
#my $t1_file_name_pro = "test-groupdb-log";
#my $cur_path_file_name = "test-groupdb-log";
#my $curr_hour =2013121411 ;

my $pre_fun_op;
my $function_op;

sub pre_type1_op
{    
    my $datarow = shift;
    my $inf = shift;
    
    if(($inf->{"action"} ne "set")&&($inf->{"action"} ne "get"))
    {
        return 0;
    }
    
    if(($inf->{"action"} eq "get")&&(substr(&rstamp(int($inf->{"timestamp"}/1000)),0,10) ne $curr_hour))
    {
        return 0;
    }

    if(($inf->{"sender_id"} eq "")||($inf->{"sender_id"} eq "null"))
    {
        print("ER\terr_source:sender_id\t".$datarow . "\n");
        return 0;
    }
 
    if((substr($inf->{"sender_id"},0,3) eq "350")&&(length($inf->{"sender_id"}) == 10)||($inf->{"sender_id"} =~/[^0-9]+/))
    {
        return 0;
    }
    if((substr($inf->{"sender_id"},0,3) eq "400")&&(length($inf->{"sender_id"}) == 10))
    {
        return 0;
    }
	
    if(($inf->{"action"} eq "get")&&(($inf->{"receiver_id"} eq "")||($inf->{"receiver_id"} eq "null")))
    {
        print("ER\terr_source:recevier_id\t".$datarow . "\n");
        return 0;
    }

    if((substr($inf->{"receiver_id"},0,3) eq "350")&&(length($inf->{"receiver_id"}) == 10))
    {
        return 0;
    }
    if((substr($inf->{"receiver_id"},0,3) eq "400")&&(length($inf->{"receiver_id"}) == 10))
    {
        return 0;
    }
	
    if(($inf->{"group_id"} eq "")||($inf->{"group_id"} eq "null"))
    {
        print("ER\terr_source:group_id\t".$datarow . "\n");
        return 0;
    }   	
    	
    if(($inf->{"service_id"} eq "")||($inf->{"service_id"} eq "null"))
    {
        print("ER\terr_source:service_id\t".$datarow . "\n");
        return 0;
    }
	
    if($inf->{"service_id"} ne "10000001")
    {
        print("ER\terr_source:service_id\t".$datarow ."\n");
        return 0;
    }
    
    if(($inf->{"sender_sn"} eq "")||($inf->{"sender_sn"} eq "null"))
    {
        print("ER\terr_source:sender_sn\t".$datarow ."\n");
        return 0;
    }
    
    if(($inf->{"msg_id"} eq "") || ($inf->{"msg_id"} eq "null"))
    {
        print("ER\terr_source:msg_id\t".$datarow ."\n");
        return 0;
    }

    if(($inf->{"msg_type"} eq "") || ($inf->{"msg_type"} eq "null"))
    {
        print("ER\terr_source:msg_type\t".$datarow ."\n");
        return 0;
    }

    if($inf->{"msg_type"} eq "1000")
    {
        return 0;
    }

    if(($inf->{"timestamp"} eq "")||($inf->{"timestamp"} eq "null"))
    {
        print("ER\terr_source:timestamp\t".$datarow . "\n");
        return 0;
    }
		
    if(($inf->{"size"} eq "")||($inf->{"size"} eq "null"))
    {
        $inf->{"size"} = 0;
    }
	
    return 1;
}

sub fun_type1_op
{
    my $inf = shift;
    my $key;
    my $logsource = "groupdb_msg_log";

    $key = $inf->{"sender_id"}."_".$inf->{"group_id"}."_".$inf->{"msg_id"}."_".$inf->{"service_id"};		

    print($key."\t".$inf->{"timestamp"}."\t".$logsource."\t".$inf->{"action"}."\t".$inf->{"msg_type"}."\t".$inf->{"size"}."\n");
}

sub stamp()
{
    my $t = shift;
    return mktime(substr($t,12,2),substr($t,10,2),substr($t,8,2),substr($t,6,2),substr($t,4,2)-1,substr($t,0,4)-1900);
}

sub rstamp()
{
    my $t = shift;
    return strftime("%Y%m%d%H%M%S",localtime($t));
}

if ($cur_path_file_name =~ m/$t1_file_name_pro/)
{
    $pre_fun_op = \&pre_type1_op;
    $function_op = \&fun_type1_op;
}
else
{    
    print STDERR "ER\treporter:counter:MapCounter,ErrorFileName,1\n";     
    exit(0);
}

while(<STDIN>)
{
    my $datarow = $_;
    chomp($datarow);
	
    #2013-11-25 21:09:06 [groupdb_msg_log] thread:140079736989440 <action:"set"><sender_id:"1000010"><sender_phonenumber:"null"><receiver_id:"null"><group_id:"103707181000004"><service_id:"10000001"><sender_sn:"1385367505945"><msg_id:"580"><msg_type:"300"><timestamp:"1385384946063"><size:"72">
	
    my @ra = split(/\].*?</, $datarow);	
    my %inf;
	
    my $i = $ra[1];
    my @l = split(/> ?<?/, $i);
    
    for(my $a = 0; $a < @l; $a ++)
    {
        $l[$a] =~ s/\"//g;
        my @kv = split(/:/, $l[$a]);
        $inf{$kv[0]} = $kv[1];
    }

    unless (&{$pre_fun_op}($datarow,\%inf))    
    {        
        next;    
    }
    
    &{$function_op}(\%inf);
}

