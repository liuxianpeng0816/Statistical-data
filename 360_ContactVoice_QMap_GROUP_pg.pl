#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

$| = 1;

 my $t1_file_name_pro = $ENV{'INPUT_TYPE1'};
 my $t2_file_name_pro = $ENV{'INPUT_TYPE2'};
 my $cur_path_file_name = $ENV{'map_input_file'};
#my $t1_file_name_pro = "test-msgrouter-log";
#my $t2_file_name_pro = "test-groupdb-log";
#my $cur_path_file_name = "test-msgrouter-log";

my $pre_fun_op;
my $function_op;

sub pre_type1_op
{    
    my $datarow = shift;
    my $inf = shift;

    if(($inf->{"action"} ne "prepare")&&($inf->{"action"} ne "get"))
    {
        return 0;
    }

    if(($inf->{"service_id"} eq "")||($inf->{"service_id"} eq "null"))
    {
        print("ER\terr_source:service_id\t".$datarow . "\n");
        return 0;
    }
	
    if($inf->{"service_id"} ne "10000001")
    {
        return 0;
    }
    
    if(($inf->{"action"} eq "prepare")&&(($inf->{"sender_sn"} eq "")||($inf->{"sender_sn"} eq "null")))
    {
        print("ER\terr_source:sender_sn\t".$datarow ."\n");
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

sub pre_type2_op
{
    my $datarow = shift;
    my $inf = shift;

    if($inf->{"action"} ne "get")
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

    if(($inf->{"msg_id"} eq "")||($inf->{"msg_id"} eq "null"))
    {
        print("ER\terr_source:msg_id\t".$datarow . "\n");
        return 0;
    }
 
    if(($inf->{"msg_type"} eq "") || ($inf->{"msg_type"} eq "null"))
    {
        print("ER\terr_source:msg_type\t".$datarow . "\n");
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
    my $logsource = "msgrouter";

    $key = $inf->{"sender_sn"}."_".$inf->{"service_id"};	

    print($key."\t".$inf->{"timestamp"}."\t".$logsource."\t".$inf->{"action"}."\t".$inf->{"msg_type"}."\t".$inf->{"size"}."\n");
}

sub fun_type2_op
{
    my $inf = shift;
    my $key;
    my $logsource = "groupdb_msg_log";

    $key = $inf->{"sender_sn"}."_".$inf->{"service_id"};		
	
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
elsif($cur_path_file_name =~ m/$t2_file_name_pro/)
{
    $pre_fun_op = \&pre_type2_op;
    $function_op = \&fun_type2_op;
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
	
    #2013-11-25 21:02:42 [msgrouter] [PID=535268] [INFO ] [c_src/qlog_driver.cc:94] [info] ['1000010':msgrouter_user:1665] <action:"set"><sender_id:"1000010"><sender_phonenumber:"13671204812"><receiver_id:"null"><service_id:"10000001"><sender_sn:"1385367505922"><msg_id:"null"><msg_type:"302"><timestamp:"1385384562085"><size:"39">
	
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

