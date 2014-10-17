#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

$| = 1;

my $t1_file_name_pro = $ENV{'INPUT_TYPE1'};
my $cur_path_file_name = $ENV{'map_input_file'};

my $pre_fun_op;
my $function_op;

sub pre_type1_op
{
    my $datarow = shift;
    my $inf = shift;

    if($inf->{"action"} ne "set")
    {
        return 0;
    }

    if(($inf->{"sender_id"} eq "")||($inf->{"sender_id"} eq "null"))
    {
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
	   
    if(($inf->{"receiver_id"} eq "")||($inf->{"receiver_id"} eq "null"))
    {
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
	
    if(($inf->{"service_id"} eq "")||($inf->{"service_id"} eq "null"))
    {
        return 0;
    }
	
    if($inf->{"service_id"} ne "10000000")
    {
        return 0;
    }
    
    if(($inf->{"sender_sn"} eq "")||($inf->{"sender_sn"} eq "null"))
    {
        return 0;
    }

    if(($inf->{"msg_id"} eq "")||($inf->{"msg_id"} eq "null"))
    {
        return 0;
    }
 
    if(($inf->{"msg_type"} eq "") || ($inf->{"msg_type"} eq "null"))
    {
        return 0;
    }
	
    #if(($inf->{"msg_type"} ne "1"))
    #{
    #	return 0;
    #}

    if($inf->{"msg_type"} eq "1000")
    {
        return 0;
    }

    if(($inf->{"timestamp"} eq "")||($inf->{"timestamp"} eq "null"))
    {
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
    my $logsource = "PEER_MSG";
    $key = $inf->{"sender_id"};
		
    if( $inf->{"msg_type"} == 1)	
    {
        print("VOC"."|".$key."\n");
    }
    print("PEER"."|".$key."\n");
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
	
    # $r[0]      $r[1]    $r[2]          $ra[1]
    # 2013-09-22 00:17:22 [msgrouter]...<action:"set"><sender_id:"1000511"><sender_phonenumber:"18665705636"><receiver_id:"1000150"><service_id:"10000000"><sender_sn:"18"><msg_id:"null"><msg_type:"0">
    # 2013-09-22 14:25:35 [msgrouter]...<action:"notify"><sender_id:"null"><sender_phonenumber:"13681161855"><receiver_id:"1000174"><service_id:"10000000"><sender_sn:"1379813278105"><msg_id:"null"><msg_type:"null">
    # 2013-09-22 10:50:00 [PEER_MSG]    <action:"set"><sender_id:"1000495"><sender_phonenumber:"null"><receiver_id:"1000165"><service_id:"10000000"><sender_sn:"92"><msg_id:"56"><msg_type:"0">
    # 2013-09-22 10:50:11 [PEER_MSG]    <action:"get"><sender_id:"1000010"><sender_phonenumber:"null"><receiver_id:"1000181"><service_id:"10000000"><sender_sn:"null"><msg_id:"79"><msg_type:"1000">
	
    my @ra = split(/\].*?</, $datarow);	
    my %inf;
	
    my $i = $ra[1];
    my @l = split(/><?/, $i);
    
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
