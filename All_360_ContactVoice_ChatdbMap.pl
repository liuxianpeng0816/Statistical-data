#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

$| = 1;

my $t1_file_name_pro = $ENV{'INPUT_TYPE1'};
my $t2_file_name_pro = $ENV{'INPUT_TYPE2'};
my $cur_path_file_name = $ENV{'map_input_file'};
#my $t1_file_name_pro = "test-chatdb-log";
#my $t2_file_name_pro = "test-chatroom-log";
#my $cur_path_file_name = "test-chatdb-log";

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
    #if(($inf->{"sender_id"} eq "")||((substr($inf->{"sender_id"},0,3) eq "350")&&(length($inf->{"sender_id"}) == 10))||($inf->{"sender_id"} =~/[^0-9]+/))
    #{
    #    return 0;
    #}
    #if(($inf->{"sender_id"} eq "")||((substr($inf->{"sender_id"},0,3) eq "400")&&(length($inf->{"sender_id"}) == 10)))
    #{
    #    return 0;
    #}
    #if($inf->{"room_id"} eq "")
    #{
    #    return 0;
    #}
    #if($inf->{"service_id"} eq "")
    #{
    #	    return 0;
    #}
    #if($inf->{"sender_sn"} eq "")
    #{
    #    return 0;
    #}
    #if($inf->{"msg_id"} eq "")
    #{
    #    return 0;
    #}
    #if($inf->{"msg_type"} eq "")
    #{
    #    return 0;
    #}
    #if($inf->{"timestamp"} eq "")
    #{
    #    return 0;
    #}
  
    #if($inf->{"size"} eq "")
    #{
    #    return 0;
    #}
    return 1;

}

sub pre_type2_op
{
    my $datarow = shift;
    my $inf = shift;

    if($inf->{"action"} eq "set") 
    {
        return 0;
    }
    #if(($inf->{"sender_id"} eq "")||((substr($inf->{"sender_id"},0,3) eq "350")&&(length($inf->{"sender_id"}) == 10))||($inf->{"sender_id"} =~/[^0-9]+/))
    #{
    #    return 0;
    #}
    #if(($inf->{"sender_id"} eq "")||((substr($inf->{"sender_id"},0,3) eq "400")&&(length($inf->{"sender_id"}) == 10)))
    #{
    #    return 0;
    #}
    #if($inf->{"sender_phonenumber"} eq "")
    #{
    #    return 0;
    #}
    #if($inf->{"receiver_id"} eq "")
    #{
    #   return 0;
    #}
    #if($inf->{"room_id"} eq "")
    #{
    #    return 0;
    #}
    #if($inf->{"service_id"} eq "")
    #{
    #    return 0;
    #}
    #if($inf->{"sender_sn"} eq "")
    #{
    #    return 0;
    #}
    #if($inf->{"msg_type"} eq "")
    #{
    #    return 0;
    #}
    return 1;

}

sub fun_type1_op
{
    my $inf = shift;
    my $key;
    my $logsource = "chatdb";


    if(($inf->{"sender_id"} ne "")&&($inf->{"action"} eq "set")&&($inf->{"msg_type"} == 0||$inf->{"msg_type"} == 1||$inf->{"msg_type"} == 2))	
    {
	    print($inf->{"sender_id"}."\t"."NULL"."\t"."NULL"."\t"."chatdb"."\t".$inf->{"timestamp"}."\n");
	    print($inf->{"room_id"}."|".$inf->{"sender_id"}."\t"."chatdb"."\t".$inf->{"sender_id"}."|".$inf->{"sender_sn"}."\t".$inf->{"timestamp"}."\n");
	    print($inf->{"action"}."\t".$inf->{"msg_type"}."\t"."chatdb"."\t".$inf->{"sender_id"}."|".$inf->{"sender_sn"}."\t".$inf->{"size"}."\t".$inf->{"timestamp"}."\n");
	    print($inf->{"room_id"}."\t"."active_chatroom"."\t".$inf->{"timestamp"}."\n");
    }
}

sub fun_type2_op
{
    my $inf = shift;
    my $key;
    my $logsource = "chatroom";
    if($inf->{"msg_type"} == 102)
    {
	    print($inf->{"room_id"}."|".$inf->{"sender_id"}."\t".$inf->{"timestamp"}."\n");
    }

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
	my $row = $_;
	chomp($row);
	my @ra;
	my @l;

	@ra = split(/<action:|<act:/, $row);
	@l = split(/>?<|><?/, "action:".$ra[1]);
	my %inf;
	for(my $a = 0; $a < @l; $a ++)
	{
		$l[$a] =~ s/\"//g;
		my @kv = split(/:/, $l[$a]);
		$inf{$kv[0]} = $kv[1];
	}


	unless (&{$pre_fun_op}($row,\%inf))    
	{        
      		next;    
	}

	&{$function_op}(\%inf);

}
# Last Version  2014-3-10 17:44
