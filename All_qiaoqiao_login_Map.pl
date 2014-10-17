#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

$| = 1;

my $t1_file_name_pro = $ENV{'INPUT_TYPE1'};
my $t2_file_name_pro = $ENV{'INPUT_TYPE2'};
my $cur_path_file_name = $ENV{'map_input_file'};
#my $t1_file_name_pro = "qiaoqiao-login-log";
#my $t2_file_name_pro = "qiaoqiao-getmessage-log";
#my $cur_path_file_name = "qiaoqiao-getmessage-log";

my $count_sendmsg = 0;
my $count_recvmsg = 0;

my $function_op;

sub fun_type1_op
{
        my $inf = shift;
	if($inf->{"action"} eq "set")
	{
		$count_sendmsg += 1;
		print("send|".$inf->{"sender_id"}."\n");
	}
}

sub fun_type2_op
{

        my $inf = shift;
	if($inf->{"action"} eq "get" && $inf->{"service_id"} eq "10000000")
	{
		$count_recvmsg += 1;
		print("get|".$inf->{"receiver_id"}."\n");
	}

}
if ($cur_path_file_name =~ m/$t1_file_name_pro/)
{
    $function_op = \&fun_type1_op;
}
elsif($cur_path_file_name =~ m/$t2_file_name_pro/)
{
    $function_op = \&fun_type2_op;
}
else
{    
    print STDERR "ER\treporter:counter:MapCounter,ErrorFileName,1\n";     
    exit(0);
}
while(<STDIN>)
{
	#[info] ['1000025304#2010#mobile':msgrouter_user:1784] <action:"set"><sender_id:"1000025304"><sender_phonenumber:"15027375761"><receiver_id:"1000046950"><service_id:"10000000"><sender_sn:"1401876813615"><msg_id:"null"><msg_type:"0"><timestamp:"1401876950881"><size:"92"><app_id:"2010"><platform:"mobile">

	my $row = $_;
	chomp($row);

	my @ra = split(/\].*?</, $row);	
	my %inf;

	my $i = $ra[1];
	my @l = split(/><?/, $i);

	for(my $a = 0; $a < @l; $a ++)
	{
		$l[$a] =~ s/\"//g;
		my @kv = split(/:/, $l[$a]);
		$inf{$kv[0]} = $kv[1];
	}

	&{$function_op}(\%inf);


}
print("MessageCount"."\t".$count_sendmsg."\t".$count_recvmsg."\n");
