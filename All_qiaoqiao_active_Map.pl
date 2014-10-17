#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

$| = 1;

my $t1_file_name_pro = $ENV{'INPUT_TYPE1'};
my $t2_file_name_pro = $ENV{'INPUT_TYPE2'};
my $cur_path_file_name = $ENV{'map_input_file'};
#my $t1_file_name_pro = "qiaoqiao-active-log";
#my $t2_file_name_pro = "qiaoqiao-login-log";
#my $cur_path_file_name = "qiaoqiao-login-log";

my $row;
my %inf;
my $postmessage = 0;
my $browselist = 0;
my $browsedetail = 0;
my $browsereply = 0;
my $likemessage = 0;
my $likereply = 0;
my $reply = 0;
my $subscribe = 0;
my $feedback = 0;
my $report = 0;
my $remove = 0;

my $function_op;

sub fun_type1_op
{
	my @r = split(/\t/, $row);
	my @devide = split(/,/, $r[5]);

	for(my $a = 0; $a < @devide; $a ++)
	{
	    $devide[$a] =~ s/\"//g;
	    my @kv = split(/:/, $devide[$a]);
	    $inf{$kv[0]} = $kv[1];
	}
	for(my $a = 0; $a < @devide; $a ++)
	{
		$devide[$a] =~ s/\"//g;
		my @kv = split(/:/, $devide[$a]);
		$inf{$kv[0]} = $kv[1];
	}
	if ($r[4]=~ m/submitcontentAction success/)
	{
		print($inf{"jid"}."\t"."NULL"."\n");
		print("postmessage"."|".$inf{"jid"}."\n");
		$postmessage += 1;
	}
	elsif($r[4]=~ m/contentlistAction success/)	
	{
		print($inf{"jid"}."\t"."NULL"."\n");
		print("browselist"."|".$inf{"jid"}."\n");
		$browselist += 1;
	}
	elsif($r[4]=~ m/contentdetailAction success/)	
	{
		print($inf{"jid"}."\t"."NULL"."\n");
		print("browsedetail"."|".$inf{"jid"}."\n");
		$browsedetail += 1;
	}
	elsif($r[4]=~ m/contentreplyAction success/)	
	{
		print($inf{"jid"}."\t"."NULL"."\n");
		print("browsereply"."|".$inf{"jid"}."\n");
		$browsereply += 1;
	}
	elsif($r[4]=~ m/heartAction success/)	
	{
		print($inf{"jid"}."\t"."NULL"."\n");
		print("likemessage"."|".$inf{"jid"}."\n");
		$likemessage += 1;
	}
	elsif($r[4]=~ m/heartreplyAction success/)	
	{
		print($inf{"jid"}."\t"."NULL"."\n");
		print("likereply"."|".$inf{"jid"}."\n");
		$likereply += 1;
	}
	elsif($r[4]=~ m/replyAction success/)	
	{
		print($inf{"jid"}."\t"."NULL"."\n");
		print("reply"."|".$inf{"jid"}."\n");
		$reply += 1;
	}
	elsif($r[4]=~ m/subscribeAction success/)	
	{
		print($inf{"jid"}."\t"."NULL"."\n");
		print("subscribe"."|".$inf{"jid"}."\n");
		$subscribe += 1;
	}
	elsif($r[4]=~ m/fAction success/)	
	{
		print($inf{"jid"}."\t"."NULL"."\n");
		print("feedback"."|".$inf{"jid"}."\n");
		$feedback += 1;
	}
	elsif($r[4]=~ m/rpAction success/)	
	{
		print($inf{"jid"}."\t"."NULL"."\n");
		print("report"."|".$inf{"jid"}."\n");
		$report += 1;
	}
	elsif($r[4]=~ m/removeAction success/)	
	{
		print($inf{"jid"}."\t"."NULL"."\n");
		print("remove"."|".$inf{"jid"}."\n");
		$remove += 1;
	}
}

sub fun_type2_op
{

	my @ra = split(/\].*?</, $row);	

	my $i = $ra[1];
	my @l = split(/><?/, $i);
	for(my $a = 0; $a < @l; $a ++)
	{
		$l[$a] =~ s/\"//g;
		my @kv = split(/:/, $l[$a]);
		$inf{$kv[0]} = $kv[1];
	}
        if($inf{"action"} eq "set")
        {
        	print($inf{"sender_id"}."\t"."NULL"."\n");
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
        #2014-04-29 10:57:01	21037	info	1	contentreplyAction success	{"cmd":"browse_contentreply","jid":"1000000008","sid":"b6b68592fc768924612b406e8535db33","parentid":"1000000005_GAAAKzpeUwA=1","pageindex":"1"}

        $row = $_;
	chomp($row);

        &{$function_op}();
	
}
print("count"."\t".$postmessage."\t".$browselist."\t".$browsedetail."\t".$browsereply."\t".$likemessage."\t".$likereply."\t".$reply."\t".$subscribe."\t".$feedback."\t".$report."\t".$remove."\n");
