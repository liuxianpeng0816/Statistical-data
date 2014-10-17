#! /usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $t1_file_name_pro = $ENV{'INPUT_TYPE1'};
my $t2_file_name_pro = $ENV{'INPUT_TYPE2'};
my $cur_path_file_name = $ENV{'map_input_file'};
#my $t1_file_name_pro = "qiaoqiao-reg-log";
#my $t2_file_name_pro = "qiaoqiao-active-log";
#my $cur_path_file_name = "qiaoqiao-active-log";

my $row;
my %inf;
my $register_flag = "zrAction success";

my $function_op;

sub fun_type1_op
{
	my @r = split(/\t/, $row);
        # 2014-06-07 15:58:55	395949	info	1	heartAction success.	{"cmd":"b_h","jid":"1000007009","sid":"645e06590ff624d13a6271e97b2bc565","cid":"1000007798_9XAAQHuSUwA1","iscancel":"0"}
	$r[0] =~s/\s|\-|\://g ;
	my $Date = substr($r[0],0,8);

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

	if($r[4] =~ m/submitcontentAction success/)
	{
		print($inf{"jid"}."_"."2\t".$Date."\n");
	}
	elsif($r[4] =~ m/contentlistAction success/)	
	{
		print($inf{"jid"}."_"."2\t".$Date."\n");
	}
	elsif($r[4] =~ m/contentdetailAction success/)	
	{
		print($inf{"jid"}."_"."2\t".$Date."\n");
	}
	elsif($r[4] =~ m/contentreplyAction success/)	
	{
		print($inf{"jid"}."_"."2\t".$Date."\n");
	}
	elsif($r[4] =~ m/heartAction success/)	
	{
		print($inf{"jid"}."_"."2\t".$Date."\n");
	}
	elsif($r[4] =~ m/heartreplyAction success/)	
	{
		print($inf{"jid"}."_"."2\t".$Date."\n");
	}
	elsif($r[4] =~ m/replyAction success/)	
	{
		print($inf{"jid"}."_"."2\t".$Date."\n");
	}
	elsif($r[4] =~ m/subscribeAction success/)	
	{
		print($inf{"jid"}."_"."2\t".$Date."\n");
	}
	elsif($r[4] =~ m/fAction success/)	
	{
		print($inf{"jid"}."_"."2\t".$Date."\n");
	}
	elsif($r[4] =~ m/rpAction success/)	
	{
		print($inf{"jid"}."_"."2\t".$Date."\n");
	}
	elsif($r[4] =~ m/removeAction success/)	
	{
		print($inf{"jid"}."_"."2\t".$Date."\n");
	}
}

sub fun_type2_op
{
	my @r = split(/\t/, $row);

        # Register Log Format
        # 2014-05-07 12:00:52	34651	info	1	zrAction success	{"cmd":"a_zr","pn":"18363096399","jid":"1000005670","sid":"4mml56l2gr5x","pwd":"adbf42c6e1c9ba049b23513912d213e6","ostype":"android","pushtoken":"","clientversion":"1.0.104","appid":2010}

	$r[0] =~s/\s|\-|\://g ;
	my $Date = substr($r[0],0,8);

	$r[5] =~ s/\{|\}//g;
	my @devide = split(/,/, $r[5]);
	my %inf;

	for(my $a = 0; $a < @devide; $a ++)
	{
		$devide[$a] =~ s/\"//g;
		my @kv = split(/:/, $devide[$a]);
		$inf{$kv[0]} = $kv[1];
	}
	if($r[4] =~ m/$register_flag/)
	{
		print($inf{"jid"}."_"."1\t".$Date."\n"); 
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
	$row = $_;
	chomp($row);

        &{$function_op}();
}

