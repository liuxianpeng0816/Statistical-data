#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

$| = 1;

my $t1_file_name_pro = $ENV{'INPUT_TYPE1'};
my $t2_file_name_pro = $ENV{'INPUT_TYPE2'};
my $cur_path_file_name = $ENV{'map_input_file'};
#my $t1_file_name_pro = "qiaoqiao-session-log";
#my $t2_file_name_pro = "qiaoqiao-reg-log";
#my $cur_path_file_name = "qiaoqiao-reg-log";

my $row;
my $register_flag = "zrAction success";
my $function_op;

sub fun_type1_op
{
        #20140606155952|15184790411|1000034077|0|1|117.136.3.9|100|[]+[]|[]|[]|[]|2010
	my @r = split(/\|/, $row);
	if($r[2] ne "" && $r[3] eq "0")
	{
		print("QIAOQIAO-LOGIN"."|".$r[2]."\n");
	}
}
#2014-06-19 15:00:27	26765	info	1	zrAction success.	{"cmd":"a_zr","pn":"15205287682","jid":"1000108224","sid":"f4rbpe477ts6","pwd":"f6fccd5b47ca47972c4897f437ed9879","ostype":"android","pushtoken":"","clientversion":"2.1.120","appid":2010,"client_ip":"114.229.120.205","statistics":"ANDROID_HY\/2.1.120 (customerid=site,osver=2.1.120,platver=16,mod=GT-N7102,devicetype=user,os=4.1.2,GID=353867056561025,Accept-Language=zh)"}
sub fun_type2_op
{
	$row =~ s/\{|\}//g;
	my @r = split(/\t/, $row);
	my @devide = split(/,/, $r[5]);
	my %inf;

	for(my $a = 0; $a < @devide; $a ++)
	{
		$devide[$a] =~ s/\"//g;
		my @kv = split(/:/, $devide[$a]);
		$inf{$kv[0]} = $kv[1];
	}
	if($row =~ m/$register_flag/)
	{
		print("QIAOQIAO-REG"."|".$inf{"jid"}."\n");
	}
        my @statistics = split(/\(|\)/,$inf{"statistics"});
        my @channel = split(/,/,$statistics[1]);

        my @customerid = split(/\=/,$channel[0]);
        if($customerid[1] ne "")
        {
		print("CHANNEL"."|".$customerid[1]."\n");
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

