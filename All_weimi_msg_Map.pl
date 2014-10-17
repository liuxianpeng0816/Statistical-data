#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

$| = 1;

my $t1_file_name_pro = $ENV{'INPUT_TYPE1'};
my $t2_file_name_pro = $ENV{'INPUT_TYPE2'};
my $cur_path_file_name = $ENV{'map_input_file'};
#my $t1_file_name_pro = "weimidistribute";
#my $t2_file_name_pro = "weimimsg";
#my $cur_path_file_name = "weimimsg";

my $row;
my $single_send_number = 0;
my $single_success_number = 0;
my $group_send_number = 0;
my $group_success_number = 0;
my $function_op;

my $weimi_distribute_flag1 = "http weimi users";
my $weimi_distribute_flag2 = "http weimi all";
my $weimi_msg_flag = "user_sup_group";
my $weimi_msg_single_flag = "single_abstract_group";
my $weimi_msg_login_flag1 ="open_session";
my $weimi_msg_login_flag2 ="username";
sub fun_type1_op
{
	#[info] [<0.10177.10>:distribute_maintain:290] [2014-08-14 10:36:17] http weimi users: Sender:<<"1">>, Receivers:[<<"660779">>], WeimiMsgId:68608319, ExpireTime:86400, Message:<<"{\"sender\":\"801960\",\"type\":0,\"message\":\"{\\\"senderid\\\":\\\"801960\\\",\\\"sendername\\\":\\\"\\\\u5f20\\\\u9759\\\",\\\"content\\\":\\\"{\\\\\\\"type\\\\\\\":1,\\\\\\\"content\\\\\\\":\\\\\\\"\\\\u7ed9\\\\u4f60\\\\u8bf4\\\\u5427\\\\u6e05\\\\u695a\\\\\\\"}\\\",\\\"tid\\\":\\\"6587251\\\",\\\"icon\\\":\\\"http:\\\\\\/\\\\\\/image.wemi.mobi\\\\\\/42d7610586c217c411ff337c185e2c66.jpg\\\",\\\"photo\\\":\\\"http:\\\\\\/\\\\\\/image.wemi.mobi\\\\\\/84947d7a0074cf6cd332f33c7026e21c.jpg\\\"}\",\"syncid\":616,\"receiver\":\"660779\",\"tid\":\"6587251\",\"status\":2000,\"addtime\":\"2014-08-14 10:36:17\",\"modtime\":\"2014-08-14 10:36:17\"}">>
	#[info] [<0.20074.11>:distribute_maintain:246] [2014-08-14 12:01:05] http weimi all: WeimiMsgId:21, ExpireTime:43137, Messagess:[<<"{\"sender\":100001,\"type\":4,\"message\":\"{\\\"senderid\\\":100001,\\\"sendername\\\":\\\"\\\",\\\"tid\\\":\\\"6589436\\\",\\\"icon\\\":\\\"http:\\\\\\/\\\\\\/image.wemi.mobi\\\\\\/f26a9df2674b7ba5cd3ab37356d92857.jpg\\\",\\\"photo\\\":\\\"http:\\\\\\/\\\\\\/image.wemi.mobi\\\\\\/73a599ad54060a1176a1fae5e6ac65a8.jpg\\\",\\\"title\\\":\\\"\\\\u5b58\\\\u94b1\\\\u4e70LV\\\\u88ab\\\\u7537\\\\u53cb\\\\u81ed\\\\u9a82\\\\uff0c\\\\u6211\\\\u6709\\\\u9519\\\\u5417\\\\uff1f\\\",\\\"ticket\\\":\\\"\\\\u5b58\\\\u94b1\\\\u4e70LV\\\\u88ab\\\\u7537\\\\u53cb\\\\u81ed\\\\u9a82\\\\uff0c\\\\u6211\\\\u6709\\\\u9519\\\\u5417\\\\uff1f\\\",\\\"content\\\":\\\"\\\\u5973\\\\u751f\\\\u5b58\\\\u94b1\\\\u4e70LV\\\\u5305\\\\u5305\\\\u5374\\\\u906d\\\\u5c0f\\\\u6c14\\\\u7537\\\\u53cb\\\\u8d23\\\\u9a82\\\\uff0c\\\\u6709\\\\u9519\\\\u5417\\\\uff1f\\\",\\\"expire\\\":1,\\\"expiretime\\\":\\\"2014-08-14 23:59:59\\\",\\\"ext\\\":[]}\",\"syncid\":-6589436,\"receiver\":\"\",\"tid\":6589436,\"status\":2000,\"addtime\":\"2014-08-14 12:01:03\",\"modtime\":\"2014-08-14 12:01:03\"}">>]
	
	if($row =~ m/$weimi_distribute_flag1/)
	{
                my @ra = split(/\[|\]/,$row);
                my $time = $ra[5];
                $time =~ s/\-|\s+|://g;

		my @rb = split(/,/, $row);
		my @devide = split(/:/, $rb[2]);
			
		$devide[0] =~ s/\s+//g;
		$devide[1] =~ s/\s+//g;
		print("single"."|".$devide[1]."\t"."distribute"."\t".&stamp($time)."\n");
		
	}
	elsif($row =~ m/$weimi_distribute_flag2/)
	{
                my @ra = split(/\[|\]/,$row);
                my $time = $ra[5];
                $time =~ s/\-|\s+|://g;

		my @rb = split(/,/, $row);
		my @devide = split(/:/, $rb[0]);
			
		$devide[5] =~ s/\s+//g;
		$devide[6] =~ s/\s+//g;
		print("group"."|".$devide[6]."\t"."distribute"."\t".&stamp($time)."\n");
		
	}
}
sub fun_type2_op
{
	#[info] [msgrouter_run_tool:msgrouter_run_tool:301] [2014-08-14 10:03:28] [user_sup_group = single_abstract_group] send sum = 1, send success = 1, send finished, message sn = 68499668, worker's name = msgrouter_run_tool!!!
        #[info] [<0.18756.10>:msgrouter_run_tool:301] [2014-08-13 19:04:18] [user_sup_group = msgrouter_user_sup12] send sum = 710, send success = 710, send finished, message sn = 21, worker's name = msgrouter_run_tool!!!
	#[info] ['1004516#2040#mobile-1408760986':msgrouter_user:478] [2014-08-23 10:29:46] [username = '1004516#2040#mobile-1408760986'] open_session, user_phone = <<"1004516">>, 
	#[info] ['1004523#2040#mobile-9BB8DE57DCC9E74AB05093FD0B26FD3C':msgrouter_user:478] [2014-08-23 10:09:22] [username = '1004523#2040#mobile-9BB8DE57DCC9E74AB05093FD0B26FD3C'] open_session, user_phone = <<"1004523">>, 	
        my @ra = split(/\[|\]/,$row);
    	my $time = $ra[5];
        $time =~ s/\-|\s+|://g;

	my @rb = split(/,/, $row);

	my @send = split(/=/, $rb[0]);
	my @success = split(/=/, $rb[1]);
	my @devide = split(/=/, $rb[3]);
		
        $send[2] =~ s/\s+//g;	
	$success[1] =~ s/\s+//g;
	$devide[1] =~ s/\s+//g;

	if($row =~ m/$weimi_msg_flag/)
	{

                if($row =~ m/$weimi_msg_single_flag/)
                {
                        $single_send_number += $send[2];
			$single_success_number += $success[1];
			print("single"."|".$devide[1]."\t"."msg"."\t".&stamp($time)."\n");
		}
                else
                {
			$group_send_number += $send[2];
                        $group_success_number += $success[1];
			print("group"."|".$devide[1]."\t"."msg"."\t".&stamp($time)."\n");
		}
		
	}
	elsif($row =~ m/$weimi_msg_login_flag1/ && $row =~ m/$weimi_msg_login_flag2/)
	{
		my @jid = split(/\#|\'/,$ra[3]);
		if($jid[1] ne "")
		{
			print("weimilogin"."|".$jid[1]."\n");
		}

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
while(<STDIN>)
{
        $row = $_;
	chomp($row);

        &{$function_op}();
	
}
print("MSGSINGLE"."\t".$single_send_number."\t".$single_success_number."\n");
print("MSGROUP"."\t".$group_send_number."\t".$group_success_number."\n");
