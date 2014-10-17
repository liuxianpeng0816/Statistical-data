#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

$| = 1;

#my $t1_file_name_pro = $ENV{'INPUT_TYPE1'};
#my $t2_file_name_pro = $ENV{'INPUT_TYPE2'};
#my $cur_path_file_name = $ENV{'map_input_file'};
my $t1_file_name_pro = "weimidistribute";
my $t2_file_name_pro = "weimimsg";
my $cur_path_file_name = "weimidistribute";

my $row;
my $function_op;

my $weimi_distribute_flag = "http weimi users";

sub fun_type1_op
{
	#[info] [<0.10177.10>:distribute_maintain:290] [2014-08-14 10:36:17] http weimi users: Sender:<<"1">>, Receivers:[<<"660779">>], WeimiMsgId:68608319, ExpireTime:86400, Message:<<"{\"sender\":\"801960\",\"type\":0,\"message\":\"{\\\"senderid\\\":\\\"801960\\\",\\\"sendername\\\":\\\"\\\\u5f20\\\\u9759\\\",\\\"content\\\":\\\"{\\\\\\\"type\\\\\\\":1,\\\\\\\"content\\\\\\\":\\\\\\\"\\\\u7ed9\\\\u4f60\\\\u8bf4\\\\u5427\\\\u6e05\\\\u695a\\\\\\\"}\\\",\\\"tid\\\":\\\"6587251\\\",\\\"icon\\\":\\\"http:\\\\\\/\\\\\\/image.wemi.mobi\\\\\\/42d7610586c217c411ff337c185e2c66.jpg\\\",\\\"photo\\\":\\\"http:\\\\\\/\\\\\\/image.wemi.mobi\\\\\\/84947d7a0074cf6cd332f33c7026e21c.jpg\\\"}\",\"syncid\":616,\"receiver\":\"660779\",\"tid\":\"6587251\",\"status\":2000,\"addtime\":\"2014-08-14 10:36:17\",\"modtime\":\"2014-08-14 10:36:17\"}">>
	
	if($row =~ m/$weimi_distribute_flag/)
	{
                my @ra = split(/\[|\]/,$row);
                my $time = $ra[5];
                $time =~ s/\-|\s+|://g;

		my @rb = split(/,/, $row);
		my @devide = split(/:/, $rb[2]);
			
		$devide[0] =~ s/\s+//g;
		$devide[1] =~ s/\s+//g;

		print($devide[1]."\t"."distribute"."\t".&stamp($time)."\n");
	}
}

sub fun_type2_op
{
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
