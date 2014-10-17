#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

$| = 1;


my $t1_file_name_pro = $ENV{'INPUT_TYPE1'};
my $t2_file_name_pro = $ENV{'INPUT_TYPE2'};
my $cur_path_file_name = $ENV{'map_input_file'};
#my $t1_file_name_pro = "mmlogin";
#my $t2_file_name_pro = "mmchat";
#my $cur_path_file_name = "mmlogin";

my $row;
my $function_op;

my $msk_msg_login_flag ="open_session";
my $login_number = 0;

sub fun_type1_op
{
	if($row =~ m/$msk_msg_login_flag/)
	{
		#[info] ['500033147#2020#mobile':msgrouter_user:479] [2014-08-24 10:38:51] [username = '500033147#2020#mobile'] open_session, user_phone = <<"18559813925">>, 
		my @ra = split(/\[|\]/,$row);
		my @rb = split(/\<|\>/,$ra[8]);
		my $phone = $rb[2];
        	$phone =~ s/\"//g;	
		$login_number += 1;
		

		print("mmlogin"."|".$phone."\n");

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
print("loginumber"."|".$login_number."\n");
