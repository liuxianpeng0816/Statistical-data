#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

$| = 1;


#my $t1_file_name_pro = $ENV{'INPUT_TYPE1'};
#my $t2_file_name_pro = $ENV{'INPUT_TYPE2'};
#my $cur_path_file_name = $ENV{'map_input_file'};
my $t1_file_name_pro = "mmlogin";
my $t2_file_name_pro = "mmchat";
my $cur_path_file_name = "mmlogin";

my $row;
my $function_op;

my $msk_msg_login_flag ="open_session";
my $msk_login_client_version ="client_ver";
my $login_number = 0;
my %client_version;
# [info] ['500014739#2020#mobile':msgrouter_user:491] [2014-09-10 17:48:25] [username = '500014739#2020#mobile'] open_session, user_phone = <<"18611597957">>, SessionId = {user_session_data,<<"0837392766">>,<<"2499261687">>,1410342505,                       
#        2020,<<"mm_area1-msgrouter@10.117.88.71">>}, client_ip = "221.222.25.197", server_ip = "10.117.88.71", net_type = 3, client_ver = 102, ot = [], ov = [], mt = [], sr = [], ch = [], appid = 2020, c_p_vr = 106
#[info] ['500006827#2020#mobile':msgrouter_user:491] [2014-09-13 00:09:43] [username = '500006827#2020#mobile'] open_session, user_phone = <<"15713930336">>, SessionId = {user_session_data,<<"2280706473">>,<<"2670359708">>,1410538183,                      
#        2020,<<"mm_area1-msgrouter@10.117.88.72">>}, client_ip = "171.14.201.152", server_ip = "10.117.88.72", net_type = 3, client_ver = 102, ot = [], ov = [], mt = [], sr = [], ch = [], appid = 2020, c_p_vr = 106
sub fun_type1_op
{
	#<<"11141290359612019888">>,
	$row =~ s/s+//g; 
	#my @ra = split(/\<|\>/,$row);
	#print("tantanlogin"."|".$ra[2]."\n");
	print("PUSH"."\t".$row."\n");
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
