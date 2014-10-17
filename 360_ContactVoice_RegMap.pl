#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $now;

if((@ARGV == 1)&&(length($ARGV[0]) == 10))
{
	$now = substr($ARGV[0],0,10);
}
else
{
	print("Parameter Error! or Input yyyymmddhh! ...\n");
	exit(0);
}

my $pre = mktime(0,0,substr($now,8,2),substr($now,6,2),substr($now,4,2)-1,substr($now,0,4)-1900)-3600;
my $lasth = strftime "%Y%m%d%H",localtime($pre);

while(<STDIN>)
{
	my $row = $_;
	chomp($row);
	
	my @rl = split(/\|/, $row);
	
	# $sms: $ri{"imsi"}."|".$ri{"jid"}."|".$ri{"token"}."|".$ri{"pn"}."|".$r1[0].$r1[1]."|".$ri{"sms_net_ip"}
	if(@rl == 6)
	{
		if((substr($rl[4],0,10) ne $now)&&(substr($rl[4],0,10) ne $lasth))
		{
			next;
		}
		
		print($row."\n");
		next;
	}
	
	if(@rl == 5)
	{
		next;
	}
	
	$row =~ s/\\//g;
	
	# 2013-09-09 09:47:40     5144    info    1       sms_cbAction    success {"imsi":"460001128020475","jid":1000353,"token":"fc00667cf1b0780e6207fb62db62ad99","pn":"13801151162","sms_net_ip":"221.179.216.75"}
	# 2013-09-09 15:55:48     22100   info    1       getAction       success {"client_ip":"10.17.5.40","client_ver":"100","default_key":"b6d605cd620ff823c93cec20213fd115","im":"862067010369557","ot":"Android","ov":"4.0.3#15","mt":"generic/ZTE    U970","sr":"540*960","nt":"WIFI","sms_code":"10690909000360","imsi":"460026105836759","pwd":"846559b8b90749497e762b32e0c54354","jid":1000181,"pn":"18210684936","token":"11ffaca187f094bc758ddb35d1eac9af","appid":2000}
	# 
	# 2013-09-09 14:53:11     11858   info    1       rAction success {"imei":"352266053262375","os_type":"Android","os_ver":"2.3.6","mb_type":"Sony/st25i","screen_resolution":"640*480","channel":"1","net_type":"WIFI","client_ver":"100","pwd":"68d429367466525a7664b2466a7aa72a","pn":"13581642472","imsi":"460030991701051","token":"","client_ip":"10.18.71.17","jid":1000294,"appid":2000}
	
	# 2013-09-09 11:52:08     11883   error   5       getAction       get_user_info_by_imsi   failed  {"client_ip":"115.170.184.90","client_ver":"100","default_key":"b6d605cd620ff823c93cec20213fd115","im":"A1000032D672E8","ot":"Android","ov":"4.1.1#16","mt":"htccn_chs_ct/HTC     T528d","sr":"480*800","nt":"3G","sms_code":"10690909000360","imsi":"460001923102135"}
	# 2013-09-09 14:40:45     12547   error   5       getAction       verify_str      verify  failed  {"client_ip":"10.18.137.46","client_ver":"100","default_key":"b6d605cd620ff823c93cec20213fd115","im":"860381000537970","ot":"Android","ov":"2.3#10","mt":"htccn_chs_cmcc/HTC      A9188","sr":"480*800","nt":"WIFI","sms_code":"10690909000360","imsi":"460019652608015","decrypt_verify_str":null,"verify_str":"sEqzZ9UuLyEEstuhSLcZkLwX2UEu4fHUb17PDWzBX7UYkHI="}
	
	my @r = split(/\s/, $row);
	my $host = shift(@r);
	
	if(($r[6] eq "success")||($r[7] eq "failed")||($r[8] eq "failed"))
	{
		print($r[0]." ".$r[1]);
		
		for(my $i = 2; $i < @r; $i ++)
		{
			print("\t".$r[$i]);
		}
		
		print("\n");
	
		$r[0] =~ s/-//g;
		$r[1] =~ s/://g;
		
		my $info = "UNKNOW";
		if($r[6] eq "success")
		{
			$info = "INFO_".$r[5];
		}
		
		if($r[7] eq "failed")
		{
			$info = "ERROR_".$r[5]." ".$r[6];
		}
		
		if($r[8] eq "failed")
		{
			$info = "ERROR_".$r[5]." ".$r[6]." ".$r[7];
		}
		
		# 注册性能输出
		# Host	时间|INFO/ERROR_xxxx_xxxx|用时|错误代码
		print("REG_FUN|".$host."\t".$r[0].$r[1]."|".$info."|".$r[2]."|".$r[4]."\n");
	}
}

# Last version 2013-7-18 10:47
