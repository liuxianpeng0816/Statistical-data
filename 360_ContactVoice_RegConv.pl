#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my %sms;

while(<STDIN>)
{
	my $row = $_;
	chomp($row);
	
	my @rl = split(/\|/, $row);
	
	# $sms{$ri{"imsi"}."|".$ri{"jid"}."|".$ri{"token"}."|".$ri{"pn"}} = $r1[0].$r1[1]."|".$ri{"sms_net_ip"}
	if(@rl == 6)
	{
		$sms{$rl[0]."|".$rl[1]."|".$rl[2]."|".$rl[3]} = $rl[4]."|".$rl[5];
		next;
	}
	
	if((@rl == 5)&&($rl[0] eq "REG_FUN"))
	{
		print($rl[1]."|".$rl[2]."|".$rl[3]."|".$rl[4]."\n");
		next;
	}
	
	# r1[0]      r1[1]        r1[2]   r1[3]   r1[4]   r1[5]           $r1[6]
	# 2013-09-09 09:47:40     5144    info    1       sms_cbAction    success {"imsi":"460001128020475","jid":1000353,"token":"fc00667cf1b0780e6207fb62db62ad99","pn":"13801151162","sms_net_ip":"221.179.216.75"}
	# 2013-09-09 15:55:48     22100   info    1       getAction       success {"client_ip":"10.17.5.40","client_ver":"100","default_key":"b6d605cd620ff823c93cec20213fd115","im":"862067010369557","ot":"Android","ov":"4.0.3#15","mt":"generic/ZTE    U970","sr":"540*960","nt":"WIFI","sms_code":"10690909000360","imsi":"460026105836759","pwd":"846559b8b90749497e762b32e0c54354","jid":1000181,"pn":"18210684936","token":"11ffaca187f094bc758ddb35d1eac9af","appid":2000}
	# 
	# 2013-09-09 14:53:11     11858   info    1       rAction success {"imei":"352266053262375","os_type":"Android","os_ver":"2.3.6","mb_type":"Sony/st25i","screen_resolution":"640*480","channel":"1","net_type":"WIFI","client_ver":"100","pwd":"68d429367466525a7664b2466a7aa72a","pn":"13581642472","imsi":"460030991701051","token":"","client_ip":"10.18.71.17","jid":1000294,"appid":2000}
	
	my @r1 = split(/\s/, $row);	
	my @r2 = split(/{|}/, $row);
	
	$r2[1] =~ s/\"//g;
	my %ri = split(/,|:/, $r2[1]);
	
	$r1[0] =~ s/-//g;
	$r1[1] =~ s/://g;
	
	if($r1[6] eq "success")
	{
		if($r1[5] eq "sms_cbAction")
		{
			$sms{$ri{"imsi"}."|".$ri{"jid"}."|".$ri{"token"}."|".$ri{"pn"}} = $r1[0].$r1[1]."|".$ri{"sms_net_ip"};
			#print($ri{"imsi"}."|".$ri{"jid"}."|".$ri{"token"}."|".$ri{"pn"}."\n");
		}
		
		# 2013-09-09 15:55:48     22100   info    1       getAction       success {"client_ip":"10.17.5.40","client_ver":"100","default_key":"b6d605cd620ff823c93cec20213fd115","im":"862067010369557","ot":"Android","ov":"4.0.3#15","mt":"generic/ZTE    U970","sr":"540*960","nt":"WIFI","sms_code":"10690909000360","imsi":"460026105836759","pwd":"846559b8b90749497e762b32e0c54354","jid":1000181,"pn":"18210684936","token":"11ffaca187f094bc758ddb35d1eac9af","appid":2000}
		if($r1[5] eq "getAction")
		{
			if(exists($sms{$ri{"imsi"}."|".$ri{"jid"}."|".$ri{"token"}."|".$ri{"pn"}}))
			{
				my @sm = split(/\|/, $sms{$ri{"imsi"}."|".$ri{"jid"}."|".$ri{"token"}."|".$ri{"pn"}});
				
				# 短信申请时间|客户端GET时间|手机号|JID|短信注册标识|注册成功标识|机型|GET网络模式|短信网关IP|客户端IP|系统类型|系统版本|客户端版本|分辨率|注册渠道
				print($sm[0]."|".$r1[0].$r1[1]."|".$ri{"pn"}."|".$ri{"jid"}."|1|1|".$ri{"mt"}."|".$ri{"nt"}."|".$sm[1]."|".$ri{"client_ip"}."|".$ri{"ot"}."|".$ri{"ov"}."|".$ri{"client_ver"}."|".$ri{"sr"}."|".($ri{"ch"}eq""?"UNKNOW":$ri{"ch"})."\n");
				delete($sms{$ri{"imsi"}."|".$ri{"jid"}."|".$ri{"token"}."|".$ri{"pn"}});
			}
			else
			{
				print("19700101080000|".$r1[0].$r1[1]."|".$ri{"pn"}."|".$ri{"jid"}."|1|1|".$ri{"mt"}."|".$ri{"nt"}."|UNKNOW|".$ri{"client_ip"}."|".$ri{"ot"}."|".$ri{"ov"}."|".$ri{"client_ver"}."|".$ri{"sr"}."|".($ri{"ch"}eq""?"UNKNOW":$ri{"ch"})."\n");
			}
		}
		
		# 2013-09-09 14:53:11     11858   info    1       rAction success {"imei":"352266053262375","os_type":"Android","os_ver":"2.3.6","mb_type":"Sony/st25i","screen_resolution":"640*480","channel":"1","net_type":"WIFI","client_ver":"100","pwd":"68d429367466525a7664b2466a7aa72a","pn":"13581642472","imsi":"460030991701051","token":"","client_ip":"10.18.71.17","jid":1000294,"appid":2000}
		if($r1[5] eq "rAction")
		{
			next;
			# 短信申请时间|客户端GET时间|手机号|JID|短信注册标识|注册成功标识|机型|GET网络模式|短信网关IP|客户端IP|系统类型|系统版本|客户端版本|分辨率|注册渠道
			print($r1[0].$r1[1]."|".$r1[0].$r1[1]."|".$ri{"pn"}."|".$ri{"jid"}."|0|1|".$ri{"mt"}."|".$ri{"nt"}."|UNKNOW|".$ri{"client_ip"}."|".$ri{"ot"}."|".$ri{"ov"}."|".$ri{"client_ver"}."|".$ri{"sr"}."|".($ri{"ch"}eq""?"UNKNOW":$ri{"ch"})."\n");
		}
	}
	
	# r1[0]      r1[1]      r1[2]   r1[3]   r1[4]   r1[5]   r1[6]                 r1[7]
	# 2013-05-31 00:00:45	7598	error	5	rAction get_user_info_by_imsi failed	{"client_ver":"100","default_key":"b6d605cd620ff823c93cec20213fd115","imsi":"460023101313736"}
	# 2013-05-31 00:00:45	7598	error	5	getAction get_user_info_by_imsi failed	{"client_ver":"100","default_key":"b6d605cd620ff823c93cec20213fd115","imsi":"460023101313736"}
	
	if(($r1[7] eq "failed")||($r1[8] eq "failed"))
	{
		my $sm = 1;		
		if($r1[5] eq "rAction")
		{
			$sm = 0;
		}
		
		if($ri{"imsi"} eq "")
		{
			$ri{"imsi"} = "UNKNOW";
		}
		
		if($ri{"im"} eq "")
		{
			$ri{"im"} = "UNKNOW";
		}
		
		# 短信申请时间|客户端GET时间|手机号|JID|短信注册标识|注册成功标识|机型|GET网络模式|短信网关IP|客户端IP|系统类型|系统版本|客户端版本|分辨率|注册渠道
		#print($r1[0].$r1[1]."|".$r1[0].$r1[1]."|IMSI:".$ri{"imsi"}."|IMSI:".$ri{"imsi"}."|".$sm."|0|UNKNOW|UNKNOW|UNKNOW|UNKNOW|UNKNOW|UNKNOW|".substr($ri{"client_ver"},0,1).".".substr($ri{"client_ver"},1,1).".".substr($ri{"client_ver"},2,1)."|UNKNOW|UNKNOW\n");
		print($r1[0].$r1[1]."|".$r1[0].$r1[1]."|IMSI:".$ri{"imsi"}."|IMEI:".$ri{"im"}."|".$sm."|0|".($ri{"mt"}eq""?"UNKNOW":$ri{"mt"})."|".($ri{"nt"}eq""?"UNKNOW":$ri{"nt"})."|UNKNOW|".($ri{"client_ip"}eq""?"UNKNOW":$ri{"client_ip"})."|".($ri{"ot"}eq""?"UNKNOW":$ri{"ot"})."|".($ri{"ov"}eq""?"UNKNOW":$ri{"ov"})."|".$ri{"client_ver"}."|".($ri{"sr"}eq""?"UNKNOW":$ri{"sr"})."|UNKNOW\n");
	}
}

for my $k (keys %sms)
{
	if($k ne "")
	{
		print($k."|".$sms{$k}."\n");
	}
}

# Last Version 2013-9-14 10:34
