#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $current_date = $ENV{'CURRENT_DATE'};
#my $current_date ='20131103';
my %net_type = ("0" => "UNKNOW", "1" => "2G", "2" => "3G", "3" => "WIFI");

my $net = "UNKNOW";
my $client_v = "UNKNOW";
my $model = "UNKNOW";
my $chan = "UNKNOW";
my $area = "UNKNOW";
#save every jid used versions ,because of upgrade
my %client_versions = ();
#save every jid used mobile ,because of change
my %mobiles = ();
#save every jid used chan ,because of change
my %chans = ();

my $active_usr_num = 0;
my %net_count;
my %version_usr_count;
my %model_usr_count;
my %chan_usr_count;
my %area_usr_count;

my $last_id = "";

while(<STDIN>)
{
    my $row = $_;
    chomp($row);
	
    my @r = split(/\t/, $row);
    my @r1 = split(/\|/, $r[1]);
	
    if($r[0] eq "ER")
    {
        print($row."\n");
    }
    if(($last_id ne $r[0])&&($last_id ne ""))
    {
        $active_usr_num ++;
        $area_usr_count{$area} += 1;
        foreach my $used_version(keys %client_versions)
        {
            $version_usr_count{$used_version} += 1;
            delete ($client_versions{$used_version});	
        }

        foreach my $used_chan(keys %chans)
        {
            $chan_usr_count{$used_chan} += 1;
            delete ($chans{$used_chan});	
        }
		
        foreach my $mobile(keys %mobiles)
        {
            $model_usr_count{$mobile} += 1;
            delete ($mobiles{$mobile});	
        }
		
        $net = "UNKNOW";
        $client_v = "UNKNOW";
        $model = "UNKNOW";
        $chan = "UNKNOW";
        $area = "UNKNOW";
		
    }
	
    # r0    r0[2]    r1[0]  r1[1]    r1[2]   r1[3]  r1[4]   r1[5]    r1[6] r1[7] r1[8] r1[9]
    # JID	手机号|操作类型|网络模式|IP|客户端版本|OS及版本|机型|分辨率|渠道|用户所在地区|日期
    #                      r0[0]    r0[2]   r1[0] r1[1]     r1[3]    r1[5]
    #用户登录/修改状态记录 JID_时间_2	    手机号|0|网络模式|IP|1.0.1|0
    #                      r0[0]    r0[2]   r1[0]  r1[1]    r1[3]  r1[4]  r1[5]
    #用户登出记录          JID_时间_9	UNKNOW|2|UNKNOW|UNKNOW|UNKNOW|0
    #                      r0[0]    r0[2]   r1[0]  r1[1]  r1[3]r1[4]r1[5]
    #用户Dump记录          JID_时间_x	手机号|2|NULL|NULL|NULL|0
			
    if(($r1[2] ne "UNKNOW")&&($r1[2] ne "NULL")&&($r1[2] ne "undefined")&&($r1[2] ne ""))
    {
        $net = $net_type{$r1[2]};
    }
				
    if(($r1[4] ne "UNKNOW")&&($r1[4] ne "NULL")&&($r1[4] ne "undefined")&&($r1[4] ne ""))
    {
        $client_v = $r1[4];
        $client_versions{$client_v} = 1;
    }
    else
    {
        $client_versions{$client_v} = 1;
    }
		
    if(($r1[6] ne "UNKNOW")&&($r1[6] ne "NULL")&&($r1[6] ne "undefined")&&($r1[6] ne ""))
    {
        $model = $r1[6];
        $mobiles{$model} = 1;
    }
    else
    {
        $mobiles{$model} = 1;
    }
	
    if(($r1[8] ne "UNKNOW")&&($r1[8] ne "NULL")&&($r1[8] ne "undefined")&&($r1[8] ne ""))
    {
        $chan = $r1[8];
        $chans{$chan} = 1;
    }
    else
    {
        $chans{$chan} = 1;
    }
	
    if(($r1[9] ne "UNKNOW")&&($r1[9] ne "NULL")&&($r1[9] ne "undefined")&&($r1[9] ne ""))
    {
        $area = $r1[9];
    }	
	
    $net_count{$net} += 1;	
    $last_id = $r[0];
    $last_id = $r[0];
}
if($last_id ne "")
{
    $active_usr_num ++;
    $area_usr_count{$area} += 1;
}
foreach my $used_version(keys %client_versions)
{
    $version_usr_count{$used_version} += 1;
    delete ($client_versions{$used_version});	
}
		
foreach my $mobile(keys %mobiles)
{
    $model_usr_count{$mobile} += 1;
    delete ($mobiles{$mobile});	
}		

foreach my $used_chan(keys %chans)
{
    $chan_usr_count{$used_chan} += 1;
    delete ($chans{$used_chan});	
}
	

print("ACTIVE\t".$current_date."\t".$active_usr_num."\n");

foreach my $network_type(keys %net_count)
{
    print("NETWORK\t".$current_date."\t".$network_type."\t".$net_count{$network_type}."\n");
}

foreach my $version_type(keys %version_usr_count)
{
    print("CLIENT\t".$current_date."\t".$version_type."\t".$version_usr_count{$version_type}."\n");
}

foreach my $model_type(keys %model_usr_count)
{
    print("MODEL\t".$current_date."\t".$model_type."\t".$model_usr_count{$model_type}."\n");
}

foreach my $chan_type(keys %chan_usr_count)
{
    print("CHAN\t".$current_date."\t".$chan_type."\t".$chan_usr_count{$chan_type}."\n");
}

foreach my $area_type(keys %area_usr_count)
{
    print("AREA\t".$current_date."\t".$area_type."\t".$area_usr_count{$area_type}."\n");
}
