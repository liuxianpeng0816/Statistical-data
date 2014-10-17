#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $current_date = $ENV{'CURRENT_DATE'};
#my $current_date ='20140706';
my $register_num = 0;
my $login_num = 0;
my $last_reg = "";
my $last_login = "";
my %channel_count;
my $channel = "";
while(<STDIN>)
{
    my $row = $_;
    chomp($row);
    $row =~ s/\s//g;
		
    my @r = split(/\|/, $row);
    if($r[0] eq "QIAOQIAO-REG")
    {
        if($r[1] ne $last_reg && $last_reg ne "")
        {
            $register_num += 1;
        }
        $last_reg = $r[1];
    }
    elsif($r[0] eq "QIAOQIAO-LOGIN")
    {
        if($r[1] ne $last_login && $last_login ne "")
        {
            $login_num += 1;
        }
        $last_login = $r[1];
    }
    elsif($r[0] eq "CHANNEL")
    {
        $channel = $r[1];
        $channel_count{$channel} += 1;
    }
    
}

if($last_reg ne "")
{
	$register_num += 1;
}
if($last_login ne "")
{
	$login_num += 1;
}
print("QIAOQIAO-REG"."\t".$current_date."\t".$register_num."\n");
print("QIAOQIAO-LOGIN"."\t".$current_date."\t".$login_num."\n");
foreach my $key(keys %channel_count)
{
        print($key."\t".$current_date."\t".$channel_count{$key}."\n");
}


