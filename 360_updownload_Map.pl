#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

$| = 1;

while(<STDIN>)
{
        #117.88.59      2013-12-19 18:35:29      1114636  info  1       upAction success {"jid":"1000026","password":"013cd7a5a1c0585f6f13c53ad43a4d37","file_type":"img","file_size":141812,"client_ver":"100","short_url":"http:\/\/filepipe.360.cn\/IubAjpwYYg6YmrBV"}
        #117.88.80      2013-12-19 21:54:40      15461      info  1       downloadAction new pic success         {"jid":"1000158","fileid":"t013d3efb39327356ac.jpg","short_code":"mpMOAw5TVTnHWFDP"}
        #               2013-12-23 13:11:17     84717   info    1       downloadAction new pic success  {"jid":"1000055","fileid":"t019c7b50e25c6627eb.png","short_code":"LFcozFzlJbuYAkpb","file_size":21489}
        #10.117.90.168	2014-01-08 20:15:10	6844	error	3	downloadAction get_data_by_code in mongo error	{"short_code":"6mgC0VDW6ojz6rdo"}
        my $row = $_;
	chomp($row);

	$row =~ s/\{|\}//g;
	my @r = split(/\t/, $row);
	my @devide = split(/,/, $r[6]);
	my %inf;
        
        $r[1] =~s/\s|\-|\://g ;
        my $date = substr($r[1],0,11) ."0";
	for(my $a = 0; $a < @devide; $a ++)
	{
	    $devide[$a] =~ s/\"//g;
	    my @kv = split(/:/, $devide[$a]);
	    $inf{$kv[0]} = $kv[1];
	}
	if ($r[5]=~ m/upAction success/)
        {
           print("upload"."|".$date."\t".$r[2]."\t".$inf{"file_size"}."\n");
	}
        elsif($r[5]=~ m/downloadAction new pic success/)	
	{
           print("download_success"."|".$date."\t".$r[2]."\t".$inf{"file_size"}."\n");
	}
        elsif($r[5]=~ m/downloadAction get_data_by_code in mongo error/)	
	{
            next;
	}
        elsif($r[5]=~ m/error/)	
	{
            print("download_error"."|".$date."\t".$r[2]."\t".$inf{"file_size"}."\n");
	}
	
	
}

