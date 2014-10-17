#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

$| = 1;


my $reply_log_flag = "print_static_log";	
#[2014-08-05 18:28:10.735][INFO][ShareController.php:113][ShareController::print_static_log] <sender_jid:"500022840"><sender_pn:"18373273307"><receiver_jid:"500022802"><receiver_pn:"13170322807"><msg:"ÄÔô֪µÀҷ¢ÉÁÄÊ˭£¿"><conv_id:"11407163520712"><timestamp:"1407234490">
while(<STDIN>)
{
	my $row = $_;
	chomp($row);

	if($row =~ m/$reply_log_flag/) 
	{	my @ra = split(/\].*?</, $row);	
		my %inf;

		my $i = $ra[1];

		my @l = split(/> ?<?/, $i);
		for(my $a = 0; $a < @l; $a ++)
		{
			$l[$a] =~ s/\"//g;
			my @kv = split(/:/, $l[$a]);
			$inf{$kv[0]} = $kv[1];
		}

		print($inf{"sender_pn"}."\n");
	}
}
