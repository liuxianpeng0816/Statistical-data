#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

$| = 1;



while(<STDIN>)
{
	my $datarow = $_;
	chomp($datarow);

	#[2014-07-28 11:34:51.943][INFO][SmsController.php:142][SmsController::sendAction] save sms with:rPcnyLq3, {"fun":"mm","sms":"\u5e72\u561b\u5462","pn":"13311193236"}

	my @ra = split(/\[|\]/, $datarow);
	my @rb = split(/:/, $ra[8]);
	my @rc = split(/,/, $rb[1]);

	print("savekey"."\t".$rc[0]."\n");


}
