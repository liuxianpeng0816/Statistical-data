#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

$| = 1;

my $savemessage_flag ="save sms with:";

while(<STDIN>)
{
	my $datarow = $_;
	chomp($datarow);

	#[2014-07-28 11:34:51.943][INFO][SmsController.php:142][SmsController::sendAction] save sms with:rPcnyLq3, {"fun":"mm","sms":"\u5e72\u561b\u5462","pn":"13311193236"}
	if($datarow =~ m/$savemessage_flag/)
	{	
		print($datarow."\n");
	}
}
