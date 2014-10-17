#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

$| = 1;

my $sharemessage_flag = "send sms success:";
my $viewmessage_flag ="view sms:";
my $sharenumber = 0;
my $date = "";



while(<STDIN>)
{
	my $datarow = $_;
	chomp($datarow);



        #[2014-06-26 08:36:19.110][INFO][sms.php:108][sms::send] send sms success:Array
	#[2014-07-28 11:28:18.383][INFO][SmsController.php:213][SmsController::viewAction] view sms: WT3urlMB, content: Array

	if($datarow =~ m/$sharemessage_flag/)
	{	
        	$sharenumber += 1;
	}
       	elsif($datarow =~ m/$viewmessage_flag/)
	{
		my @ra = split(/\[|\]/, $datarow);
                my @rb = split(/:/, $ra[8]);
                my @rc = split(/,/, $rb[1]);
		print("viewkey"."\t".$rc[0]."\n");
	
	}

}
print("sendmessage"."\t".$sharenumber."\n");
