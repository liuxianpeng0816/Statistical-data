#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;


while(<STDIN>)
{
	my $row = $_;
	chomp($row);
	$row =~ s/\s//g;
		
	my @r = split(/\|/, $row,-1);
	
	# User Log Input
        #20140124152332|99990000003|4000000001|0|1|10.18.40.95|101|[]+[]|[]|[]|[]|3000	
	if((@r == 12) && $r[11] == 3000 && $r[3] == 0)
	{
	    if((substr($r[2],0,3) eq "350")&&(length($r[2]) == 10)||($r[2] =~/[^0-9]+/))
	    {
	        next;
	    }
	    if((substr($r[2],0,3) eq "400")&&(length($r[2]) == 10))
	    {
	        next;
	    }
		
	    # Log Output
		
	      print($r[2]."|".$r[0]."\t".$r[1]."|".$r[3]."\n");
	    # print($r[0]."\t".$r[2]."\t".$r[1]."|".$r[3]."\n");
	}
	if((@r == 12) && $r[3] == 2)
	{
	    if((substr($r[2],0,3) eq "350")&&(length($r[2]) == 10)||($r[2] =~/[^0-9]+/))
	    {
	        next;
	    }
	    if((substr($r[2],0,3) eq "400")&&(length($r[2]) == 10))
	    {
	        next;
	    }
		
	    # Log Output
		
	    print($r[2]."|".$r[0]."\t".$r[1]."|".$r[3]."\n");
	    #print($r[0]."\t".$r[2]."\t".$r[1]."|".$r[3]."\n");
	}

	
}

