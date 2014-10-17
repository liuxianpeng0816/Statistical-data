#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $curr_hour = $ENV{'INPUT_HOUR'};
#my $curr_hour = "2013122510";
my $lastr = "";

my $t1 = "NULL";
my $t2 = "NULL";
my $t3 = "NULL";

my $mt = "NULL";
my $size = 0;

my $in = 0;
my $out = 0;

my %fail_num; 
my %set_num; 
my %total_spent; 
my %match; 
my %max_spent; 
my %match_section;

while(<STDIN>)
{
    my $row = $_;
    chomp($row);
	
    my @r = split(/\t/, $row);
    
    my $array_len = @r;
    if($array_len != 6)
    {
        print($row."\n");#print error line from mapper,that's "ER \t ..."
        next;
    }
    # 0                                             1      2             3          4           5                   
    # senderphonenumber_sendersn_serviceid        date  log_type       action     msg_type     size
	
    if(($r[0] ne $lastr)&&($lastr ne ""))
    {
        if(($in <= 1)&&($out <= 1))
        {
            if(($t1 ne "NULL")&&($t2 ne "NULL"))
            {
                my $err = "";
                if(($t2 - $t1) < -20)
                {
                    print("ER\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$size."\tER1\n");
                }
                else
                {   
                    $set_num{substr(&rstamp(int($t1/1000)), 0, 11)."0"} += 1; 	
                    my $spent_time = $t2 - $t1;

                    if($spent_time < 0)
                    {
                        $spent_time = 0;
                    }

                    $total_spent{substr(&rstamp(int($t2/1000)), 0, 11)."0"} += $spent_time;
                    $match{substr(&rstamp(int($t2/1000)), 0, 11)."0"} += 1;

                    if( $spent_time > $max_spent{substr(&rstamp(int($t2/1000)), 0, 11)."0"})
                    {
                        $max_spent{substr(&rstamp(int($t2/1000)), 0, 11)."0"} = $spent_time;
                    }

                    unless (exists $match_section{substr(&rstamp(int($t2/1000)), 0, 11)})
                    {
                        my %match_part ;
                        
                        if($spent_time <= 100)
                        {
                            $match_part{100} += 1;
                        }
                    
                        my @time_array = (
                                        100,200,300,400,500,600,700,800,900,
                                        1000,1100,1200,1300,1400,1500,1600,1700,1800,1900,
                                        2000,2100,2200,2300,2400,2500,2600,2700,2800,2900
                                     );
                     
                        my $time_part; 
                        foreach $time_part (@time_array) 
                        {
                            my $time_part_ = $time_part + 100;
                            if(($spent_time > $time_part)&&($spent_time <= $time_part_))
                            {
                                $match_part{$time_part_} += 1;
                            }

                        }
                    
                        if($spent_time > 3000)
                        {
                            $match_part{3001} += 1;
                        }

                        $match_section{substr(&rstamp(int($t2/1000)), 0, 11)} = \%match_part;
                    }
                    else
                    {
                        my $tmp_part = $match_section{substr(&rstamp(int($t2/1000)), 0, 11)};
                        
                        if($spent_time <= 100)
                        {
                            $tmp_part->{100} += 1;
                        }
                    
                        my @time_array = (
                                        100,200,300,400,500,600,700,800,900,
                                        1000,1100,1200,1300,1400,1500,1600,1700,1800,1900,
                                        2000,2100,2200,2300,2400,2500,2600,2700,2800,2900
                                     );
                     
                        my $time_part; 
                        foreach $time_part (@time_array) 
                        {
                            my $time_part_ = $time_part + 100;
                            if(($spent_time > $time_part)&&($spent_time <= $time_part_))
                            {
                                $tmp_part->{$time_part_} += 1;
                            }

                        }
                    
                        if($spent_time > 3000)
                        {
                            $tmp_part->{3001} += 1;
                        }
                    }
                }
                if(substr(&rstamp(int($t2/1000)), 0, 10) gt $curr_hour)
                {
                    print("UN\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$size."\n");
                }

            }
            else
            {
                if($t1 ne "NULL")
                {
                    $set_num{substr(&rstamp(int($t1/1000)), 0, 11)."0"} += 1; 	
                }
                print("UN\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$size."\n");
            }
        }
        else
        {
            print("ER\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$size."\tER2\n");
        }

        $t1 = "NULL";
        $t2 = "NULL";
        $mt = "NULL";
        $size = 0;
		
        $in = 0;
        $out = 0;
    }
	
    if(($r[2] eq "msgrouter")&&($r[3] eq "set"))
    {
        $t1 = $r[1];
        $in += 1;
    }	
    if(($r[2] eq "groupserver")&&($r[3] eq "over"))
    {
        $t2 = $r[1];
        $out += 1;
    }
    if(($r[2] eq "groupserver")&&($r[3] eq "fail"))
    {
        $t3 = $r[1];
        $fail_num{substr(&rstamp(int($t3/1000)), 0, 11)."0"} += 1; 	
    }	
    if(($r[4] ne "null")&&($r[4] ne ""))
    {
        $mt = $r[4];
    }
	
    $size = 0;
	
    $lastr = $r[0];
}

if(($in <= 1)&&($out <= 1))
{
    if(($t1 ne "NULL")&&($t2 ne "NULL"))
    {
        my $err = "";
        if(($t2 - $t1) < -20)
        {
            print("ER\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$size."\tER1\n");
        }
        else
        {   
            $set_num{substr(&rstamp(int($t1/1000)), 0, 11)."0"} += 1; 	
            my $spent_time = $t2 - $t1;

            if($spent_time < 0)
            {
                $spent_time = 0;
            }

            $total_spent{substr(&rstamp(int($t2/1000)), 0, 11)."0"} += $spent_time;
            $match{substr(&rstamp(int($t2/1000)), 0, 11)."0"} += 1;

            if( $spent_time > $max_spent{substr(&rstamp(int($t2/1000)), 0, 11)."0"})
            {
                $max_spent{substr(&rstamp(int($t2/1000)), 0, 11)."0"} = $spent_time;
            }

            unless (exists $match_section{substr(&rstamp(int($t2/1000)), 0, 11)})
            {
                my %match_part ;
                
                if($spent_time <= 100)
                {
                    $match_part{100} += 1;
                }
            
                my @time_array = (
                                100,200,300,400,500,600,700,800,900,
                                1000,1100,1200,1300,1400,1500,1600,1700,1800,1900,
                                2000,2100,2200,2300,2400,2500,2600,2700,2800,2900
                             );
             
                my $time_part; 
                foreach $time_part (@time_array) 
                {
                    my $time_part_ = $time_part + 100;
                    if(($spent_time > $time_part)&&($spent_time <= $time_part_))
                    {
                        $match_part{$time_part_} += 1;
                    }

                }
            
                if($spent_time > 3000)
                {
                    $match_part{3001} += 1;
                }

                $match_section{substr(&rstamp(int($t2/1000)), 0, 11)} = \%match_part;
            }
            else
            {
                my $tmp_part = $match_section{substr(&rstamp(int($t2/1000)), 0, 11)};
                
                if($spent_time <= 100)
                {
                    $tmp_part->{100} += 1;
                }
            
                my @time_array = (
                                100,200,300,400,500,600,700,800,900,
                                1000,1100,1200,1300,1400,1500,1600,1700,1800,1900,
                                2000,2100,2200,2300,2400,2500,2600,2700,2800,2900
                             );
             
                my $time_part; 
                foreach $time_part (@time_array) 
                {
                    my $time_part_ = $time_part + 100;
                    if(($spent_time > $time_part)&&($spent_time <= $time_part_))
                    {
                        $tmp_part->{$time_part_} += 1;
                    }

                }
            
                if($spent_time > 3000)
                {
                    $tmp_part->{3001} += 1;
                }
            }
        }
        if(substr(&rstamp(int($t2/1000)), 0, 10) gt $curr_hour)
        {
            print("UN\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$size."\n");
        }

    }
    else
    {
        if($t1 ne "NULL")
        {
            $set_num{substr(&rstamp(int($t1/1000)), 0, 11)."0"} += 1; 	
        }
        print("UN\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$size."\n");
    }
}
else
{
    print("ER\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$size."\tER2\n");
}

my $time;
my %time_array;
$time_array{$curr_hour . "00"} = 1;
$time_array{$curr_hour . "10"} = 1;
$time_array{$curr_hour . "20"} = 1;
$time_array{$curr_hour . "30"} = 1;
$time_array{$curr_hour . "40"} = 1;
$time_array{$curr_hour . "50"} = 1;
foreach $time(keys %time_array)
{
    if($set_num{$time} == "")
    {
        $set_num{$time} = 0;
    };

    if($total_spent{$time} == "")
    {
        $total_spent{$time} = 0;
    };
    
    if($match{$time} == "")
    {
        $match{$time} = 0;
    };
    
    if($max_spent{$time} == "")
    {
        $max_spent{$time} = 0;
    };
 
    if(substr($time, 0, 10) eq $curr_hour)
    {
        print("FI\t" . $time . "00" . "\tSN\t" .$fail_num{$time}."\t". $set_num{$time} . "\t" . $total_spent{$time} . "\t" . $match{$time} . "\t" . $max_spent{$time} . "\tNULL\t"."\n");
    }
    elsif(substr($time, 0 ,10) lt $curr_hour)
    {
        print("INCR\t" . $time ."00". "\tSN\t" . $fail_num{$time}."\t".$set_num{$time} . "\t" . $total_spent{$time} . "\t" . $match{$time} . "\t" . $max_spent{$time} . "\tNULL\t"."\n");
    }
}


my $time_hour;
foreach $time_hour(keys %match_section)
{
    my $temp_part = $match_section{$time_hour};
    my $time_parts;
    foreach $time_parts(keys %$temp_part)
    {
        if(substr($time_hour, 0, 10) eq $curr_hour)
        {
            print("FI\t" . $time_hour ."000". "\tSNR\t" . "NULL\tNULL\t" . $time_parts . "\t" . $temp_part->{$time_parts} . "\tNULL\tNULL"."\n");
        }
        elsif(substr($time_hour, 0 ,10) lt $curr_hour)
        {
            print("INCR\t" . $time_hour ."000". "\tSNR\t" . "NULL\tNULL\t" . $time_parts . "\t" . $temp_part->{$time_parts} . "\tNULL\tNULL"."\n");
        }
    }
}


sub rstamp()
{
    my $t = shift;
	
    if($t =~ /^\d+$/)
    {
        return strftime("%Y%m%d%H%M%S",localtime($t));
    }
    else
    {
        return $t;
    }
}

