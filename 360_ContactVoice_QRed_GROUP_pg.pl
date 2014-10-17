#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $curr_hour = $ENV{'INPUT_HOUR'};
#my $curr_hour = "2013112810";
my $lastr = "";

my $t1 = "NULL";
my $t2 = "NULL";

my $mt = "NULL";
my $mid = "NULL";
my $des = 0;

my $in = 0;
my $out = 0;

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

	
    if(($r[0] ne $lastr)&&($lastr ne ""))
    {
        if(($in <= 1)&&($out <= 1))
        {
            if(($t1 ne "NULL")&&($t2 ne "NULL"))
            {
                my $err = "";
                if(($t2 - $t1) < -20)
                {
                    print("ER\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$des."\tER1\n");
                }
                else
                {  
                    if($des > 0)
                    {
                        my $spent_time = int(($t2 - $t1)/$des);
                        if($spent_time < 0)
                        {
                            $spent_time = 0;
                        }
                        $match{substr(&rstamp(int($t2/1000)), 0, 11)."0"} += 1;
                        if($spent_time > $max_spent{substr(&rstamp(int($t2/1000)), 0, 11)."0"})
                        {
                            $max_spent{substr(&rstamp(int($t2/1000)), 0, 11)."0"} = $spent_time;
                        }

                        $total_spent{substr(&rstamp(int($t2/1000)), 0, 11)."0"} += $spent_time;

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

                }
                if(substr(&rstamp(int($t2/1000)), 0, 10) gt $curr_hour)
                {
                    print("UN\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$des."\n");
                }

            }
            else
            {
                print("UN\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$des."\n");
            }
        }
        else
        {
            print("ER\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$des."\tER2\n");
        }

        $t1 = "NULL";
        $t2 = "NULL";
        $mt = "NULL";
        $mid = "NULL";
        $des = 0;
		
        $in = 0;
        $out = 0;
    }
	
    if(($r[2] eq "msgrouter")&&($r[3] eq "prepare"))
    {
        $t1 = $r[1];
        $in += 1;
    }
		
    if(($r[2] eq "msgrouter")&&($r[3] eq "get"))
    {
        $t2 = $r[1];
        $out += 1;
    }

    if(($r[2] eq "groupdb_msg_log")&&($r[3] eq "get"))
    {
        $des += 1;
    }

		
    if(($r[4] ne "null")&&($r[4] ne ""))
    {
        $mt = $r[4];
    }
	
	
    $lastr = $r[0];
}

if(($in <= 1)&&($out <= 1))
{
    if(($t1 ne "NULL")&&($t2 ne "NULL"))
    {
        my $err = "";
        if(($t2 - $t1) < -20)
        {
            print("ER\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$des."\tER1\n");
        }
        else
        {  
            if($des > 0)
            {
                my $spent_time = ($t2 - $t1)/$des;
                if($spent_time < 0)
                {
                    $spent_time = 0;
                }
                $match{substr(&rstamp(int($t2/1000)), 0, 11)."0"} += 1;
                if($spent_time > $max_spent{substr(&rstamp(int($t2/1000)), 0, 11)."0"})
                {
                    $max_spent{substr(&rstamp(int($t2/1000)), 0, 11)."0"} = $spent_time;
                }

                $total_spent{substr(&rstamp(int($t2/1000)), 0, 11)."0"} += $spent_time;

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

        }
        if(substr(&rstamp(int($t2/1000)), 0, 10) gt $curr_hour)
        {
            print("UN\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$des."\n");
        }

    }
    else
    {
        print("UN\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$des."\n");
    }
}
else
{
    print("ER\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$des."\tER2\n");
}

my $time;
foreach $time(keys %match)
{
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
        print("FI\t" . $time ."00" . "\tPG\t" . "NULL\t" . "NULL" . "\t" . $total_spent{$time} . "\t" . $match{$time} . "\t" . $max_spent{$time} . "\t" . "NULL" . "\n");
    }
    elsif(substr($time, 0 ,10) lt $curr_hour)
    {
        print("INCR\t" . $time ."00" . "\tPG\t" . "NULL\t" . "NULL" . "\t" . $total_spent{$time} . "\t" . $match{$time} . "\t" . $max_spent{$time} . "\t" . "NULL" . "\n");
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
            print("FI\t" . $time_hour . "000" . "\tPGR\t" . "NULL\t" . "NULL\t" . $time_parts . "\t" . $temp_part->{$time_parts} . "\tNULL\t". "NULL".  "\n");
        }
        elsif(substr($time_hour, 0 ,10) lt $curr_hour)
        {
            print("INCR\t" . $time_hour  . "000" . "\tPGR\t" . "NULL\t" . "NULL\t" . $time_parts . "\t" . $temp_part->{$time_parts} . "\tNULL\t". "NULL".  "\n");
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

