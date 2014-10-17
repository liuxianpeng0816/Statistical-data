#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $curr_hour = $ENV{'INPUT_HOUR'};
my $lastr = "";
my $lastkey = "";
my $lastime = "";

my $t1 = "NULL";
my $t2 = "NULL";

my $mt = "NULL";
my $mid = "NULL";
my $size = 0;

my $in = 0;
my $out = 0;

my %chatmem;
my %set_num; 
my %total_spent; 
my %match; 
my %max_spent; 
my %flow;
my %match_section;
my %match_msgtype;
my %flow_msgtype;


while(<STDIN>)
{
    my $row = $_;
    chomp($row);
	
    my @r = split(/\t/, $row);
    my $array_len = @r;
    if($array_len != 6 && $array_len != 3)
    {
        print($row."\n");#print error line from mapper,that's "ER \t ..."
        next;
    }

	# 0                              1        2            3          4               5
	# senderid_sendersn_serviceid   date  log_source    action     msg_type          size
	
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
                    $flow{substr(&rstamp(int($t1/1000)), 0, 11)."0"} += $size; 	
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
                            my $time_part_increase = $time_part + 100;
                            if(($spent_time > $time_part)&&($spent_time <= $time_part_increase))
                            {
                                $match_part{$time_part_increase} += 1;
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
                            my $time_part_increase = $time_part + 100;
                            if(($spent_time > $time_part)&&($spent_time <= $time_part_increase))
                            {
                                $tmp_part->{$time_part_increase} += 1;
                            }

                        }
                    
                        if($spent_time > 3000)
                        {
                            $tmp_part->{3001} += 1;
                        }
                    }

                    unless (exists $match_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"})
                    {
                        my %match_msg;
                        my %flow_msg;
                        
                        $match_msg{$mt} += 1; 
                        $flow_msg{$mt} += $size; 

                        $match_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"} = \%match_msg;
                        $flow_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"} = \%flow_msg;
                    }
                    else
                    {
                        my $tmp_match_msg = $match_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"};
                        my $tmp_flow_msg = $flow_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"};
                        
                        $tmp_match_msg->{$mt} += 1; 
                        $tmp_flow_msg->{$mt} += $size; 

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
                    $flow{substr(&rstamp(int($t1/1000)), 0, 11)."0"} += $size; 	
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
        $mid = "NULL";
        $size = 0;
		
        $in = 0;
        $out = 0;
	}
	
    if(($r[2] eq "msgrouter")&&($r[3] eq "set"))
    {
        if($in < 1)   
        {    
            $in += 1;
        }

        $t1 = $r[1];
    }
    if(($r[2] eq "groupdb_msg_log")&&($r[3] eq "set"))
    {
        if($out <1)
        {
            $out += 1;
        }
        $t2 = $r[1];
    }
    if(($r[4] ne "null")&&($r[4] ne ""))
    {
        $mt = $r[4];
    }
	
    if(($r[2] eq "groupdb_msg_log")&&($r[5] ne "null")&&($r[5] ne "")&&($r[5] > 0))
    {
        $size = $r[5];
    }
    # inorder to get group active number of people
    if( $r[1] ne "groupdb_msg_log")
    {
        $lastr = $r[0];
    }
    else
    {
        if(($r[0] ne $lastkey)&&($lastkey ne ""))
        {
            $chatmem{$lastime} += 1;
        }
        $lastkey = $r[0];
        $lastime = substr(&rstamp(int($r[2]/1000)),0,11)."0";
    }
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
            $flow{substr(&rstamp(int($t1/1000)), 0, 11)."0"} += $size; 	
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
                    my $time_part_increase = $time_part + 100;
                    if(($spent_time > $time_part)&&($spent_time <= $time_part_increase))
                    {
                        $match_part{$time_part_increase} += 1;
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
                    my $time_part_increase = $time_part + 100;
                    if(($spent_time > $time_part)&&($spent_time <= $time_part_increase))
                    {
                        $tmp_part->{$time_part_increase} += 1;
                    }

                }
            
                if($spent_time > 3000)
                {
                    $tmp_part->{3001} += 1;
                }
            }

            unless (exists $match_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"})
            {
                my %match_msg;
                my %flow_msg;
                
                $match_msg{$mt} += 1; 
                $flow_msg{$mt} += $size; 

                $match_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"} = \%match_msg;
                $flow_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"} = \%flow_msg;
            }
            else
            {
                my $tmp_match_msg = $match_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"};
                my $tmp_flow_msg = $flow_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"};
                
                $tmp_match_msg->{$mt} += 1; 
                $tmp_flow_msg->{$mt} += $size; 

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
            $flow{substr(&rstamp(int($t1/1000)), 0, 11)."0"} += $size; 	
        }
        print("UN\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$size."\n");

    }
}
else
{
    print("ER\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$size."\tER2\n");
}
if($lastkey ne "")
{
    $chatmem{$lastime} += 1;
}

my %keytime_array;
$keytime_array{$curr_hour . "00"} = 1;
$keytime_array{$curr_hour . "10"} = 1;
$keytime_array{$curr_hour . "20"} = 1;
$keytime_array{$curr_hour . "30"} = 1;
$keytime_array{$curr_hour . "40"} = 1;
$keytime_array{$curr_hour . "50"} = 1;
my $time;
foreach $time(keys %keytime_array)
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
    if($chatmem{$time} == "")
    {
        $chatmem{$time} = 0;
    };
    if(substr($time, 0, 10) eq $curr_hour)
    {
        print("FI\t" . $time ."00" . "\tSS\t" . $chatmem{$time}."\t" . $set_num{$time} . "\t" . $total_spent{$time} . "\t" . $match{$time} . "\t" . $max_spent{$time} . "\t" . $flow{$time} . "\n");
    }
    elsif(substr($time, 0 ,10) lt $curr_hour)
    {
        print("INCR\t" . $time . "00" . "\tSS\t" . $chatmem{$time}."\t" . $set_num{$time} . "\t" . $total_spent{$time} . "\t" . $match{$time} . "\t" . $max_spent{$time} . "\t" . $flow{$time} . "\n");
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
            print("FI\t" . $time_hour . "000" . "\tSSR\t" . "NULL\t" . "NULL\t" . $time_parts . "\t" . $temp_part->{$time_parts} . "\tNULL\t". "NULL".  "\n");
        }
        elsif(substr($time_hour, 0 ,10) lt $curr_hour)
        {
            print("INCR\t" . $time_hour  . "000" . "\tSSR\t" . "NULL\t" . "NULL\t" . $time_parts . "\t" . $temp_part->{$time_parts} . "\tNULL\t". "NULL".  "\n");
        }
    }
}

my $time_hour2;
foreach $time_hour2(keys %match_msgtype)
{
    my $temp_match_msg = $match_msgtype{$time_hour2};
    my $temp_flow_msg = $flow_msgtype{$time_hour2};
    my $msg_types;
    foreach $msg_types(keys %$temp_match_msg)
    {
        if(substr($time_hour2, 0, 10) eq $curr_hour)
        {
            print("FI\t" . $time_hour2 . "\tSSF\t" . $msg_types . "\t" . "NULL\t" . "NULL\t" . $temp_match_msg->{$msg_types} . "\t" . "NULL\t" . $temp_flow_msg->{"$msg_types"} . "\n");
        }
        elsif(substr($time_hour2, 0 ,10) lt $curr_hour)
        {
            print("INCR\t" . $time_hour2 . "\tSSF\t" . $msg_types . "\t" . "NULL\t" . "NULL\t" . $temp_match_msg->{$msg_types} . "\t" . "NULL\t" . $temp_flow_msg->{"$msg_types"} . "\n");
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

