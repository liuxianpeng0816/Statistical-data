#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $curr_hour = $ENV{'INPUT_HOUR'};
#my $curr_hour = "2013122610";
my $lastr = "";
my $lastkey = "";
my $lastime = "";

my $t1 = "NULL";
my $t2 = "NULL";

my $mt = "NULL";
my $mid = "NULL";
my $des = 0;

my $in = 0;
my $out = 0;

my %chatmem;
my %set_num; 
my %total_span; 
my %launch; 
my %max_spent; 
my %flow;
my %launch_section;
my %launch_msgtype;
my %flow_msgtype;


while(<STDIN>)
{
    my $row = $_;
    chomp($row);
	
    my @r = split(/\t/, $row);
    
    my $array_len = @r;
    if($array_len != 7 && $array_len != 3)
    {
        print($row."\n");#print error line from mapper,that's "ER \t ..."
        next;
    }

    # 0                                                                     1     2              3          4           5                   6
    # (senderid/senderphonenumber_)sendersn/msgid_serviceid_receiverid      date  log_type       action     msg_type    msg_id/sender_sn    size
	
    if(($r[0] ne $lastr)&&($lastr ne ""))
    {    
        if(($in <= 1)&&($out <= 1))
        {
            if(($t1 ne "NULL")&&($t2 ne "NULL"))
            {
                my $err = "";
                if(($t2 - $t1) < -20)
                {
                    print("ER\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$mid."\t".$des."\tER1\n");
                }
                else
                {   
                    $set_num{substr(&rstamp(int($t1/1000)), 0, 11)."0"} += 1; 	
                    $flow{substr(&rstamp(int($t1/1000)), 0, 11)."0"} += $des; 	
                    my $spent_time = $t2 - $t1;

                    if($spent_time < 0)
                    {
                        $spent_time = 0;
                    }

                    $total_span{substr(&rstamp(int($t2/1000)), 0, 11)."0"} += $spent_time;
                    $launch{substr(&rstamp(int($t2/1000)), 0, 11)."0"} += 1;

                    if( $spent_time > $max_spent{substr(&rstamp(int($t2/1000)), 0, 11)."0"})
                    {
                        $max_spent{substr(&rstamp(int($t2/1000)), 0, 11)."0"} = $spent_time;
                    }
                    
                    unless (exists $launch_section{substr(&rstamp(int($t2/1000)), 0, 11)})
                    {
                        my %launch_part ;
                        
                        if($spent_time <= 100)
                        {
                            $launch_part{100} += 1;
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
                                $launch_part{$time_part_} += 1;
                            }

                        }
                    
                        if($spent_time > 3000)
                        {
                            $launch_part{3001} += 1;
                        }

                        $launch_section{substr(&rstamp(int($t2/1000)), 0, 11)} = \%launch_part;
                    }
                    else
                    {
                        my $tmp_part = $launch_section{substr(&rstamp(int($t2/1000)), 0, 11)};
                        
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

                    unless (exists $launch_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"})
                    {
                        my %launch_msg;
                        my %flow_msg;
                        
                        $launch_msg{$mt} += 1; 
                        $flow_msg{$mt} += $des; 

                        $launch_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"} = \%launch_msg;
                        $flow_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"} = \%flow_msg;
                    }
                    else
                    {
                        my $tmp_launch_msg = $launch_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"};
                        my $tmp_flow_msg = $flow_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"};
                        
                        $tmp_launch_msg->{$mt} += 1; 
                        $tmp_flow_msg->{$mt} += $des; 

                    }

                }
                if(substr(&rstamp(int($t2/1000)), 0, 10) gt $curr_hour)
                {
                    print("UN\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$mid."\t".$des."\n");
                }

            #	print("FI\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$mid."\t".$des.$err."\n");
            }
            else
            {
                if($t1 ne "NULL")
                {
                    $set_num{substr(&rstamp(int($t1/1000)), 0, 11)."0"} += 1; 	
                    $flow{substr(&rstamp(int($t1/1000)), 0, 11)."0"} += $des; 	
                }
                print("UN\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$mid."\t".$des."\n");
            }
        }
        else
        {
            print("ER\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$mid."\t".$des."\tER2\n");
        }

        $t1 = "NULL";
        $t2 = "NULL";
        $mt = "NULL";
        $mid = "NULL";
        $des = 0;
		
        $in = 0;
        $out = 0;
    }
	
    if(($r[2] eq "msgrouter")&&($r[3] eq "set"))
    {
        $t1 = $r[1];
        $in += 1;
    }
		
    if(($r[2] eq "PEER_MSG")&&($r[3] eq "set"))
    {
        $t2 = $r[1];
        $out += 1;
    }
	
    if(($r[6] ne "null")&&($r[6] ne "")&&($r[6] > 0))
    {
        $des = $r[6];
    }
		
    if(($r[4] ne "null")&&($r[4] ne ""))
    {
        $mt = $r[4];
    }
	
    if(($r[5] ne "null")&&($r[5] ne ""))
    {
        $mid = $r[5];
    }
    # inorder to get peer active number of people
    if( $r[1] ne "PEER_MSG")
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
            print("ER\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$mid."\t".$des."\tER1\n");
        }
        else
        {   
            $set_num{substr(&rstamp(int($t1/1000)), 0, 11)."0"} += 1; 	
            $flow{substr(&rstamp(int($t1/1000)), 0, 11)."0"} += $des; 	
            my $spent_time = $t2 - $t1;

            if($spent_time < 0)
            {
                $spent_time = 0;
            }

            $total_span{substr(&rstamp(int($t2/1000)), 0, 11)."0"} += $spent_time;
            $launch{substr(&rstamp(int($t2/1000)), 0, 11)."0"} += 1;

            if( $spent_time > $max_spent{substr(&rstamp(int($t2/1000)), 0, 11)."0"})
            {
                $max_spent{substr(&rstamp(int($t2/1000)), 0, 11)."0"} = $spent_time;
            }
            
            unless (exists $launch_section{substr(&rstamp(int($t2/1000)), 0, 11)})
            {
                my %launch_part ;
                
                if($spent_time <= 100)
                {
                    $launch_part{100} += 1;
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
                        $launch_part{$time_part_} += 1;
                    }

                }
            
                if($spent_time > 3000)
                {
                    $launch_part{3001} += 1;
                }

                $launch_section{substr(&rstamp(int($t2/1000)), 0, 11)} = \%launch_part;
            }
            else
            {
                my $tmp_part = $launch_section{substr(&rstamp(int($t2/1000)), 0, 11)};
                
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

            unless (exists $launch_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"})
            {
                my %launch_msg;
                my %flow_msg;
                
                $launch_msg{$mt} += 1; 
                $flow_msg{$mt} += $des; 

                $launch_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"} = \%launch_msg;
                $flow_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"} = \%flow_msg;
            }
            else
            {
                my $tmp_launch_msg = $launch_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"};
                my $tmp_flow_msg = $flow_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"};
                
                $tmp_launch_msg->{$mt} += 1; 
                $tmp_flow_msg->{$mt} += $des; 

            }

        }
        if(substr(&rstamp(int($t2/1000)), 0, 10) gt $curr_hour)
        {
            print("UN\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$mid."\t".$des."\n");
        }

    #	print("FI\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$mid."\t".$des.$err."\n");
    }
    else
    {
        if($t1 ne "NULL")
        {
            $set_num{substr(&rstamp(int($t1/1000)), 0, 11)."0"} += 1; 	
            $flow{substr(&rstamp(int($t1/1000)), 0, 11)."0"} += $des; 	
        }
        print("UN\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$mid."\t".$des."\n");
    }
}
else
{
    print("ER\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$mid."\t".$des."\tER2\n");
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

    if($total_span{$time} == "")
    {
        $total_span{$time} = 0;
    };
    
    if($launch{$time} == "")
    {
        $launch{$time} = 0;
    };
    
    if($max_spent{$time} == "")
    {
        $max_spent{$time} = 0;
    };
 
    if(substr($time, 0, 10) eq $curr_hour)
    {
        print("FI\t" . $time ."00" . "\tSS\t" . $chatmem{$time}."\t" . $set_num{$time} . "\t" . $total_span{$time} . "\t" . $launch{$time} . "\t" . $max_spent{$time} . "\t" . $flow{$time} . "\n");
    }
    elsif(substr($time, 0 ,10) lt $curr_hour)
    {
        print("INCR\t" . $time . "00" . "\tSS\t" .$chatmem{$time}."\t" . $set_num{$time} . "\t" . $total_span{$time} . "\t" . $launch{$time} . "\t" . $max_spent{$time} . "\t" . $flow{$time} . "\n");
    }
}

my $time_hour;
foreach $time_hour(keys %launch_section)
{
    my $temp_part = $launch_section{$time_hour};
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
foreach $time_hour2(keys %launch_msgtype)
{
    my $temp_launch_msg = $launch_msgtype{$time_hour2};
    my $temp_flow_msg = $flow_msgtype{$time_hour2};
    my $msg_types;
    foreach $msg_types(keys %$temp_launch_msg)
    {
        if(substr($time_hour2, 0, 10) eq $curr_hour)
        {
            print("FI\t" . $time_hour2 . "\tSSF\t" . $msg_types . "\t" . "NULL\t" . "NULL\t" . $temp_launch_msg->{$msg_types} . "\t" . "NULL\t" . $temp_flow_msg->{"$msg_types"} . "\n");
        }
        elsif(substr($time_hour2, 0 ,10) lt $curr_hour)
        {
            print("INCR\t" . $time_hour2 . "\tSSF\t" . $msg_types . "\t" . "NULL\t" . "NULL\t" . $temp_launch_msg->{$msg_types} . "\t" . "NULL\t" . $temp_flow_msg->{"$msg_types"} . "\n");
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

