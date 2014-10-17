#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $curr_hour = $ENV{'INPUT_HOUR'};
#my $curr_hour = 2013121411;
my $lastr = "";

my $t1 = "NULL";
my $t2 = "NULL";

my $mt = "NULL";
my $size = 0;

my $in = 0;
my $out = 0;

my %set_num; 
my %launch; 
my %flow;
my %launch_msgtype;
my %flow_msgtype;


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

    # 0                                                                     1     2              3          4           5                   
    # senderid_sendersn_serviceid                                          date  log_type       action     msg_type    size
	
    if(($r[0] ne $lastr)&&($lastr ne ""))
    {
        if(($in <= 1)&&($out >= 1))
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
   
                    $set_num{substr(&rstamp(int($t2/1000)), 0, 11)."0"} += $out; 	
                    $flow{substr(&rstamp(int($t2/1000)), 0, 11)."0"} += $out*$size; 	
           
                    $launch{substr(&rstamp(int($t2/1000)), 0, 11)."0"} += $out;
                    
                    my $spent_time = $t2 - $t1;
                    if($spent_time < 0)
                    {
                        $spent_time = 0;
                    }

                    unless (exists $launch_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"})
                    {
                        my %launch_msg;
                        my %flow_msg;
                        
                        $launch_msg{$mt} += $out; 
                        $flow_msg{$mt} += $out*$size; 

                        $launch_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"} = \%launch_msg;
                        $flow_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"} = \%flow_msg;
                    }
                    else
                    {
                        my $tmp_launch_msg = $launch_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"};
                        my $tmp_flow_msg = $flow_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"};
                        
                        $tmp_launch_msg->{$mt} += $out; 
                        $tmp_flow_msg->{$mt} += $out*$size; 

                    }

                }
                if(substr(&rstamp(int($t2/1000)), 0, 10) gt $curr_hour)
                {
                    print("UN\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$size."\n");
                }

            }
            else
            {
                if($t2 ne "NULL")
                {
                    $launch{substr(&rstamp(int($t2/1000)), 0, 11)."0"} += $out;
                    $flow{substr(&rstamp(int($t2/1000)), 0, 11)."0"} += $out*$size; 	

                    unless (exists $launch_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"})
                    {
                        my %launch_msg;
                        my %flow_msg;
                        
                        $launch_msg{$mt} += $out; 
                        $flow_msg{$mt} += $out*$size; 

                        $launch_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"} = \%launch_msg;
                        $flow_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"} = \%flow_msg;
                    }
                    else
                    {
                        my $tmp_launch_msg = $launch_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"};
                        my $tmp_flow_msg = $flow_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"};
                        
                        $tmp_launch_msg->{$mt} += $out; 
                        $tmp_flow_msg->{$mt} += $out*$size; 

                    }
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
	
    if(($r[2] eq "groupdb_msg_log")&&($r[3] eq "set"))
    {
        $t1 = $r[1];
        $in += 1;
    }
		
	if(($r[2] eq "groupdb_msg_log")&&($r[3] eq "get"))
    {
        $t2 = $r[1];
        $out += 1;
    }
	
    if(($r[4] ne "null")&&($r[4] ne ""))
    {
        $mt = $r[4];
    }
	
    if(($r[5] ne "null")&&($r[5] ne "")&&($r[5] > 0))
    {
        $size = $r[5];
    }
		
    $lastr = $r[0];
}


if(($in <= 1)&&($out >= 1))
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
 
            $set_num{substr(&rstamp(int($t2/1000)), 0, 11)."0"} += $out; 	
            $launch{substr(&rstamp(int($t2/1000)), 0, 11)."0"} += $out;
            $flow{substr(&rstamp(int($t2/1000)), 0, 11)."0"} += $out*$size; 	
   
            my $spent_time = $t2 - $t1;
            if($spent_time < 0)
            {
                $spent_time = 0;
            }

            unless (exists $launch_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"})
            {
                my %launch_msg;
                my %flow_msg;
                
                $launch_msg{$mt} += $out; 
                $flow_msg{$mt} += $out*$size; 

                $launch_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"} = \%launch_msg;
                $flow_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"} = \%flow_msg;
            }
            else
            {
                my $tmp_launch_msg = $launch_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"};
                my $tmp_flow_msg = $flow_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"};
                
                $tmp_launch_msg->{$mt} += $out; 
                $tmp_flow_msg->{$mt} += $out*$size; 

            }

        }
        if(substr(&rstamp(int($t2/1000)), 0, 10) gt $curr_hour)
        {
            print("UN\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$size."\n");
        }

    }
    else
    {
        if($t2 ne "NULL")
        {
            $launch{substr(&rstamp(int($t2/1000)), 0, 11)."0"} += $out;
            $flow{substr(&rstamp(int($t2/1000)), 0, 11)."0"} += $out*$size; 	

            unless (exists $launch_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"})
            {
                my %launch_msg;
                my %flow_msg;
                
                $launch_msg{$mt} += $out; 
                $flow_msg{$mt} += $out*$size; 

                $launch_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"} = \%launch_msg;
                $flow_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"} = \%flow_msg;
            }
            else
            {
                my $tmp_launch_msg = $launch_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"};
                my $tmp_flow_msg = $flow_msgtype{substr(&rstamp(int($t2/1000)), 0, 11) . "000"};
                
                $tmp_launch_msg->{$mt} += $out; 
                $tmp_flow_msg->{$mt} += $out*$size; 

            }
        }
        print("UN\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$size."\n");
    }
}
else
{
    print("ER\t".$lastr."\t".&rstamp(int($t1/1000))."\t".$t1."\t".$t2."\t".&rstamp(int($t2/1000))."\t".$mt."\t".$size."\tER2\n");
}

my $time;
foreach $time(keys %launch)
{
    if($set_num{$time} == "")
    {
        $set_num{$time} = 0;
    };

    if($launch{$time} == "")
    {
        $launch{$time} = 0;
    };
    
    if(substr($time, 0, 10) eq $curr_hour)
    {
        print("FI\t" . $time . "00" . "\tSG\t" . "NULL\t" .$set_num{$time} . "\tNULL\t" . $launch{$time} . "\tNULL\t" . $flow{$time} . "\n");
    }
    elsif(substr($time, 0 ,10) lt $curr_hour)
    {
        print("INCR\t" . $time . "00" . "\tSG\t" . "NULL\t" .$set_num{$time} . "\tNULL\t" . $launch{$time} . "\tNULL\t" . $flow{$time} . "\n");
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
            print("FI\t" . $time_hour2 . "\tSGF\t" . $msg_types . "\tNULL\tNULL\t" . $temp_launch_msg->{$msg_types} . "\tNULL\t" . $temp_flow_msg->{$msg_types} . "\n");
        }
        elsif(substr($time_hour2, 0 ,10) lt $curr_hour)
        {
            print("INCR\t" . $time_hour2 . "\tSGF\t" . $msg_types . "\tNULL\tNULL\t" . $temp_launch_msg->{$msg_types} . "\tNULL\t" . $temp_flow_msg->{$msg_types} . "\n");
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

