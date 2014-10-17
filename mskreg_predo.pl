#!/usr/bin/perl
use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

my $line="";

my $timestamp = "";

while(<STDIN>)
{
        my $row = $_;
        chomp($row);

        my $user_register_error_flag = "ERROR";
        my $user_register_man_flag = "user down register successful";
        my $user_register_stop_flag = "mb";
        if($row =~ m/$user_register_error_flag/)
        {
        }
        else
        {
                if($row =~ m/$user_register_man_flag/)
                {
                        if($line ne "")
                        {
                                print($line."\n");
                                $line = $row;
                        }
                }
                elsif($row =~ m/$user_register_stop_flag/)
                {
                        $line = $row;
                }
        }
}
