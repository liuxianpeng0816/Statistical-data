#!/usr/bin/perl

use strict;
use POSIX;
use Fcntl ':flock';
use Encode;

$| = 1;

#my $t1_file_name_pro = $ENV{'INPUT_TYPE1'};
#my $t2_file_name_pro = $ENV{'INPUT_TYPE2'};
#my $t3_file_name_pro = $ENV{'INPUT_TYPE3'};
#my $cur_path_file_name = $ENV{'map_input_file'};
my $t1_file_name_pro = "mmchat";
my $t2_file_name_pro = "xuyuanchat";
my $t3_file_name_pro = "mskdongtai";
my $cur_path_file_name = "mskdongtai";

my $row;
my %inf;
my $newdialog = 0;
my $createwish_number = 0;
my $createsubject_number = 0;
my $content_text = 0;
my $content_voice = 0;
my $content_image = 0;
my $content_assistant = 0;

my $create_flag = "do_createsubjectreq";
my $heart_flag = "do_heartsubjectreq";
my $comment_flag = "do_createcommentreq";

my $function_op;

my $newdialog_flag = "msg_content";
sub fun_type1_op
{
	#[info] ['500005364#2020#mobile':msgrouter_user:2027] <action:"set"><sender_id:"500005364"><sender_phonenumber:"18244915550"><receiver_id:"500005365"><service_id:"10000000"><sender_sn:"1403744078195"><msg_id:"null"><msg_type:"111000"><timestamp:"1403744225998"><size:"219"><app_id:"2020"><platform:"mobile"><msg_content:"8·就ä<conv_id:"18244915550|13066373115|1403744222290">
	my @ra = split(/\].*?</, $row);
	my @devide = split(/><?/, $ra[1]);

	for(my $a = 0; $a < @devide; $a ++)
	{
	    $devide[$a] =~ s/\"//g;
	    my @kv = split(/:/, $devide[$a]);
	    $inf{$kv[0]} = $kv[1];
	}
	if($inf{"action"} eq "set")
	{
		if($inf{"msg_type"} eq "111000"|$inf{"msg_type"} eq "0")
		{
			$content_text += 1;
			#print("SENDTEXT"."|".$inf{"sender_id"}."\n");
		}
		elsif($inf{"msg_type"} eq "111001"|$inf{"msg_type"} eq "1")
		{
			$content_voice += 1;
			#print("SENDVOICE"."|".$inf{"sender_id"}."\n");
		}
		elsif($inf{"msg_type"} eq "111002"|$inf{"msg_type"} eq "2")
		{
			$content_image += 1;
			#print("SENDIMAGE"."|".$inf{"sender_id"}."\n");
		}
		#新会话
	        if($row =~ m/$newdialog_flag/)
		{
			$newdialog += 1;
			#print("DIALOGMEM"."|".$inf{"sender_id"}."\n");
		}
                #给小助手发消息 else是活跃发消息人数，把给小助手发消息的过滤掉
		if($inf{"chat_type"} eq "2")
		{
			$content_assistant += 1;
			#print("SENDASSISTANT"."|".$inf{"sender_id"}."\n");
		}
                else
		{
			print($inf{"sender_id"}."\n");
		}
	}
}

sub fun_type2_op
{
        # [info] [<0.2524.0>:xuyuanchat_qlog:10] <thread:"null"><action:"getwish"><sender_id:"500000145"><sender_phonenumber:"null"><sender_sn:"null"><receiver_id:"null"><service_id:"10000009"><timestamp:"1403578707406">
        # 0.2524.0>:xuyuanchat_qlog:10
	my @ra = split(/\].*?</, $row);	

	my @devide = split(/><?/, $ra[2]);
	for(my $a = 0; $a < @devide; $a ++)
	{
		$devide[$a] =~ s/\"//g;
		my @kv = split(/:/, $devide[$a]);
		$inf{$kv[0]} = $kv[1];
	}
        if($inf{"action"} eq "createwish")
        {
                $createwish_number += 1;
        	#print("CREATEWISH"."|".$inf{"sender_id"}."\n");
        }
        elsif($inf{"action"} eq "getwish")
        {
        	#print("GETWISH"."|".$inf{"sender_id"}."\n");
        }
        elsif($inf{"action"} eq "createsubject")
        {
                $createsubject_number += 1;
        	#print("CREATESUBJECT"."|".$inf{"sender_id"}."\n");
        }
        elsif($inf{"action"} eq "getsubject")
        {
        	#print("GETSUBJECT"."|".$inf{"sender_id"}."\n");
        }
        if($inf{"action"} eq "createwish" || $inf{"action"} eq "getwish" || $inf{"action"} eq "creatsubject" || $inf{"action"} eq "getsubject")
        {
                print($inf{"sender_id"}."\n");
        }
}
sub fun_type3_op{

	#2014-07-03 15:56:39 651, do_createsubjectreq: addsubject: user:500000008, sid:gMkUtwy1UwAACQAA
	#2014-07-03 15:56:58 986, do_heartsubjectreq: likesubject: user:500000013, sid:gMkUtwy1UwAACQAA, like:1
	#2014-07-03 15:57:06 1075, do_createcommentreq: addcomment: user:500000013, cid:gMkU0gy1UwAACgAA, sid:gMkUtwy1UwAACQAA
	$row =~ s/\s//g;
	my @r = split(/,/, $row);
	my @devide = split(/\:/, $r[1]);
        my $sender_id = $devide[3];
	
	if($row =~ m/$create_flag/)
	{
		print($sender_id."\n");
	}
	elsif($row =~ m/$heart_flag/)
	{
		print($sender_id."\n");
	}
	elsif($row =~ m/$comment_flag/)
	{
		print($sender_id."\n");
	}

}
if ($cur_path_file_name =~ m/$t1_file_name_pro/)
{
    $function_op = \&fun_type1_op;
}
elsif($cur_path_file_name =~ m/$t2_file_name_pro/)
{
    $function_op = \&fun_type2_op;
}
elsif($cur_path_file_name =~ m/$t3_file_name_pro/)
{
    $function_op = \&fun_type3_op;
}
else
{    
    print STDERR "ER\treporter:counter:MapCounter,ErrorFileName,1\n";     
    exit(0);
}
while(<STDIN>)
{
        $row = $_;
	chomp($row);

        &{$function_op}();
	
}
#print("WISHSUBJECT"."|".$createwish_number."|".$createsubject_number."\n");
#print("CONTENTCOUNT"."|".$content_text."|".$content_voice."|".$content_image."|".$content_assistant."\n");
#print("DIALOGNUM"."|".$newdialog."\n");
