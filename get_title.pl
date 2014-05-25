#! /usr/bin/perl
#@author : qiangrw 
#@created time: 2011-7-15
#@function: get title from url using threads
#		@input: url.txt format  [document id]\t[url1]\t[url]...\n
#		@output: out.txt

use strict;
use warnings;
use HTTP::Cookies;
use HTTP::Response;
use LWP::ConnCache;
use LWP::Simple;
use Encode;
use threads;
use threads::shared;
use Thread::Semaphore;
use FileHandle;
#use Lingua::Han::Utils;


my $usage = "[USAGE] perl $0 input_file output_file thread_no";
my $input = shift @ARGV or die $usage;
my $output = shift @ARGV or die $usage;
my $thread_no = shift @ARGV;
$thread_no = 1 unless defined $thread_no;

open OUT, ">>", $output;	# Output file
open FH, $input or die $!;			# the url file 
open ERROR,">>", "$output.err";

my $s = Thread::Semaphore->new(); 
my $es = Thread::Semaphore->new();
my $read_lock =  Thread::Semaphore->new();
OUT->autoflush(1);
ERROR->autoflush(1);

my $browser;
$browser = LWP::UserAgent->new( ) unless $browser;										#Create a new UserAgent
$browser->conn_cache(new LWP::ConnCache);												#Use HTTP KeepAlive
push @{$browser->requests_redirectable}, 'POST';										#Use POST Redirection
$browser->agent('Mozilla/5.0 (X11; U; Linux x86_64; en-US; rv:1.7.6)');					#Set proxy
$browser->timeout(3);																	#Set timeout
#$browser->cookie_jar(HTTP::Cookies->new('file' => "lwpcookies.txt", 'autosave' => 1 ));	#Set Cookies

print "Start Crawling URLS ... \n";
my $url_count:shared = 0;
my $succ_url_count:shared = 0;
my $line_no:shared = 0;

my $thread_num = 0;
my $MAX_THREADS = $thread_num;
my @threads_list;

if (-e "$output.log") {
    open FHL,"$output.log";
    while (<FHL>){
        next unless defined $_;
        $line_no = $_;
    }
    close FHL;
    foreach(1..$line_no){
        my $vacant = <FH>;
    }
}
print "START CRAWL URLS FROM LINE $line_no\n";


open LOG,">>$output.log" or die $!;
LOG->autoflush(1);
if($thread_num <= $MAX_THREADS){
    $threads_list[$thread_num] = threads->create(\&getTitle); 
    $thread_num ++;
    print "THREADS CREATE DONE\n";
}
if($thread_num > 0){
    foreach (0..$thread_num-1){
        $threads_list[$_]->join();
    }
}
print "DONE! Succ URL /Total URL :",$succ_url_count,"/",$url_count,"\n";
close FH;
close OUT;
close ERROR;
close LOG;

sub getTitle(){
    while(1){
        $read_lock->up();
        my $line = <FH>;
        next unless defined $line;
        chomp($line);
        $line_no++;
        print LOG "$line_no\n";
        $read_lock->down();

        last unless defined $line;

        my @elements = split /\s+/,$line;	#split the line
        my @output;							#save the output
        push @output,$elements[0];			#push the id into the output
        foreach my $i (1..$#elements) {
            next unless defined ($elements[$i]);
            my $url = $elements[$i];
            next unless ($url =~ m/:/);
            $url_count  ++;
            my $res = $browser->get($url);	#get the url
            unless ($res->is_success()) {
                $es->down();
                print ERROR "[ERROR]\t",$elements[0],"\t$elements[$i]\tSTATUS:",$res->status_line,"\n";
                $es->up();
                next; 						#try the next url
            }

            # change character set
            my $content_type = $res->header('Content-Type');
            next unless ($content_type =~ /text\/html|text\/plain/i);	#only process html
            my $charset = "utf-8";			#default to UTF-8
            if($content_type =~ /.*charset=([^\s;]+);?/i){
                $charset = $1;											#get the charset
            } else{
                #try to find in the head area
                my $page_content = $res->content;
                if($page_content =~ m{<meta[^>]*charset\s*=\s*([^\s/;]*)\s*;?\s*"\s*/>}is){
                    $charset = $1;
                }
            }

            #print "CHARSET:$charset ISUTF8:",utf8::is_utf8($res->header('Title'))," \n";
            my $title;
            if($charset =~ m/utf-8/i){
                eval{$title = $res->header('Title');};
                if($@) {
                    $es->down();
                    print ERROR "[CHARSET_ERROR:IDURL]\t",$elements[0],"\t$url\n";
                    $es->up();
                }
            } else{
                eval{$title = encode('UTF-8',decode("$charset",$res->header('Title')));};
                if($@) {
                    $es->down();
                    print ERROR "[CHARSET_ERROR:IDURL]\t",$elements[0],"\t$url\n";
                    $es->up();
                }
            }
            #my $title = Lingua::Han::Utils::cdecode($res->header('Title'));	
            next unless defined($title);								#must have title
            print "[SUCC]\t$line_no\t$url\nCHARSET:$charset\tTITLE:$title\n";
            push @output,$title;										#push the title
            $succ_url_count ++;
        }
        if($#output >= 1) {					#This item is useful
            $s->down();                		# P operation 
            foreach my $element(@output){
                print OUT "$element\t";
            }
            print OUT "\n";
            $s->up();                  		# V operation 
        }
    }
}
