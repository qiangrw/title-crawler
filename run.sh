#!sh
text=sample
threads=1
rm data/$text.output.log
perl get_title.pl data/$text.url data/$text.output $threads
