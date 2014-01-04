dump-dbs
========

Small configurable utility to manage multiple database dumps. Useful to run as a nightly cron job in an environment with many databases. 

For Stanford production, this is currently driven from a cron job that
looks like this:

    # m h  dom mon dow   command
    0 8 * * * (echo; date; find /data/dump -maxdepth 1 -mtime +10 -exec rm -rv {} \;; date) 2>&1 >> /data/dump/clean.log
    0 9 * * * (echo; date; cd ~/src/dump-dbs; python dump_dbs.py; date) 2>&1 >> /data/dump/dump_dbs.log

The first command just cleans up any dumps more than ten days old.  The
second one calls the script and logs the output.

You end up with dumps that look like this

    sef@deploy dump> ls -l modulestore*
    -rw-rw-r-- 1 sef sef 9128725 Dec 24 09:00 modulestore-20131224.tar.gz
    -rw-rw-r-- 1 sef sef 9129554 Dec 25 09:00 modulestore-20131225.tar.gz
    -rw-rw-r-- 1 sef sef 9131314 Dec 26 09:00 modulestore-20131226.tar.gz
    -rw-rw-r-- 1 sef sef 9145001 Dec 27 09:00 modulestore-20131227.tar.gz
    -rw-rw-r-- 1 sef sef 9267190 Dec 28 09:00 modulestore-20131228.tar.gz
    -rw-rw-r-- 1 sef sef 9309751 Dec 29 09:00 modulestore-20131229.tar.gz
    -rw-rw-r-- 1 sef sef 9321564 Dec 30 09:00 modulestore-20131230.tar.gz
    -rw-rw-r-- 1 sef sef 9325699 Dec 31 09:00 modulestore-20131231.tar.gz
    -rw-rw-r-- 1 sef sef 9334733 Jan  1 09:00 modulestore-20140101.tar.gz
    -rw-rw-r-- 1 sef sef 9338620 Jan  2 09:00 modulestore-20140102.tar.gz
    -rw-rw-r-- 1 sef sef 9347997 Jan  3 09:00 modulestore-20140103.tar.gz
    -rw-rw-r-- 1 sef sef 9374341 Jan  4 09:00 modulestore-20140104.tar.gz
    lrwxrwxrwx 1 sef sef      27 Jan  4 09:00 modulestore-latest.tar.gz -> modulestore-20140104.tar.gz


