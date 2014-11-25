check-hdfs-stat
===============
check hdfs stat for nagios ex.
usage:check_hdfs_stat.rb [options] -w warnning -c critical,-h for help

    -d                               check hdfs disk urate
    -u                               check Under replicated blocks num
    -r                               check Blocks with corrupt replicas num
    -m                               check Missing blocks num
    -l                               check Live datanodes num
    -w, --warnning warn              Pass in warnning value
    -c, --critical crit              Pass in critical value
