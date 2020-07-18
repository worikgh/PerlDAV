package WDGlobals;
use strict;
use warnings;
require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw |
    $ROOT
    $LOGLEVEL
    $RESOURCE_PATH_FN
    $DATADIR
    $DEAD_PROPERTIESDBFN
    $LIVE_PROPERTIES
    $LIVE_PROPERTIESDBFN
    $MAXSTORAGE
    $READLIMIT
    $PROTOCOL
    %LIVE_PROPERTIES
    _LOG
    _readSTDIN
    date_1123
|;
use Fcntl qw(:flock SEEK_END); # For logfile

our $ROOT = "ROOT";
# 1 => commands. 2 => some debug messages 3 => log all methods, 0 => just log excepions
our $LOGLEVEL = 3;
# The translation table for resources to file paths
our $RESOURCE_PATH_FN = "._tr";
# Where data is stored (Duh!)
our $DATADIR = 'DATA';


# The live properties.  Each key is the property name.  Each value is
# an ARRAY ref.  

#  Position 0 is PROTECTED: 1 => MUST, 0 => SHOULD/MAY and -1 => NOT.
#  See RFC4918 Sec 15: " A protected property is one that cannot be
#  changed with a PROPPATCH request.  There may be other requests that
#  would result in a change to a protected property (as when a LOCK
#  request affects the value of DAV:lockdiscovery).  Note that a given
#  property could be protected on one type of resource, but not
#  protected on another type of resource.

# Position 1 is COPY behaviour: 1 => recalculate 0 => preserve.

# Position 2 is MOVE behaviour: 1 => recalculate 0 => preserve.

# Position 3 is a function ref for calculating the property
our %LIVE_PROPERTIES = ();

# Database for properties (flat file)
our $LIVE_PROPERTIESDBFN = "._PropertiesLive";
our $DEAD_PROPERTIESDBFN = "._PropertiesDead";

our $MAXSTORAGE = 10000000; # 10MB  
our $READLIMIT = 1000000;  # The most to be read from STDIN in any one go
our $PROTOCOL = 'https'; # This is not in headers on nginx

# The file to write logs to
my $LOGFN = "webdav.log";

sub _LOG( ;$ ){
    my $message = shift;
    defined($message) or $message = "";
    open(my $fh, ">>", $LOGFN) or die "$!: '$LOGFN'";
    flock($fh, LOCK_EX) or die "$!: Cannot lock '$RESOURCE_PATH_FN'";
    my @caller = caller();
    my $caller = join(":", @caller);
    my $_msg = int(time())." ".scalar(localtime())." $$ $caller ".
        sprintf("%06d", $$)." $message\n";
    print $fh $_msg;
    close $fh or die "$!:$LOGFN";
}
sub date_1123  ( $ ){
    # Passed a unix timestamp and returns a time stamp a-la RFC1123
    # Sun, 06 Nov 1994 08:49:37 GMT
    my $time = shift;
    defined($time) or die; # 0 is a valid time stamp
    my @time = gmtime($time);

    # #  0    1    2     3     4    5     6     7     8
    # ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) 
    my @days = qw|Sun Mon Tue Wed Thu Fri Sat|;
    my @months = qw|Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec|;
    my $ret = sprintf("%s, %02d %s %d %02d:%02d:%02d GMT",
                      $days[$time[6]], $time[3], $months[$time[4]],
                      $time[5]+1900,
                      $time[2], $time[1], $time[0]);
    return $ret;
}
sub _readSTDIN( ;$ ){
    my $content = '';
    my $contentLength = shift;
    defined($contentLength) or
        $contentLength = $READLIMIT;
    # if($contentLength){
    my $_l = "";
    while(defined($_l) and length($content) < $contentLength){
        my $_buffer = '';
        my $len = $contentLength - length($content);
        my $read = read(STDIN, $_buffer, $len);
        if(defined($read)){
            if($read){
                # Read data
                $LOGLEVEL > 2&&length($content)&&_LOG("Reading content has taken more than ".
                                                   "one trip around the main loop in _readSTDIN");
                $content .= $_buffer;
            }else{
                # No data left
                last;
            }
        }else{
            # A system error
            die "500 Internal Server Error:'$!'";
        }
    }
    return $content;
}

1;
