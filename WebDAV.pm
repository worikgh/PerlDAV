package WebDAV;
use strict;
use warnings;
use FCGI;
use XML::LibXML;

use Carp;
use Fcntl qw(:flock SEEK_END); # For accessing properties
# To validate resource URLs from clients
use Data::Validate::URI  qw(is_uri is_web_uri); 
use URI::Escape::XS qw/uri_escape uri_unescape/; # User supplied URLs
use Time::HiRes qw(stat time); # Generate ETAGs and tokens
use utf8;
use ResourceStore;
use WDGlobals qw|
    $ROOT 
    $DAVPASSWD
    $PWD
    %LIVE_PROPERTIES
    $LOGLEVEL
    $DATADIR
    $PROTOCOL
    $MAXSTORAGE
    $READLIMIT
    _LOG
    _readSTDIN
    date_1123
|;

# Prototypes
sub _comp( $$ );
sub _cleanTables();

# GLOBALS

# Namespaces
my $DAV_ns = 'DAV:';
my $DAV_pfx = 'D';
my $CalSrv_ns = 'http://calendarserver.org/ns/';
my $CalSrv_pfx = 'CS';
my $CDAV_ns = 'urn:ietf:params:xml:ns:caldav';
my $CDAV_pfx = 'C';


# File names.  FIXME  Move these out of the Web Server's DocumentRoot

# The password file.  WebDAV does not need this directly but utilities
# that add and remove users need it, so here it is.  Much match the
# file in the web server configoration file
# FIXME This needs to be in a configuration file
my $PASSWORD_FN = $PWD.$DAVPASSWD;


# The authorisation file
my $AUTHORISATION_FN = "._authorise";

# The file of users matches users with root directories
my $USERS_FN = "._Users";

# DAV Locks are in this file
my $LOCK_FN = '._DAV_LOCKS';

# The root collection
#my $ROOT = "ROOT";



# Translate an HTTP code to a message. FIXME must be a package for
# this!
my %HTTP_CODE_MSG = ();

# Getters for globals. Written as needed
sub get_USERS_FN(){return $USERS_FN;} # FIXME Is this ever used?
sub get_PASSWORD_FN(){return $PASSWORD_FN;}
sub get_ROOT(){return $ROOT;}


sub my_unescape_url( $ ){
    my $url = shift;
    # An empty string is valid.
    defined($url) or confess;
    return uri_unescape($url);

}


# Getters for LIVE properties

sub _creationdate( $ ) {
    # RFC4918 Sec 15.1
    #    Value:   date-time (defined in [RFC3339], see the ABNF in Section 5.6.)

    my $resource = shift;
    defined($resource) or confess; # Empty string is a valid resource"

    # Modification time as a proxy for creation
    my $mod_time = ResourceStore::get_att($resource, 'mod_time');
    my @gmtime = gmtime($mod_time);
    my $ret = sprintf("%4d-%02d-%02dT%02d:%02d:%02dZ", 
                      $gmtime[5]+1900, $gmtime[4]+1, $gmtime[3],
                      $gmtime[2], $gmtime[1], $gmtime[0]);
    return $ret;
}    
sub _displayname( $ ){
    # RFC4918 Sec 15.2
    #    Value:   Any text

    my $resource = shift or confess;
    return my_unescape_url(ResourceStore::get_displayname($resource));
}

sub _getcontentlanguage( $ ){
    # RFC4918 Sec 15.3
    # Value: language-tag (language-tag is defined in Section 3.10 of
    #    [RFC2616])
    my $resource = shift;
    defined($resource) or die; # "" is root collection 

    # If this is passed in then set it from the header, else default
    # to "en-UK"

    # FIXME Is this the correct default?  Can I do more to guess the
    # language?  Should I?
    return ResourceStore::get_getcontentlanguage($resource);
}
sub _getcontentlength( $ ) {
    # 15.4.  getcontentlength Property

    my $resource = shift;
    defined($resource) or confess; # "" is root collection 

    # FIXME Where is documentation for returning 0 for a collection content length?
    my $actual = _isCollection($resource)?0:ResourceStore::get_getcontentlength($resource);
    return $actual;
}
sub _getcontenttype( $ ){
    # 15.5.  getcontenttype Property
    # Value:   media-type (defined in Section 3.7 of [RFC2616])
    my $resource = shift;
    defined($resource) or die; # "" is root collection 

    # If there is a content-type stored for the resource use that.
    # Else empty string
    my $ret = ResourceStore::get_getcontenttype($resource);
    return $ret;
}
sub _resourcetype( $ ) {
    # 15.9.  resourcetype Property

    my $resource = shift;
    defined($resource) or die; # "" is root collection 

    my $type = ResourceStore::get_resource_type($resource);
    my $ret = XML::LibXML::Element->new("resourcetype");
    $ret->setNamespace($DAV_ns, $DAV_pfx, 1);
    # Collectons are the only resource types defined in RFC4918
    if($type eq 'collection'){
        $ret->addNewChild($DAV_ns, "collection");
    }
    #_LOG("_resourcetype \$ret: $ret  \$type $type \$resource $resource");
    return $ret;
    
}
sub _supportedlock( $ ){
    # 15.10.  supportedlock Property

    # FIXME When I implement locks...
    my $ret = XML::LibXML::Element->new("supportedlock");
    $ret->setNamespace($DAV_ns, $DAV_pfx, 1);
    return $ret;
}
sub _getetag( $ ) {
    # 15.6.  getetag Property
    my $resource = shift;
    defined($resource) or die; # "" is root collection 
    return ResourceStore::generate_etag($resource);    
}
sub _getlastmodified( $ ) {
    # 15.7.  getlastmodified Property
    # Value:   rfc1123-date (defined in Section 3.3.1 of [RFC2616])
    my $resource = shift;
    defined($resource) or confess;
    my $last_modified = ResourceStore::get_last_modified($resource);
    my $ret = date_1123($last_modified);
    return $ret;
}
sub _lockdiscovery( $ ){
    #15.8.  lockdiscovery Property
    # FIXME When I implement locks...

    my $ret = XML::LibXML::Element->new("lockdiscovery");
    $ret->setNamespace($DAV_ns, $DAV_pfx, 1);
    return $ret;
}
sub _quota_available_bytes( $ ){
    # RFC 4331 3.  DAV:quota-available-bytes
    my $resource = shift;
    defined($resource) or confess;
    my $ret = XML::LibXML::Element->new("quota-available-bytes");
    $ret->setNamespace($DAV_ns, $DAV_pfx, 1);
    $ret->appendTextNode(ResourceStore::get_available_bytes($resource));
    return $ret;
}

sub _quota_used_bytes( $ ){
    my $resource = shift or die;
    my $ret = XML::LibXML::Element->new("quota-used-bytes");
    $ret->setNamespace($DAV_ns, $DAV_pfx, 1);
    $ret->appendTextNode(ResourceStore::get_used_bytes($resource));
    return $ret;
}

## End of live property getters

sub initialise_live_properties(){
    #$LOGLEVEL > 2 and _LOG("initialise_live_properties");    
    %LIVE_PROPERTIES = (
        "creationdate" => [0, 1, 0, \&_creationdate],
        "displayname" => [-1, 1, 0, \&_displayname],
        "getcontentlanguage" => [-1, 0, 0, \&_getcontentlanguage],
        "getcontentlength" => [1, 0, 0, \&_getcontentlength], 
        "getcontenttype" => [-1, 0, 0, \&_getcontenttype],
        "getetag" => [1, 0, 0, \&_getetag],
        "getlastmodified" => [1, 0, 0, \&_getlastmodified], # RFC4918 is Ambiguous FIXME How?
        "lockdiscovery" => [1, 1, 1, \&_lockdiscovery],
        "resourcetype" => [1, 0, 0, \&_resourcetype],
        "supportedlock" => [1, 0, 0, \&_supportedlock],
        "quota-available-bytes" =>[1, 1, 1, \&_quota_available_bytes],
        "quota-used-bytes" =>[1, 1, 1, \&_quota_used_bytes]
        );

}
sub initialiseServer( ){
    # Check all the files we need exist and are readable
    eval{
        foreach my $_fn ( $AUTHORISATION_FN, $LOCK_FN){
            -r $_fn or die "$!: '$_fn' not readable";
            -w $_fn or die "$!: '$_fn' not writable";
            
        }
	initialise_live_properties();
	ResourceStore::initialise();
    };
    if($@){
        die $@;
    }

    -d $DATADIR or die "\$DATADIR '$DATADIR' is not a directory";


    %HTTP_CODE_MSG = (
        100  => "Continue",
        101  => "Switching Protocols",
        102  => "Processing",
        200  => "OK",
        201  => "Created",
        202  => "Accepted",
        203  => "Non-Authoritative Information",
        204  => "No Content",
        205  => "Reset Content",
        206  => "Partial Content",
        207  => "Multi-Status",
        208  => "Already Reported",
        226  => "IM Used",
        300  => "Multiple Choices",
        301  => "Moved Permanently",
        302  => "Found",
        303  => "See Other",
        304  => "Not Modified",
        305  => "Use Proxy",
        307  => "Temporary Redirect",
        308  => "Permanent Redirect",
        400  => "Bad Request",
        401  => "Unauthorized",
        402  => "Payment Required",
        403  => "Forbidden",
        404  => "Not Found",
        405  => "Method Not Allowed",
        406  => "Not Acceptable",
        407  => "Proxy Authentication Required",
        408  => "Request Timeout",
        409  => "Conflict",
        410  => "Gone",
        411  => "Length Required",
        412  => "Precondition Failed",
        413  => "Payload Too Large",
        414 =>	"URI Too Long",
        415  => "Unsupported Media Type",
        416  => "Range Not Satisfiable",
        417  => "Expectation Failed",
        421  => "Misdirected Request",
        422  => "Unprocessable Entity",
        423  => "Locked",
        424  => "Failed Dependency",
        425  => "Unassigned",
        426  => "Upgrade Required",
        427  => "Unassigned",
        428  => "Precondition Required",
        429  => "Too Many Requests",
        430  => "Unassigned",
        431  => "Request Header Fields Too Large",
        500  => "Internal Server Error",
        501  => "Not Implemented",
        502  => "Bad Gateway",
        503  => "Service Unavailable",
        504  => "Gateway Timeout",
        505  => "HTTP Version Not Supported",
        506  => "Variant Also Negotiates",
        507  => "Insufficient Storage",
        508  => "Loop Detected",
        509  => "Unassigned",
        510  => "Not Extended",
        511  => "Network Authentication Required"
        );
    eval {
        _cleanTables();
    };
    if($@){
        _LOG($@);
    }
}

#==================================
# Documentation

# RFC4918.

# Principal - A distinct human or computational actor that initiates
# access to network resources.

# Live Properties: Maintained by the server.  Either a derived
# property of data on the server (such as length or a hash of it) or a
# property whoes syntax is checked by the server

# Dead Properties: Properties which the server simply records

# Property Names: Universally unique identifier associated with a
# schema.  XML namespaces are used to stop collisions between property
# names.

# Property Values:  Well formed XML

# Resources: Identified, uniquely, by a URI.  A resource can be
# identified by more than one URI, but a URI describes just one
# resource.  A resource accessible over the network is accessed by a
# URL (a specialisation of a URI)

# Collections: A resource that is a container for other resources

# Depth: In the context of a collection 'depth' can be 0, 1 or
# 'infinity'.  '0' specifis only the collection. '1' specifies the
# colletion and directly contained resources. 'infinity' specifies the
# collection and all contained resources recursively

# Collection State: (At least [sic section 5.2]) a set of mappings
# from path segments to resources and a set of properties for the
# collection itself.  There can be additional state "...such as entity
# bodies returned by GET" (page 16 sec. 5.2)

# Collections and URL Namespaces: If a URL A/B/C defines a WebDAV
# complient resource X, and URL A/B defines a WebDAV complient
# resource Y then Y is a collection that contains exactly one mapping
# from "C" to X.  Analogous to file systems

# Collection URLs: A collection may be referred to with out the
# trailing slash ('/') In that case the server should add the trailing
# slash to the URL itself.

# Locks: Exclusive or shared, write locks only (but extensible).  A
# direct lock is when a LOCK request is made to a WebDAV complient
# resource. (The resource is created if it does not exist).  If a
# collection is locked and a resource is added to a locked collection
# it becomes indirectly locked.  (It must not have a conflicting lock
# already) If a resource is removed from a locked collection the lock
# is removed from the resource.  

# Lock Tokens: Each lock is identified by a single unique lock token.

# If the the original URL of a lock (the lock-root) becomes unmapped
# by a request then that lock MUST also be deleted by the request.

# Shared Locks: Every request for a shared lock on a resource by a
# principal gets its own token.

# Destroying Locks: It is not only the lock owner who can destroy a
# lock.  Other users MAY be able to destroy a lock.  RFC3744 WebDAV
# ACL describs access control

# Privileges and Locks: Holding a lock does not imply that a principal
# has full control over a resource.  There may exist normal privilege
# and authentication mechanisms that do not depend on (possibly
# opaque) lock tokens. (Section 6.4 Page 20)

# Lock Tokens: Lock tokens are opaque to clients.  Each lock has
# exactly one token.  Lock token URIs MUST be unique across all
# resources for all time.  Lock tokens are returned in a Lock-Token
# response header.

# Lock Timeouts: Lock time outs can be recommended by the client but
# are decided by the server.  When a time out expires then the server
# SHOULD act as if the lock was unlocked (by a UNLOCK method).  But a
# client MUST NOT rely on locks being removed on time out (clients
# MUST assume that locks can dissapear at any time)

# Unlockable Live Properties: All live properties are lockable, unless
# otherwise declared not.

# HTTP/WebDAV Methods: PUT, POST, PROPPATCH, LOCK, UNLOCK, MOVE, COPY
# (for the destination resource), DELETE, and MKCOL are effected by
# [write] locks.  No ther methods are (GET in particular)

# Properties: Dead properties of locked resources and Live properties
# defined as lockable can only be changed by a holder of a lock to
# that resource.

# Unmapped URLs: A write lock on an unmapped URL creates a (non
# collection) empty resource and locks it.  Return a 201 code ("201
# Created")

# MKCOL and Locks: There is no atomic way (as of RFC4918) to create a
# collection and lock it

# Locked Collections: A depth 0 lock protects the properties and the
# internal URLs of the collection, not the content or properties
# (other than URLs) of contained resources.  So a depth 0 lock
# prevents internal members being DELETEd, MOVEd away from the
# collection or new resources into the collection, external resources
# COPY'd into the collection PUT or MKCOL requests to create new
# internal members.

# Locked Collections: A depth-infinity lock does all a depth-0 does in
# adition: Any new resource added to the collection becomes
# "indirectly locked".  (Any indirectly locked member moved out of a
# collection to a unlocked collection becomes unlocked.  But this
# contradicts specification of depth-0 locks.  Moved into a locked
# collection the indirect lock of the resource is now that of the
# destination collection

# Submitting Lock Tokens: Lock tokens are subbmitted in "If" headers,
# and must be submitted for each operation on a locked resource when
# doing an operation that would be prevented by the lock.

# Copying Locked Resources: Since only write locks are covered by
# RFC4918 locked resources can be copied.  The locks are not
# duplicated.

# Refreshing Locks: Locks are refreshed by submitting a lock request
# (using an "If" header) on a resource with out a body.  At the least
# this resets timers.

# Error Reporting: Report authorisation errors first.  This prevents
# leaking information about resources (see Sec. 8.1).  The server MUST
# do authorisation checks before checking any HTTP conditional header
# (Sec. 8.5).

# Content-Type: application/xml SHOULD be used for all communications
# in WebDAV that use XML.  Clients MUST accept both application/xml
# and text/xml.  Use of text/xml is deprecated.

# Reference Resolution: Every "href" value in a multi-status response
# must use the same scheme for URLs.  A reletive reference, (relative
# to the Request-URI), or a full URI.

# Request Bodies: Some WebDAV methods (and HTTP methods) do not
# require bodies.  The server MUST check for a body in all cases.  If
# one is found for a method that the server would ignore the request
# MUST be rejected with 415 (Unsupported Media Type)

# ETag: Use ETags and preserve them.  

# Methods
# -------

# PROPFIND: (9.1) Retrieve properties of a resource identified by URI.
# MUST be supported.  

# PROPPATCH: (9.2) Sets and/or removes properties in the identified
# resource.  MUST be supported.

# MKCOL: (9.3) Creates a new collection resource at the Request-URI.
# If the Request-URI is already mapped to a resource then MKCOL MUST
# fail.  If any of the ancestors of the Request-URI do not exist the
# method must fail.  E.g., creating /A/B/C/D/ when /A/B/C/ does not
# exist MUST fail

# GET, HEAD for Collections: (9.4) unchanged from normal HTTP.
# Defined by the server (c.f. GET http://example.com may return an
# index.html or a directory listing).  The returned data may well not
# be correlated with the actual collection.  Up to the server

# POST for Collections: (9.5) Opaque documentation.  TBC

# DELETE: (9.6) Delete a resource.  In addition MUST destroy locks
# rooted on the deleted resource, MUST remove mapping from the
# Request-URI to the resource.

# DELETE for Collections: (9.6.1) Server MUST act as if "Depth:
# infinity" header used.  (The client MUST NOT submit a depth header
# on a DELETE request on a collection with any value other than
# "infinity").  Delete the whole collection, except if any member
# cannot be deleted for whatever reason the descendants of the
# un-deleted resource MUST NOT be deleted.

# PUT: (9.7.1) Replace the resource

# PUT for Collections: (9.7.2)  Undefined.  Use MKCOL to create collections

# COPY: (9.8) Create a duplicate of a resource.  Collections can be
# copied in their entirety (Depth1:infinity) or just the collection and
# its properties (Depth:0)

# MOVE: (9.9) A COPY followed by "consistency maintenance" to keep the
# servers state consistent - URLs to the original resource must be
# updated (it is not made clear in the RFC sec. 9.9 what URLs are
# maintained) followed by a DELETE of the original resource.  MOVE on
# collections is analogous to COPY on collections

# LOCK: (9.10)  Lock a resource

# UNLOCK: (9.11) Unlock a resource

# Headers
# -------

# DAV: (10.1) From server. Reports the "complience class" of the
# server and other DAV related capabilities for an OPTION method.

# Depth: (10.2) From client.  Used with resources that may have
# internal members.  Depth:0 implies the resource only, Depth:1 The
# resource ant its "internal members only" [sic How is this different
# from Depth:infinity?] and Depth:infinity implies the resource and
# all its members.

# Destination: (10.3) For COPY and MOVE, where the resource is going.
# Can be absolute and another server...

# If: (10.4) Various conditional transactions can be implemented using
# 'If:'.  Passing locks, ETag dependencies etcetera.  Some deep
# concepts here!

# Lock-Token: (10.4) Identifies the lock token to be removed in an
# UNLOCK request.

# Overwrite: (10.6) During a COPY or MOVE indicates if the detination
# resource should be overwritten if it exists.  MUST support

# Timeout: (10.7) Submitted with a LOCK request by the client.  The
# server does not have to honor it

# Status Code Extensions
# ----------------------

# 207 Multi-Status: (11.1) Provides status for multiple independant
# operations (Sec. 13)

# 422 Unprocessable Entity: (11.2) There is an error in the WebDAV
# part of the request not the HTTP part.

# 423 Locked: (11.3) The resource is locked. (Duh!)

# 424 Failed Dependency: (11.4) The method to be performed depended on
# another action that could not be performed.

# 507 Insufficient Storage: (11.5)

# 412 Precondition Failed: (12.1) The request contained a conditional
# header (HTTP: If-Match, If-Modified-Since, etcetera) or "If" or
# "Overwrite" WebDAV headers and the condition failed this is
# returned.

# 414 Request-URI Too Long: (12.2) ONLY for Request-URIs.

# Multi-Status Responses
# ----------------------

# When one operation can result in several status codes.  The codes
# can be in the 200, 300, 400 or 500 families, not 100.

# The root element holds zero or more "response" elements in any
# order, each with information about an individual resource. Each
# "response" element MUST have a "href" element to identify the
# resource.

# Generally the "response" element has a "status" element as a child
# defining the status code for the operation on that resource

# For PROPFIND and PROPPATCH there can be a "propstat" rather than
# "status" providing information about the properties of a resource.

# Use of a "Location:" header with a "Multi-Status" response is
# undefined (13.1)

# Redirection (13.2) The "location" element must be used in a response
# when a resource has moved, rather than a "Locatuion:" header.

# Properties
# ----------

# creationdate: (15.1) Date and time a resource is created.  

# displayname: (15.2) Contains a description of the resource that is
# suitable for presentation to a user.

# getcontentlanguage: (15.3) Content-Language header value to use for
# a resource

# getcontentlength: (15.4) Defined on any WebDAV resource that
# supplies a Content-Length geader on a GET

# getcontenttype: (15.5) Content-Type header value for a resource

# getetag: (15.6) ETag header value for a resource

# getlastmodified: (15.7) Last-Modified value for a resource

# lockdiscovery: (15.8) Returns a list of who has a lock.

# resourcetype: (15.9) MUST be defined on all WebDAV-complient
# resources.  Specifies the type of the resource.  Defaults to empty

# supportedlock: (15.10) 

# Pre/Post-Condition Errors
# -------------------------

# Section 16

# Many methods have pre and post conditions.  If those conditions are
# violated there are errors XML elements that can be in the body of
# the response.

# lock-token-matches-request-uri (precondition) 409 Conflict:  The
# lock token in an UNLOCK request does not include the resource in its
# scope.

# lock-token-submitted (precondition) 423 Locked: There should have
# been a lock submitted with the request.

# no-conflicting-lock (precondition) Typically 423 Locked: A LOCK
# request failed because of an existing lock

# no-external-entities (precondition) 403 Forbidden: The server is
# rejecting a requestbecause the body contains an external entity. (?)

# no-external-entities (postcondition) 409 Conflict: The server a
# (otherwise) valid COPY or MOVE request but cannot maintain the live
# properties at the new destination


# propfind-finite-depth (precondition) 403 Forbidden: The server does
# not allow infinite depth PROPFIND requests on collections

# cannot-modify-protected-property (precondition) 403 Forbidden: The
# client attempted to set a protected property with a PROPPATCH

# DAV Class
# ---------

# Section 18

# Class 1: Implements all MUST requirements in RFC4918

# Class 2: Class 1 and supports LOCK methods, properties and the
# "owner" XML element

# Class 3: Class 1 and support for the revisions made to RFC2518 by
# RFC4918.  MAY support Class 2.

# Security
# --------

# Section 20

# Authentication: Digest authentication scheme RFC2617 MUST be
# supported

# Denial of Sevice Attacks: Possible at almost every level.  PUTing
# large files, recursive operations on large collections etcetera.
# RFC4918 advises responding with 400 level codes

# Privacy Concerns Connected to Locks: (20.4) The LOCK request can
# have an "owner" element that cntains private information.  Servers
# SHOULD limit read access to DAV:lockdiscovery.  

# Privacy Concerns Connected to Properties: (20.5) Properties can
# contain information (e.g., "author" elements).  Sercvers are
# encouraged to [sic] develop access control mechanisms that separate
# read access to the resource body and read access to the resource's
# properties.

# XML Entities: (20.6) There is a mechanism for including external
# entities in XML.  These are untrustworthy.  If a server chooses not
# to handle external XML entities, it SHOULD respond to requests
# containing external entities with the 'no-external-entities'
# condition code.

# Additionally there's also a risk based on the evaluation of
# "internal entities" as defined in Section 4.2.2 of [REC-XML].  A
# small, carefully crafted request using nested internal entities may
# require enormous amounts of memory and/or processing time to
# process.  Server implementers should be aware of this risk and
# configure their XML parsers so that requests like these can be
# detected and rejected as early as possible.

# Lock Tokens: It is tempting to make unique tokes using MAC
# addresses.  This leaks information and should be avoided.  See
# RFC4122

#==================================

#my $socket = FCGI::OpenSocket("ABC", 10000);
#my $socket = FCGI::OpenSocket("ABC", 10000);

# my $warn_handler = sub { print STDERR @_ };
# my $die_handler = sub { print STDERR @_ unless $^S };
# $SIG{__WARN__} = $warn_handler if (tied (*STDIN));
# $SIG{__DIE__} = $die_handler if (tied (*STDIN));

#my $request = FCGI::Request(\*STDIN, \*STDOUT, \*STDERR, \%ENV, 0, FCGI::FAIL_ACCEPT_ON_INTR);

#==================================================================
# Funtions that control the state of resources and their properties

# Properties File: Properties are in blocks separated with blank
# lines.  Each block contains properties for one resource.  The first
# line of a block is the resource URI staring at column 0 (no leading
# white space).  Then follows the block of properties. The block ends
# with a blank line.

# Each property starts in column 0.  From the start to the first "\t"
# character is the name of the property.  If the property continues
# over several lines each continuation line starts with a single space
# character (if the first character of the continuing line is a space
# there will be two spaces in a row).  FIXME Test multi line property
# values.

# LIVE properties are stored as name/value pairs.  DEAD properies
# stored as XML where the root element is used as the name complete
# wih '<' and '>' and the XMLNS declaration

# Getters for LIVE properties

sub getCreationDate( $ ){
    my $resource = shift;
    defined($resource) or confess; # Empty string is a valid resource"
    return ResourceStore::_creationdate($resource);
    # my $path = _resourceToPath($resource) or 
    #     die "500 Server Error: Resource '$resource' has ano path";
    # -e $path or die "500 Server Error: Resource '$resource' has a ".
    #     "path: '$path' that does not exist";
    # my @stat = stat($path);
    # my $modTime = $stat[9]; # Modification time as a proxy for creation
    # my @gmtime = gmtime($modTime);
    # my $ret = sprintf("%4d-%02d-%02dT%02d:%02d:%02dZ", 
    #                   $gmtime[5]+1900, $gmtime[4]+1, $gmtime[3],
    #                   $gmtime[2], $gmtime[1], $gmtime[0]);
    # return $ret;
}

# sub getDisplayName( $ ){
#     $LOGLEVEL > 1 and _LOG("");
#     my $resource = shift or confess;
#     $resource =~ /([^\/]+)$/ or die "\$resource '$resource' is invalid";
#     my $ret = $1;
#     $ret = _myUnescapeURL($ret);
#     return $ret;
# }
# sub getGetContentLanguage( $ ){
#     $LOGLEVEL > 1 and _LOG("");
#     # If this is passed in then set it from the header, else default
#     # to "en-UK"

#     # FIXME Is this the correct default?  Can I do more to guess the
#     # language?  Should I?

#     my $ret = _getHeader("Content-Language");
#     defined($ret) or $ret = "en-UK";
#     return $ret;
# }
# sub getGetContentLength( $ ){
#     $LOGLEVEL > 1 and _LOG("");
#     my $resource = shift;
#     defined($resource) or confess; # "" is root collection 
#     return ResourceStore::_creationdate( $resource );
#     # my $path = _resourceToPath($resource) or 
#     #     die "500 Server: Error. Resource '$resource' has no path";
#     # -e $path or die "$!: Asking for 'getcontentlength' on ".
#     #     "'$resource' at '$path' that does ot exist";

#     # # We check this twice
#     # my $collection = _isCollection($resource);

#     # my $actual = $collection?0:-s $path;
#     # my $header = _getHeader("Content-Length");
#     # !$collection and defined($header) and $header ne $actual and 
#     #     _LOG("'getContentLength' Resource '$resource' has header ".
#     #          "'Content-Length' at '$header' but the actual size of ".
#     #          "the file at '$path' is $actual Called from: ".join(":", caller()));
#     # return $actual;
# }
# sub getGetContentType( $ ){
#     $LOGLEVEL > 1 and _LOG("");
#     # If there is a content-type header use that.  Else return an
#     # empty string
#     my $ret = _getHeader("Content-Type");
#     defined($ret) or $ret = "";
#     return $ret;
# }
# sub getGetETAG( $ ){
#     $LOGLEVEL > 1 and _LOG("");
#     my $resource = shift;
#     defined($resource) or die; # "" is root collection 
#     return _generateETAG($resource);    
# }
# sub getGetLastModified( $ ){
#     $LOGLEVEL > 1 and _LOG("");
#     my $resource = shift;
#     defined($resource) or confess; # "" is root collection
#     return ResourceStore::get_last_modified($resource);
#     # my $path = _resourceToPath($resource) or 
#     #     die "500 Server: Error. Resource '$resource' has no path";
#     # my @stat = stat($path);
#     # my $ret = _date1123($stat[9]);
#     # return $ret;
# }    
# sub getLockDiscovery( $ ){
#     $LOGLEVEL > 1 and _LOG("");
#     # FIXME When I implement locks...
#     my $ret = XML::LibXML::Element->new("lockdiscovery");
#     $ret->setNamespace($DAV_ns, $DAV_pfx, 1);
#     # No active locks!
#     return $ret;
# }
# sub getResourceType( $ ){
#     $LOGLEVEL > 1 and _LOG("");
#     my $resource = shift;
#     defined($resource) or die; # "" is root collection 
#     my $ret = XML::LibXML::Element->new("resourcetype");
#     $ret->setNamespace($DAV_ns, $DAV_pfx, 1);
#     # Collectons are the only resource types defined in RFC4918
#     if(_isCollection($resource)){
#         $ret->addNewChild($DAV_ns, "collection");
#     }
#     return $ret;
# }
sub _cannonicalResourceName( $$ ){
    # Pass in a resource named with our prepended root and give it
    # back in the form the client passed it
    my $principal = shift or confess;
    my $resource = shift or confess;

    my $_pRoot = _principalsRoot($principal);
    $resource =~ s/^ROOT\/$_pRoot\/?// or die "Cannot rename '$resource'";
    return $resource;
}
sub _localResourceName( $$ ){
    # Pass in a resource fom the client and prepend the ROOT resource
    # on it
    my $principal = shift or confess;
    my $resource = shift;
    defined($resource) or confess; # Empty string is root
    $resource = _principalsRoot($principal)."/$resource";
    $resource = "$ROOT/$resource";
    return $resource;
}
sub _getMaxStorage( $ ){
    my $principal = shift or die;
    return $MAXSTORAGE;
}

# # <D:quota-available-bytes>596650</D:quota-available-bytes>
# # <D:quota-used-bytes>403350</D:quota-used-bytes>
# sub _getAvailableBytes( $ ){
#     _LOG("FIXME:  Quota manegement unimplemented");
#     return 1000;
# }
# sub _getUsedBytes( $ ){
#     _LOG("FIXME:  Quota manegement unimplemented");
#     return MAXSTORAGE/2;
# }
# sub getQuotaAvailableBytes( $ ){
#     $LOGLEVEL > 1 and _LOG("");
#     my $resource = shift or die;
#     my $ret = XML::LibXML::Element->new("quota-available-bytes");
#     $ret->setNamespace($DAV_ns, $DAV_pfx, 1);
#     $ret->appendTextNode(_getAvailableBytes($resource));
#     return $ret;
# }

# sub getQuotaUsedBytes( $ ){
#     $LOGLEVEL > 1 and _LOG("");
#     my $resource = shift or die;
#     my $ret = XML::LibXML::Element->new("quota-used-bytes");
#     $ret->setNamespace($DAV_ns, $DAV_pfx, 1);
#     $ret->appendTextNode(_getUsedBytes($resource));
#     return $ret;
# }


# Functions to handle the Property files

sub _initialiseResourceProperty( $ ){
    my $resource = shift;
    defined($resource) or confess; # "" is root collection
    ResourceStore::initialise_resource_property($resource);
}

sub _removeResourceProperty( $$ ){
    my $resource = shift;
    defined($resource) or die; # "" is root collection 
    my $which = shift or confess; #LIVE or DEAD
    return ResourceStore::remove_resource_property($resource, $which);
}


sub _addProperty( $$$$ ){
    my $resource = shift;
    defined($resource) or die; # "" is root collection 
    my $name = shift or confess;
    my $value = shift;
    defined($value) or die; # May be 0 or ""
    my $which = shift or confess; #LIVE or DEAD
    return ResourceStore::add_property($resource, $name, $value, $which);
    # if($which eq "DEAD"){
    #     if(ref($name) =~ /^XML:LibXML:/){
    #         # An XML object
    #         $name = $name->toString();
    #         $LOGLEVEL > 2 and _LOG("FIXME _addProperty called with a name that is a ".
    #                               "XML::LibXML object:'$name' \$value '$value'");
    #     }elsif(ref($name) ne ""){
    #         # A string is OK.  Anything else a type error
    #         die "500 Server Error:_addProperty Property name invalid. ".
    #             "ref(\$name) is '".ref($name)."' which is not understood";
    #     }
    # }
    # ref($name) eq "" or die "500 Server Error:\$name '$name' ref(\$name) '".
    #     ref($name)."'";
    # my $fh = _lockProperties($which);
    # my $ret = _addPropertyf($resource, $name, $value, $fh);
    # close($fh)  or die "$!: '$LIVE_PROPERTIESDBFN'";
    # return $ret;
}

sub _readProperty( $$$ ){
    my $resource = shift;
    defined($resource) or die; # "" is root collection 
    
    my $propertyName = shift or confess;
    my $which = shift or confess; #LIVE or DEAD
    my $value =  ResourceStore::read_property($resource, $propertyName, $which);

    # Some values have to be encoded in XML and some do not.
    return $value;
    # my $fh = _lockProperties($which);
    # my $ret = _readPropertyf($resource, $propertyName, $fh);
    # close($fh)  or die "$!: '$LIVE_PROPERTIESDBFN'";
    # return $ret;
}

sub _deleteProperty( $$$ ){
    my $resource = shift;
    defined($resource) or die; # "" is root collection 
    my $propertyName = shift or confess;
    my $which = shift or confess; #LIVE or DEAD
    return ResourceStore::delete_property($resource, $propertyName, $which);
    # my $fh = _lockProperties($which);
    # my $ret = _deletePropertyf($resource, $propertyName, $fh);
    # _checkForOrphanProperties();
    # close($fh)  or die "$!: '$LIVE_PROPERTIESDBFN'";
    # return $ret;
}


sub _editProperty( $$$$ ){
    my $resource = shift;
    defined($resource) or die; # "" is root collection 
    my $name = shift or confess;
    my $value = shift;
    defined($value) or confess;
    my $which = shift or confess; #LIVE or DEAD
    return ResourceStore::edit_property($resource, $name, $value, $which);
    
    # my $fh = _lockProperties($which);
    # my $ret = _editPropertyf($resource, $name, $value, $fh);
    # close($fh)  or die "$!: '$LIVE_PROPERTIESDBFN'";
    # return $ret;
}

sub _listProperties( $;$ ){
    my $resource = shift;
    defined($resource) or die; # "" is root collection 
    my $what = shift;
    defined($what) or $what = "";
    return ResourceStore::list_properties($resource, $what);
    # # If $what eq "" return a HASH.  2-keys: LIVE and DEAD.  Values
    # # are ARRAY refs of property names.  If $what eq "LIVE" or $what
    # # eq "DEAD" return an array of property names
    # my @live;
    # my @dead;
    # $what and $what ne "LIVE" and $what ne "DEAD" and
    #     die "Unknown value of \$what: '$what'";
    

    # if($what eq "" or $what eq "LIVE"){
    #     my $fh = _lockProperties("LIVE");
    #     @live = _listPropertiesf($resource, $fh);
    #     _unlockProperties($fh);
    # }

    # if($what eq "" or $what eq "DEAD"){
    #     my $fh = _lockProperties("DEAD");
    #     @dead = _listPropertiesf($resource, $fh);
    #     _unlockProperties($fh);
    # }
    # if($what eq ""){
    #     return (LIVE => \@live, DEAD => \@dead);
    # }else{
    #     $what eq "LIVE" and return @live;
    #     $what eq "DEAD" and return @dead;
    #     die "Unknown value of \$what: '$what'";
    # }
    # die "Reality interuption...";
}



# sub _supportedlock( $ ){
#     # FIXME Implement locks
#     my $resourse = shift or confess;
#     my $ret = XML::LibXML::Element->new("$DAV_pfx:supportedlock");
#     return $ret;
# }
sub _copyDeadProperties( $$ ){
    # Copy the dead properties form the first resource passed to the
    # second
    my $src = shift or confess;
    my $dst = shift or confess;
    my @props = _listProperties($src, "DEAD");
    my $fh = _lockProperties("DEAD");
    foreach my $prop (@props){
        my $v = _readPropertyf($src, $prop, $fh);
        _addPropertyf($dst, $prop, $v, $fh);
    }
    _unlockProperties($fh);
}
# sub _createCollection( $ ) {
#     # Create the collection defind by the pased resource.  All the
#     # checking must be done *BEFORE* this is called
#     my $resource = shift;
#     defined($resource) or die; # "" is root collection 
#     my $path = _resourceToPath($resource, "Collection");

#     # A special case is the root collection.  The path will exist
#     if($resource eq ""){
# 	-e $path or die "$!: No root collection directory";
#     }else{
# 	mkdir($path) or die "500 Internal Server Error '$!'";
#     }

#     _initialiseResourceProperty($resource);
#     _setLiveProperties($resource);
#     return 1;
# }

#=====================================
#
# Start internal LOCK handling

# Format of the lock file.  A lock per line.  The 'owner' field is a
# problem because it may contain new lines.  We should not just
# eleminate them as some of the text fields may contain new lines
# *internally*.  But tough luck.  If you put a nicely formatted
# address field in the 'owner' field all the new lines will be
# replaced by spaces.  FIXME!  Format of lock record:
# "<principal>\t<resource>\t<root>\t<scope>\t<type>\t<owner>\t<timeout>\t<refreshed>\t<token>"
# Where <refreshed> is the UNIX time stamp of when the lock was
# created or refreshed


# sub _getLockEntries_fh( $$ ){
#     # Passed a file handle (assume to $LOCK_FN) that is open for
#     # append and locked (we will not append, but that will be how the
#     # file is open) and also passed a resource look for a lock entry
#     # for that resource and if we find it return it.  The complete
#     # line.  FIXME This is putting the format of the lock file lines
#     # in several places.
#     my $fh = shift or confess;
#     my $resource = shift;
#     defined($fh) or confess; # "" is root
#     seek($fh, 0, 0) or confess "$!: Cannot rewind '$LOCK_FN'";
#     my $ln = 0;
#     my @entries = (); # Return this
#     while(my $line = <$fh>){
# 	$ln++;
# 	chomp $line;
# 	# FIXME This code is more than one place and we could put the
# 	# definition of lock file entries in just *one* place
# 	$line =~ /^\s*#/ and next; #Comments
# 	$line =~ /\S/ or next; #  Ignore blank lines
# 	my @l =  split(/\t/, $line);
# 	unless(@l and @l == 9){
# 	    confess "Line $ln of $LOCK_FN is '$line' and confuses us";
# 	}
# 	if($l[1] eq $resource){
# 	    push(@entries, $line);
# 	}
#     }
#     return @entries;
# }


{
    # Closure to hold lock data

    # When using locks first open the lock file ($LOCK_FN) with this
    # handle and lock it exclusive.  FIXME this is so that resource
    # locks cannot be changed whist the resource is in use.  This does
    # not need the whole file locked, we could only lock the resources
    # we need to lock.  
    my $LOCK_FH = undef;

    # Lock line in files: 
    # "<token>\t<principal>\t<resource>\t<root>\t<scope>\t<type>\t<owner>\t<timeout>\t<refreshed>\t<depth>"

    # type is 'write' as that is the only type of lock there is.

    # Structures to hold locks: %resource_lock is hash mapping
    # resources to lock tokens.  The value is an ARRAY ref one to many
    # mappings.  %locks maps lock_tokens to HASH ref of lock data
    my %resource_lock = ();
    my %locks = ();


    sub _getLockTokens( $ ){
        my $resource = shift or confess;
        my @ret = ();
        defined($resource_lock{$resource}) and @ret = @{$resource_lock{$resource}};
        return @ret;
    }

    # FIXME  A comment block for this HASH please!
    sub _getTokens( ){
        # Debugging function
        return join(" :: ", sort keys %locks);
    }

    #
    sub _locks_as_text{
	# Pass in a array of tokens and return a line for each lock
	my $ret = "";
	while(my $token = shift){
	    $ret .= "$token\t";
	    if(!defined($locks{$token})){
		$ret .= "UNDEF\n";
		next;
	    }
	    my %lock = %{$locks{$token}};
	    #my @keys = qw| principal resource root scope type owner timeout refreshed depth |;
	    $ret .= defined($lock{resource})?$lock{resource}:'<undef>';
	    $ret .= "\t";
	    $ret .= defined($lock{refreshed})?scalar(localtime($lock{refreshed})):'<undef>';
	    $ret .= "\t";
	    $ret .= defined($lock{timeout})?$lock{timeout}:'<undef>';
	    $ret .= "\t";
	    $ret .= defined($lock{principal})?$lock{principal}:'<undef>';
	    $ret .= "\t";
	    $ret .= defined($lock{root})?$lock{root}:'<undef>';
	    $ret .= "\t";
	    $ret .= defined($lock{scope})?$lock{scope}:'<undef>';
	    $ret .= "\t";
	    $ret .= defined($lock{type})?$lock{type}:'<undef>';
	    $ret .= "\t";
	    $ret .= defined($lock{owner})?$lock{owner}:'<undef>';
	    $ret .= "\t";
	    $ret .= defined($lock{depth})?$lock{depth}:'<undef>';
	    $ret .= "\n";
	}
	return $ret;
    }
    sub _debugStringLocks(){
        my $token = shift;
        my @tokens = defined($token)?($token):keys(%locks);
        my $ret = '';
        foreach my $_t(@tokens){
            $ret .= "Token '$_t' ";
            # Get all lock parameters for the token
            $ret .= " Details: ".
		join(", ", 
		     map{"'$_' -> '$locks{$_t}->{$_}'"}
		     sort keys %{$locks{$_t}});
        }
        return $ret;
    }

    sub _getLocks( $ ) {
        # Pass a token and return lock data for it
        my $token = shift;
        defined($token) or confess;
        return $locks{$token};
    }

    # This is incremented to keep lock tokens global.  See
    # '_makeLockToken($$)'
    my $LOCK_SFX = 1;

    sub _initialiseLocks(){
	#$LOGLEVEL>2 and _LOG("_initialiseLocks()");
        defined($LOCK_FH) or confess "Lock file '$LOCK_FN' not open";
        seek($LOCK_FH, 0, 0) or confess "$!: Cannot rewind '$LOCK_FN'";
        %resource_lock = ();
        %locks = ();

        while(my $l = <$LOCK_FH>){
            chomp($l);
            $l =~ /\S/ or next; # Skip blank lines
            my @fields = split(/\t/, $l) or 
                confess "Line $. of '$LOCK_FN': '$l' is invalid";
            # Lock line in files: 
            # "<token>\t<principal>\t<resource>\t<root>\t<scope>\t<type>\t<owner>\t<timeout>\t<refreshed>\t<depth>"
            scalar(@fields) == 10 or
                confess "Line $. of '$LOCK_FN': '$l' is invalid";
            my $token = $fields[0];
            my $resource = $fields[2];
            defined($resource_lock{$resource}) or $resource_lock{$resource} = [];
            push(@{$resource_lock{$resource}}, $token);

            if(!defined($locks{$token})){
                $locks{$token} = {
                    'principal' => $fields[1],
                        'resource' => $fields[2],
                        'root' => $fields[3],
                        'scope' => $fields[4],
                        'type' => $fields[5],
                        'owner' => $fields[6],
                        'timeout' => $fields[7],
                        'refreshed' => $fields[8],
                        'depth' => $fields[9],
                };
            }else{
                die "Token '$token' defined twice";
            }			  
        }
    }

    sub _releaseLocks(){
	#$LOGLEVEL>2 and _LOG("_releaseLocks()");

        if(defined($LOCK_FH)){

            # Save all the locks
            truncate($LOCK_FH, 0) or die "$!: Cannot truncate  '$LOCK_FN'";
            seek($LOCK_FH, 0, 0) or confess "$!: Cannot rewind '$LOCK_FN'";
            my @lock_keys = qw|principal resource root scope type owner timeout refreshed depth|;

            foreach my $token (keys(%locks)){
                # For each resource this token locks write a line in the file
                my @_lockData = ($token);
                for(my $i = 0; $i < @lock_keys; $i++){
                    my $_ld = $locks{$token}->{$lock_keys[$i]};
                    if(defined($_ld)){
                        push(@_lockData, $_ld);
                    }else{
                        _LOG("Undefined key: '$token' '$lock_keys[$i]'");
                    }
                }
                my $line = join("\t", @_lockData);
                
                if($line !~ /\S/){
                    # Line is blank
                    next;
                }
                print($LOCK_FH "$line\n") or 
                    confess "$!: Could not write '$line' to '$LOCK_FN'";
            }		
            close($LOCK_FH) or confess "$!:  Cannot close '$LOCK_FN'";
            $LOCK_FH = undef;
        }
    }

    sub _setLockFH(){
        # If the file was opened and locked return 1.  If it was
        # already open return 0.  Faclitates initialising structures
        # we use for holding locks

        # FIXME  This should have a better name...
        my $ret = 0;
        if(!defined($LOCK_FH)){

            open($LOCK_FH, "+<", $LOCK_FN) or
                confess "$!: Could not open '$LOCK_FN' for append";
            flock($LOCK_FH, LOCK_EX) or confess "$!: Could not lock '$LOCK_FN' ";
            _initialiseLocks();
            
            $ret = 1;
        }
        return $ret;
    }

    sub _cleanStaleLocks(){
	$LOGLEVEL>2 and _LOG("_cleanStaleLocks()");

        # Clean up all stale locks from it

        # A time based algorithm.  If <refresh> plus <timeout> is more
        # than $now then the lock is stale
        my $now = time(); 

        my @tokens = keys(%locks);
        foreach my $token (@tokens){
            my ($timeout, $refresh) = ($locks{$token}->{timeout},
                                       $locks{$token}->{refreshed});
            if($timeout + $refresh < $now){
                # $refresh is when lock created, $timeout is how long
                # it can live.  So if $now is beyond that sum it has
                # gone stale This is a stale lock.
                my $r = $locks{$token}->{resource};
                delete($locks{$token});

                my @_t = grep{$_ ne $token} @{$resource_lock{$r}};
                if(@_t){
                    $resource_lock{$r} = \@_t;
                }else{
                    delete($resource_lock{$r});
                }
            }
        }
    }

    # When If clauses unlock resources put them in this keyed by
    # resources value an array ref of locks that match.  FIXME Why?

    # Store all lock tokens we encounter in IT headers here as ARRAY
    # refs: [<token>, <resource>]
    my @known_locks = ();

    sub _if( $$ ){

        # FIXME  This is crazey complex.  

        # Handle If clauses.  Mostly deals with locks...

        # Return a hash keyed by resource and each value an ARRAY ref
        # with the lock tokens and etags that apply to that resource.  

        # Passed a resource.  Get the data with the 'If' header.  It
        # will be 'token's (locks as of RFC4918) and etags.  They may
        # be matched with resources in the header or they may apply to
        # the reource passed (tagged or not, below).  There is a
        # complex logic in the syntax of the 'If' header that is
        # evaluated (if the resources are in the scope of locks and if
        # etags match the resource).  A hash is returned with the
        # element keyed by '_if' being 1 or 0 and is the result of
        # that logic.  All other resource/etag and resource/lock pairs
        # where the key is the resource and the value is an array of
        # locks and/or etags
        
        # RFC4918 12.1 says that if a request fails because of an if
        # header then return '412'.  For accessing locked resources
        # this function is used but the return hash is studied in the
        # caller to see if a corrct lock was passed.  So the overall
        # 'If' header must evaluate to true AND the lock must be
        # valid.  Thi sis due to the complex logic of the 'If' header,
        # and FIXME still not sure it is correct.  

        # If it is a lock token that is inappropriate still return 412
        # Precondition Failed not 423 Locked.  But the callers must
        # make that decision
        
        my $principal = shift or confess;
        my $resource = shift or confess; 
        
        my $IF = _getHeader('If');
        if(!defined($IF)){

            # FIXME Should we fail if no header?
            # FIXME  Bad style returning here
            return (_if => 1);
        }
        
        # 10.4.2.  Syntax

        #      If = "If" ":" ( 1*No-tag-list | 1*Tagged-list )

        #      No-tag-list = List
        #      Tagged-list = Resource-Tag 1*List

        #      List = "(" 1*Condition ")"
        #      Condition = ["Not"] (State-token | "[" entity-tag "]")
        #      ; entity-tag: see Section 3.11 of [RFC2616]
        #      ; No LWS allowed between "[", entity-tag and "]"

        my $clauses = $IF;

        # Check for No-tagged lists
        my $tagged = 1;
        # FIXME  Is leading whitespace legally possible here?
        if($clauses =~ /^\s*\(/){
            $tagged = 0;
        }elsif($clauses =~ /^\s*</){
            $tagged = 1;
        }else{
            confess "\$clauses: '$clauses'";
        }

        # RFC4918 10.4.2 The syntax distinguishes between untagged
        # lists ("No-tag-list") and tagged lists ("Tagged-list").
        # Untagged lists apply to the resource identified by the
        # Request-URI, while tagged lists apply to the resource
        # identified by the preceding Resource-Tag.

        # In this array the resources and conditions applied to them
        # will be stored.  Each entry is a ARRAY ref.  For each member
        # array the first entry is a resource and the subsequent
        # entries are ARRAY refs.  Each ARRAY ref holds an array of
        # conditons.  If just one of those arrays of conditions
        # evaluate to true we can return 1.  But we must examine all
        # conditions as we need to record if we have seen a
        # State-token (i.e., a lock)

        # This cannot be a hash keyed by resources because in a tagged
        # list the same resource can be used twice as a tag

        # For No-tagged lists @resource_conditions will have one
        # entry.  
        my @resource_conditions;

        # This is the hash to return on success.  Each key is a
        # resource and each value a ARRAY ref of tokens and etags
        # passed

        my %resource_tags = ();

        my $_processList = sub {
            # Passed a list of Conditions (RFC4918 10.4.2) return a
            # ARRAY ref of conditions.
            my $_c = shift or confess;
            my $ret = [];
            $_c =~ /\s*.+\s*$/ or confess "Invalid condition list: '$_c'";



            while($_c){
                my $_cond = '';

                # If condition is negated
                $_c =~ s/^\s*Not\s*// and $_cond = '!';

                # '<urn:uuid:AEC9D9D6-D3A5-4A20-4A20-2EEDBEBDCFC8FD06>'

                # FIXME "[abc>" will pass this RE
                $_c =~ s/^\s*([<\[][^>\]]+[>\]])// or 
                    confess "In condition list: '$_c' Invalid condition: '$_cond'";

                $_cond .= $1;

                push(@$ret, $_cond);
            }
            return $ret;
        };
        # Extract the conditions with the resources they are tagged to
        if($tagged){

            # A Resource-Tag applies to all subsequent Lists, up to
            # 	the next Resource-Tag.

            # Each condition has a tagged resource
            while($clauses =~ /\S/){
                # While there are non white space characters in the
                # clauses string

                # Strip off leading tag
                $clauses =~ s/^\s*<([^>]+)>// or 
                    confess "No leading tag in clause: '$clauses'";
                my $_tag = $1;
                if(_isURL($_tag)){
                    $_tag = _decodeURL('Ooops', $_tag);
                }
                $_tag = _localResourceName($principal, $_tag);

                # The resource/conditon array starts with the tag
                my @rc = ($_tag);

                # Strip off lists untill we find another tag
                while($clauses =~ s/^\s*\(([^\)]+)\)//){
                    # $1 is list in raw form
                    my $_list = &$_processList($1);
                    # $_list is an array ref of conditions from the list
                    push(@rc, $_list);
                }
                # $clauses either has another tag or is empty
                push(@resource_conditions, \@rc);
            }

        }else{

            # No-tag-list All conditions apply to the passed resource,
            # so $clauses is a string of "List" as in 10.4.2

            my @cond = ($resource);
            while($clauses =~ s/^\s*\(([^\)]+)\)//){

                # $1 is list in raw form
                my $_list = &$_processList($1);
                # $_list is an array ref of conditions from the list
                push(@cond, $_list);
            }
            @resource_conditions = (\@cond);

        }


        # We need to evaluate each condition and remember the
        # State-tokens.  If one condition evaluates to true we return
        # true.  Never the less we remember all State-tokens and
        # associated resources
        my $TRUE = 0; # If we find a list where all conditions are
        # true then set this
        foreach my $rc (@resource_conditions){
            #foreach my $rc (keys %resource_tags){
            
            my $res = shift(@$rc);

            # The resource is often encoded as a URL FIXME: _decodeURL
            # requires a 'principal' argument butdoes not use it
            # (yet).  We need to either pass the principal around
            # *everywhere* or nowhere and put it in a global
            # The return hash must be maintained
            defined($resource_tags{$res} ) or $resource_tags{$res} = [];
            foreach my $_c (@$rc){
                # FIXME  Why are we stripping out '!'?
                my @_c = @$_c;
                push(@{$resource_tags{$res}}, map{s/^\!//; $_} @_c);
            }


            foreach my $cond(@$rc){
                my $_TRUE = 1;  # Each @$cond is a conjunct.  So if one
                # is false they are all false
                foreach my $c (@$cond){
                    
                    # Check for a negated rule
                    my $_not = $c =~ s/^\!//?1:0;
                    # RFC4918 10.4.4.  Handling unmapped URLs: For
                    # both ETags and state tokens, treat as if the URL
                    # identified a resource that exists but does not
                    # have the specified state.
                    if(!_resourceExists($res)){
                        $_TRUE = 0;
                    }else{
                        if($c =~ /^\[(.+)\]$/){
                            # Matching entity tag: Where the entity tag
                            # matches an entity tag associated with the
                            # identified resource.  Servers MUST use
                            # either the weak or the strong comparison
                            # function defined in Section 13.3.3 of
                            # [RFC2616].
                            my $_etag0 = $1;
                            my $_etag1 = _generateETAG($res);
                            if($_etag0 ne $_etag1){
                                $_TRUE = 0;
                            }
                        }elsif($c =~ /^<(.+)>$/){
                            # A lock tag
                            my $lock = $1;

                            # RFC4918 10.4.1 Additionally, the mere fact
                            # that a state token appears in an If header
                            # means that it has been "submitted" with the
                            # request.  In general, this is used to
                            # indicate that the client has knowledge of
                            # that state token.  The semantics for
                            # submitting a state token depend on its type
                            # (for lock tokens, please refer to Section
                            # 6).  FIXME  Why?  What is the point?
                            push(@known_locks, [$lock, $res]);
                            
                            if($_TRUE){
                                $_TRUE = _matchLock($res, $lock);
                            }
                        }
                        if($_not){
                            if($_TRUE){
                                $_TRUE = 0;
                            }else{
                                $_TRUE = 1;
                            }
                        }			
                    } # if(!$exists){..}else{..}
                }

                # If we have not yet found a condition that is true
                # (!$TRUE) then set the value of $TRUE to $_TRUE.  
                !$TRUE and $TRUE = $_TRUE;
            }
        }

        $resource_tags{'_if'} = $TRUE;
        return %resource_tags;
    }


    sub _isLocked( $;$ ){
        # As of RFC4918 there are only WRITE locks.  But this may change.
        
        # This returns 1 if the resource is locked directly and if
        # resource is locked indirectly returns 'Cn' if locked depth
        # infinity and 'C0' if locked depth 0

        my $resource = shift;
        defined($resource) or die; # "" is root collection 
        my $type = shift;
        defined($type) or $type = "WRITE";
        
        $type eq 'READ' and return 0; # There should be read locks....
        $type ne "WRITE" and confess "Unknown lock type: '$type'";
	$LOGLEVEL>2 and _LOG("_isLocked($resource, $type)");
        my $ret = 0;
        #_setLockFH(); Must have been called

        my $_resource = $resource;
        my $_tokenRef = $resource_lock{$_resource};
        if(defined($_tokenRef)){
            # There is a lock on this resource.  We do not care what
            # token locked it
	    if($LOGLEVEL>2){
		_LOG("$resource is directly locked with: "._locks_as_text(@$_tokenRef));
	    }
            $ret = 1;
        }else{
            # $_resource is not locked.  But is it part of a locked
            # collection?
            while(1){
                $_resource eq $ROOT and last;
                my $_r = _getParentCollection($_resource);
                $_resource = $_r;
                $_tokenRef = $resource_lock{$_resource};
                defined($_tokenRef) or next;

                # There may a lock on an ancestor of the resource.
                # This time we care because we treat it differently if
                # it is a depth 0 or infinity lock.  We check to see
                # if any ancestor is locked 'infinity' if so we can
                # quit the loop as then our resource is locked.  If
                # any ancestor is locked depth 0 we report that in
                # $ret so caller knows it cannot move the resource (or
                # delete it) but it still may be locked 'infinty'
                # higher up FIXME Is this logic correct?  FIXME There
                # needs to be a test for this
                foreach my $_token(@$_tokenRef){
                    defined($locks{$_token}) or 
                        die "Checking for lock on resource '$_resource' and token '$_token' is in \%resource_lock but not \%locks";
                    my $depth = $locks{$_token}->{depth};
                    defined($depth)  or 
                        die "Checking for lock on resource '$_resource' and token '$_token' no depth defined";
                    if($depth eq 'infinity'){
                        $ret = 'Cn';
                    }elsif($depth == 0){
                        $ret = 'C0';
                    }else{
                        die "Checking for lock on resource '$_resource' and token '$_token' and depth is '$depth'";
                    }
                }
                $ret eq 'Cn' and last;
            }
        }    

        return $ret;
    }

    sub _getLock( $$;$ ) {
        # The first argument is the principal.  The second argument is
        # the resource that is the URL.  If the lock is obtained for
        # another resource (that was passed in a header) it is the
        # third argument and we check for a valid lock for that.  If
        # the user passed no token return -1. Else if all the locks on
        # the resource are shared or the user passed a lock token for
        # the resource and it is valied return '1'.  If the user
        # passed an invalid token return 0.

        # FIXME If the user passes a token, it is invalid but all
        # locks are shared, should we still return 1?

        my $principal = shift or confess;
        my $resource1 = shift or confess;
        my $resource2 = shift;
        my $resource = defined($resource2)?$resource2:$resource1;

        # Find if there are any tokens passed in a 'if' header
        my $_tref;
        my %_rt;
        my $_resource = $resource;
        while(1){
            %_rt = _if($principal, $_resource);
            $_tref = $_rt{$_resource} and last;

            # $_resource has no header but it may be part of a
            # collection that does
            $_resource = _getParentCollection($_resource);
            $_resource eq $ROOT and last;
        }    
        
        my $ret = 0; 
        my $all_shared = 1;
        if(!defined($_tref)){
            # No token
            $ret = -1;
        }else{
            # There was a token (tokens) so check if there is a valid
            # token
            my @tokens = @$_tref;
            # The "If" header clause must have been evaluated as true
            if($_rt{'_if'}){
                # Passed the clause, so default to -1, user passed no token
                $ret = -1;
                foreach my $t(@tokens){
                    # Two sorts of token: Etags "[...]" and lock tokens
                    # "<...>".  We care about lock tokens
                    $t =~ /^\s*<(.+)>\s*$/ or next;
                    my $_token = $1;
                    if(!_validToken($_token)){
                        $ret = -1;
                    }elsif(!defined($locks{$_token})){
                        # If token is not in locks it is invalid
                        $ret = 0;
                    }elsif(_matchLock($resource, $_token)){
                        # Got the lock
                        $ret = 1;
                        last;
                    }elsif($locks{$_token}->{scope} ne 'shared'){
                        # Lock exixts but is not matched and is not
                        # shared so owner passed invalid token
                        $all_shared = 0;
                    }
                }

                # If all the locks on the resource are shared the user
                # can have the lock

                # But not if the user is suppliying a lock token via a
                # header.  The user needs to use LOCK method

                # $all_shared and $ret = 1;

            }else{
            }
        }
        return $ret;
    }
    sub _removeLock( $$ ){
        # Remove a lock from a resource.  Passed the resource and the
        # lock token.  Checks that the lock is valid 

        my $resource = shift or confess;
        my $token = shift or confess;
        # Remove the lock in $token for $resource
        my $_canUnlock = sub {
            my $_res = shift;
            my $_root = shift;
            my $_t = shift;
            # Return true if the resource $_res is locked with token
            # $_t and has as its root $_root
            my $res = 0;

            if(defined($resource_lock{$_res})) {
                my @_a = grep {/^$_t$/} @{$resource_lock{$_res}};
                if(@_a and $locks{$_t}->{root} eq $_root){
                    $res = 1;
                }
            }
            return $res;
        };

        # Check the lock token passed exists 
        defined($locks{$token}) or 
            die "409 Conflict:lock-token-matches-request-uri No lock defined for '$token'";

        # Get the root of the lock
        my $root = $locks{$token}->{root};
        defined($root) or die "Lock token '$token' for resource '$resource' has no root";
        &$_canUnlock($resource, $root, $token) or 
            die  "409 Conflict:lock-token-matches-request-uri";


        # But it is possible to have a resource locked directly and
        # indirectly.  So commented out.  FIXME Need a test for
        # that...

        # If we get to here we can unlock everything
        delete($locks{$token});
        @{$resource_lock{$resource}} = 
            grep {$_ !~ /^$token$/} @{$resource_lock{$resource}};
        # If this is the last lock on a resource delete record of tokens locking it
        @{$resource_lock{$resource}} or delete($resource_lock{$resource}); 
    }
    

    sub _cannotLock( $$$$ ){
        
        # Check that the principal can lock the resource.  If they can
        # retuen FALSE (0) but if they cannot return a error string of
        # form "<status> <message>:<explanation>

        my $principal = shift or confess;
        my $resource = shift;
        defined($resource) or confess; # "" is root collection 
        my $scope = shift or confess; # Locking 'exclusive' is different from 'shared'
        my $token = shift or confess;
        my $ret = 0;

        if(!_authoriseResource($principal, $resource, "LOCK")){
            # Get out now with an error
            $ret =  "403 Forbidden:";
        }else{

            # Check for conflicting locks
            # See if a lock for this resource exists
            my @tokens = ();
            if(defined($resource_lock{$resource})){
                if(ref($resource_lock{$resource}) eq 'ARRAY'){
                    @tokens = @{$resource_lock{$resource}};
                }else{
                    # This is an error
                    _LOG("\%resource_lock{'$resource'} is not an ARRAY ref.  Is: '".ref($resource_lock{$resource})."'");
                }
            }

            # If there are some locks on ths resource check for
            # incompatibility

            # From RFC4918 9.10.5
            # +--------------------------+----------------+-------------------+
            # | Current State            | Shared Lock OK | Exclusive Lock OK |
            # +--------------------------+----------------+-------------------+
            # | None                     | True           | True              |
            # | Shared Lock              | True           | False             |
            # | Exclusive Lock           | False          | False*            |
            # +--------------------------+----------------+-------------------+
            if(@tokens){

                # The resource has locks out standing
                if($scope eq "exclusive"){
                    # There are locks on the resource and the user
                    # asked for an excusive lock
                    $ret = "423 Locked:resource '$resource' has a conflicting lock";
                }else{
                    # Check if any existing lock is exclusive
                    if(grep{
                        $locks{$_}->{scope} eq 'exclusive' 
                       } @tokens){
                        # There exists an exclusive lock on the
                        # resource
                        $ret = "423 Locked:resource '$resource' has a confliting lock";
                    }else{
                    }
                }
            }
        }

        return $ret;
    }

    sub _lock( $$$$$$$;$$$ ){

        # Get a lock on a resource 

        my $principal = shift or confess;
        my $resource = shift;
        defined($resource) or confess; # "" is root collection 
        my $root = shift;
        defined($root) or confess; # Can be "" root collection
        my $scope = shift or confess; # shared or exclusive
        my $type = shift or confess; # 'write'
        my $token = shift or confess;
        my $depth = shift;
        defined($depth) or confess; # Can be zero
        my $owner = shift; # Can be undef
        my $timeout = shift; # Can be undef
        my $refresh = shift; # Can be undef
	$LOGLEVEL>2 and _LOG("_lock($principal, $resource, $root, $scope, $type, $token, $depth...");
        # A new lock for a resource

        # Assume tha the principal is authorised

        # Initialise status for locking an existing resource.  If we have
        # to create an empty resource this will be reset to 201
        my ($status, $message) = (200, "OK");

        _cleanStaleLocks();
        if(defined($owner)){
            # Ensure that the XML has no new lines or tabs.  FIXME This is
            # bad as white space in in strings should be preserved
            $owner =~ s/[\n\r\t]/ /g;
        }else{
            $owner = '';
        }

        # Check resource.  If it exists do nothing.
	if(!ResourceStore::resource_exists($resource)){
        # my $path = _resourceToPath($resource);
        # unless(defined($path) and -e $path){
            # Create an empty resource.
	    _authoriseResource($principal, $resource, "CREATE") or
		die "401 Unauthorised:Failed authentication for '$resource'";
	    ResourceStore::create_resource($resource) or
		die "500 Server Error:$! Could not create resource '$resource'";
        }

        # Can get lock
        my $now = time(); 
        
        defined($resource_lock{$resource}) or $resource_lock{$resource} = [];
        $refresh or push(@{$resource_lock{$resource}}, $token);

        if(!defined($refresh) or !$refresh){
            defined($locks{$token}) and 
                confess "Locking resource '$resource' with token '$token' but ".
                "that token is already in use";
        }
        $locks{$token} = {
            'principal' => $principal,
                'resource' => $resource,
                'root' => $root,
                'scope' => $scope,
                'type' => $type,
                'owner' => $owner,
                'timeout' => $timeout,
                'refreshed' => $now,
                'depth' => $depth
        };

        return ($timeout, $status, $message);
    }

    sub _matchLock( $$ ){
        # Return true if the resource passed is in the scope of the passed
        # lock.  
        my $resource = shift;
        defined($resource) or confess; # "" is root collection 
        my $token = shift or confess;

        my $ret = 0;
        if(defined($locks{$token})){
            # There is a lock for the token

            # $resource is in scope of lock $token iff $resource is
            # locked by $token or an ancestor of $resource is locked
            # by $token

            my $_r1 = $locks{$token}->{resource};
            my $_r2 = $resource;
            while(!$ret){
                $_r1 cmp $_r2 or $ret = 1;
                $_r2 = _getParentCollection($_r2);
                $_r2 eq $ROOT and  last;
            }
        }
        return $ret;
    }
    sub _validToken( $ ){
        # Return 1 if the passed token looks like one we might make
        my $tok = shift or confess;
        # urn:uuid:D88FA61A-5649-2AD4-2AD4-6332CBE984A0098A
        $tok =~ /^urn:uuid:[0-9A-H]{8}-[0-9A-H]{4}-[0-9A-H]{4}-[0-9A-H]{4}-[0-9A-H]{16}/;
    }
    sub _makeUniqueToken( ){
        # Make a unique token.  Return string like:
        # "f81d4fae-7dec-11d0-a765-00a0c91e6bf6" We need 32 hex
        # digits.  Instead we will use the PID and $LOCK_SFX.
        # $LOCK_SFX keeps the lock unique in this process.  To make it
        # unique over time we will concatenate the time()
        my $raw = $$.$LOCK_SFX.time();
        my $token = sprintf("%X", $raw);
        $LOCK_SFX++;

        # Extend the token to 32 didgets
        while(length($token) < 32){
            $token = sprintf("$token%X", rand(16))
        }
        $token =~ /^[0-9a-h]{32}/i or confess "Token: '$token' is not 32 hex didgets.";
        my $ret = substr($token, 0, 8).'-'.
            substr($token, 8, 4).'-'.substr($token, 12, 4).'-'.
            substr($token, 12, 4).'-'.substr($token, 16);
        return $ret;    
    }
    sub _makeLockToken( $$ ){
        # Make a lock token unique for a resource.  Return string like:
        # "urn:uuid:f81d4fae-7dec-11d0-a765-00a0c91e6bf6" 
        my $principal = shift or confess;
        my $resource = shift;
        defined($resource) or confess; # "" is root collection 

        return 'urn:uuid:'._makeUniqueToken();
    }

}


# End of internal LOCK handling
#=================================================


sub _href( $ ){
    # Return the HREF for a resource
    my $resource = shift;
    defined($resource) or die; # Empty string is a valid resource"
    #my $resource = shift or confess;
    my $pfx = $ENV{SCRIPT_NAME};
    my $host = $ENV{SERVER_NAME};
    my $protocol = _getProtocol();
    #$resource =~ s/^\/$pfx// or die "Invalid resource '$resource'";
    my $ret =   "$protocol://$host$pfx/$resource";    
    _LOG("HREF: \$resource '$resource' \$host '$host' \$ret $ret");
    return $ret;
}
sub _rootURI( ) {
    # Generate the 'root-URI' from %ENV and return it.
    return $PROTOCOL . '://' . $ENV{HTTP_HOST}.$ENV{SCRIPT_FILENAME}.'/';
}


# sub _sizeOfResourcePathTable(){
#     # Debugging function
#     my $s = -s $RESOURCE_PATH_FN;
#     return "Size of $RESOURCE_PATH_FN': $s";
# }


sub _resourceOnThisWebDAV( $ ){
    #  Returns 1 if the passed resource on this server
    my $resource = shift;
    defined($resource) or die; # "" is root collection 

    # FIXME  How?
    my $ret = 1;
    if(is_web_uri($resource)){
        my $root = _rootURI();
        $resource !~ /^$root/ and $ret = 0;
    }
    return $ret;
}

sub _addToResourceTables( $$ ){

    # Passed a resource name and a resource type enter that resource
    # into the tables with a path for it.  Return the path in the file
    # system local to the working directory

    my $resource = shift or confess; 
    my $type = shift or confess;
    return ResourceStore::add_resource($resource, $type);
    # if($resource eq $ROOT){
    #     confess "Cannot call _addToResourceTables for '$resource'";
    # }
    # $type eq 'resource' or $type eq 'collection' or 
    #     confess "Type: '$type' missunderstood";

    # my $ret = undef;
    # my $_p = _getParentCollection($resource);
    # defined($_p) or
    #     die "409 Conflict:Parent of resource '$resource' does not exst";
    # my $parentPath = _resourceToPath($_p);

    # defined($parentPath) or 
    #     die "409 Conflict:No parent collection for '$resource'";
    # # We do not want the actual path in the file system, just where it
    # # is in the directory we use to store data
    # $parentPath =~ s/^$DATADIR\/?// or die "'$parentPath' not understood";


    # my $fh = _lockResourcePathTable();
    # my ($tableref, $typesref, $last) = _readResourcePathTable_fh($fh);
    # my %table = %$tableref;
    # my %types = %$typesref;

    # # $last will need to be incremented if we add a resource to %table
    # # and %types
    # my $_increment_last = sub(){
    #     if(!defined($last)){
    #         $last = "A";
    #     }else{
    #         $last =~ /([A-Z])$/ or die "last: '$last' invalid";
    #         my $one = $1;
    #         if($one eq "Z"){
    #             $last .= "A";
    #         }else{
    #             my $_next = chr(ord($one) + 1);
    #             $last =~ s/$one$/$_next/;
    #         }
    #     }
    #     return $last;
    # };

    # defined($table{$resource}) and 
    #     confess "500 Server Error: adding a resource that is alredy added";

    # # We must add this resource First check that the parent
    # # resource is there and is a collection

    # my $path = &$_increment_last();

    
    # $parentPath and $path = $parentPath . "/$path";
    # $table{$resource} = $path;
    # $types{$resource} = $type;

    # $ret = $DATADIR.'/'.$path;
    # -e $ret and 
    #     confess "500 Server Error:Path for resource '$resource' ".
    #     "exists in file system";
    
    # # Modified resource/path tables

    # _storeResourcePathTable_fh(\%table, \%types, $fh);
    # close($fh) or die "$! '$RESOURCE_PATH_FN'";

    # return $ret; 
}


sub _createCollection( $ ){
    # Create a collection
    my $collection = shift or confess; 
    return ResourceStore::create_collection($collection);
    # my $path = _createPath($collection,'collection');

    # if(defined($path)){
    #     my $_r = mkdir($path) or confess "500 Internal Server Error: '$!' mkdir('$path')";
        
    # }
    # $collection eq $ROOT and _LOG("_initialiseResourceProperty( '$ROOT' )");
    # _initialiseResourceProperty($collection);

    # # FIXME Should this be called here?
    # _setLiveProperties($collection);

    # return $path;
}
sub _createResource( $;$ ){
    # Create a resource
    my $resource = shift or confess;

    # Can be passed a ref to HASH that has properties to store....
    my $properties = shift;
    defined $properties or $properties = {};

    ResourceStore::create_resource($resource) or confess "Failed to create resource: $resource";
    foreach my $k (keys(%$properties)){
	my $name = $k;
	my $value = $properties->{$name};

	ResourceStore::add_property($resource, $name, $value, "LIVE");
    }
    return 1;
    # my $path = _createPath($resource,'resource');
    # open(my $_fh, ">$path") or confess "$! Cannot open '$path' for '$resource'";
    # close($_fh) or confess "Cannot close '$path'  for '$resource'";
    # $resource eq "$ROOT" and _LOG("_initialiseResourceProperty( $ROOT )");
    # _initialiseResourceProperty($resource);


    # # FIXME Should this be called here?
    # # _setLiveProperties($resource);

    # return $path;
}
# sub _resourceToPath( $ ){
#     # A mapping between resources and the file system:
#     my $resource = shift;
#     return ResourceStore::resource_to_path($resource);
#     # defined($resource) or die; # "" is root collection 
#     # # 20161111 Changed root resource to "ROOT"
#     # $resource eq $ROOT and return $DATADIR; # Special case, the root

#     # # All resources are stored WITHOUT the trailing slash.
#     # $resource =~ s/\/\s*$//;

#     # my $fh = _lockResourcePathTable();
#     # my ($tableref, $typesref, $last) = _readResourcePathTable_fh($fh);

#     # my %table = %$tableref;
#     # my %types = %$typesref;

#     # my $ret = undef;
#     # if($table{$resource}){
#     #     # Resource exists
#     #     $ret = $DATADIR . '/'.$table{$resource};
#     # }
#     # $LOGLEVEL > 2&&!defined($ret)&&_LOG("_resourceToPath($resource) undefined ");
#     # close($fh) or die "$! '$RESOURCE_PATH_FN'";
#     # return $ret;
# }

sub _cleanTables(){
    return ResourceStore::clean_tables();
    # # Ensure that the tables that translate resources to paths and
    # # store the types are in a good state.

    # # Do this by reading the tables and examine the paths in the file
    # # system.  The file system is the gold standard, if a document is
    # # in the tables but not in the file system delete it from the the
    # # tables.  If a document/directory is in the file system but not
    # # in the tables log but ignore.


    # # FIXME Should this be called here?
    # _initialiseResourceProperty($ROOT);
    # _setLiveProperties($ROOT);

    # my $fh = _lockResourcePathTable();
    # my ($tableref, $typesref, $last) = _readResourcePathTable_fh($fh);

    # # Check each file and directory in table exists
    # my @paths = sort keys %$tableref;
    # map{
    #     my $path = $tableref->{$_};
    #     unless(-e $DATADIR.'/'.$path){
    #         _LOG("Path: '$path' for resource: '".$_.
    #              "' type: '".$typesref->{$_}.
    #              "' not in file system.  Deleting from tables");
    #         delete($tableref->{$_});
    #         delete($typesref->{$_});

    #     }
    # } @paths;

    # # Log any entries in file system that are not in the tables

    # @paths = grep {$_ ne $DATADIR} map{chomp; $_} `find $DATADIR`;
    # $? and die "'$DATADIR' $?"; # Error in shell
    # foreach my $p  (grep{/\S/} @paths){
    #     $p =~ s/^$DATADIR\/?// or die "Path '$p' not understood";
    #     if(!grep{$_ eq $p} values %$tableref){
    #         _LOG("Entry in file system: '$p' not in tables");
    #     }
    # }
    # # # Ensure that the root resource is in the tables
    # # $$tableref{$ROOT} = $DATADIR;
    # # $$typesref{$ROOT} = 'collection';
    # _storeResourcePathTable_fh($tableref, $typesref, $fh);	
    # close($fh) or die "$! '$RESOURCE_PATH_FN'";


}

sub _removeResourceFromTables( $ ){
    # Remove the passed resource from the translation tables
    my $resource = shift;
    return remove_resource($resource);
    # defined($resource) or die; # "" is root collection 
    # my $fh = _lockResourcePathTable();
    # my ($tableref, $typesref, $last) = _readResourcePathTable_fh($fh);
    # delete($tableref->{$resource});
    # delete($typesref->{$resource});
    # _storeResourcePathTable_fh($tableref, $typesref, $fh);
    # close($fh) or die "$! '$RESOURCE_PATH_FN'";
}

sub _myUnescapeURL( $ ){
    my $url = shift;
    # An empty string is valid.
    defined($url) or confess;
    return uri_unescape($url);

}
sub _myEscapeURL( $ ){
    # 20150724 12:20

    my $url = shift;
    # An empty string is valid.
    defined($url) or confess;
    # FIXME  What characters to escape here?  
    return uri_escape($url, ' %');

 }

sub _principalsRoot( $ ){
    # Adjust the resource so it is under the principal's root
    my $principal = shift or confess;
    open(my $users, $USERS_FN) or die "$!: '$USERS_FN'";
    flock($users, LOCK_EX) or die "$!: Cannot get lock on '$USERS_FN'";
    my @root = grep{defined}map{/^$principal\t(.+)$/?$1:undef}<$users>;
    close($users) or die "$!: '$USERS_FN'";
    my $ret;
    if(@root){
        @root > 1 and die "There is more than one user record for '$principal'";
        $ret = $root[0];
    }
    return $ret;
}

sub _getResourceRAW( ){
    my $requesturi = $ENV{REQUEST_URI};
    my $scriptname = $ENV{SCRIPT_NAME};
    my $res = $requesturi;
    $res =~ s/^$scriptname//;
    $res =~ s/^\///; # Remove leading slash
    # Now resource is complete as sent.  If it is the root resource it
    # is an empty string.  
    return $res;
}
sub _getResource( $ ){
    my $principal = shift or confess;
    my $res = _getResourceRAW();

    # Modify the resource to account for the principal's data in
    # USER_FN
    $res = _localResourceName($principal, $res);

    $res =~ s/\/$//; # Remove trailing slash  FIXME Why?
    $res = _myUnescapeURL($res);
    return $res;
}

sub _addUsers( $ ){
    # Take a list of user records (passed as ARRAY refand append them
    # to the user file.  It is assumed that the records are all new
    # and each element of the supplied array is in the correct format,
    # no checking is done.  FIXME: Is it correct to not check
    # uniqueness and format here?
    my $userRecords = shift or confess;
    ref($userRecords) eq 'ARRAY' or confess ref($userRecords);
    open(my $users, ">>".$USERS_FN) or die "$!: '$USERS_FN'";
    flock($users, LOCK_EX) or die "$!: Cannot get lock on '$USERS_FN'";
    foreach my $record (@$userRecords){
        print($users "$record\n") or die "$!: Printing '$users' to '$USERS_FN'";
    }
    close($users) or die "$!: 'Webdav::$USERS_FN'";
}
sub _getUsers( ;$ ){
    my $name = shift;
    open(my $users, "<".$USERS_FN) or die "$!: '$USERS_FN'";
    flock($users, LOCK_EX) or die "$!: Cannot get lock on '$USERS_FN'";
    my @users = ();
    if(defined($name)){
        # Limit to searching for a user name
        @users = grep{defined} map{/^$name\t(.+)$/?"$name\t$1":undef}<$users>;
    }else{
        # All users
        @users = map{chomp; $_} <$users>;
    }
    close $users or die $!;
    return @users;
}

sub _isURL($){
    # Return true if the passed string is a URL for this system
    my $URL = shift or confess;
    my $protocol = _getProtocol();
    my $host = $ENV{SERVER_NAME};
    my $pfx = $ENV{SCRIPT_NAME};
    my $root = "$protocol://$host$pfx/";
    my $ret = 0;
    $URL =~ /^$root/ and $ret = 1;
    return $ret;
}    

sub _decodeURL( $$ ){

    # Pass a URL return the resource:

    # Return the resource name
    
    my $principal = shift or confess;
    my $URL = shift or confess;

    # Validate the resource URL.  It must be valid by RFC3986 
    my $ret = is_uri($URL);
    defined($ret) or die "400 Bad Request: Invalid Resource URI '$URL'";
    
    # FIXME: The principal and the user encoded in the URL interact.  
    my $protocol = _getProtocol();
    my $host = $ENV{SERVER_NAME};
    my $pfx = $ENV{SCRIPT_NAME};
    my $root = "$protocol://$host$pfx/";
    # my $root = _rootURI();
    $ret =~ s/^$root// or die "400 Bad Request:URL '$ret' not understood";

    # We remove the trailing slash from all resource names
    $ret =~ s/\/\s*$//;

    return $ret;
}

sub _removeLocks( $ ){
    # remove all locks from a resource.  
    my $resource = shift;
    defined($resource) or die; # "" is root collection 

    # FIXME this is a stub
    return 1;
    
}

sub _cannotDeleteError( $$ ){
    # Cannot delete a resource so return the appropriate XML
    my $resource = shift;
    defined($resource) or die; # "" is root collection 
    my $error = shift or confess;

    my $xml = XML::LibXML::Element->new("$DAV_pfx:response");
    my $href = $xml->addNewChild($DAV_ns, "href");
    my $status = $xml->addNewChild($DAV_ns, "status");
    my $responsedescription = 
        $xml->addNewChild($DAV_ns, 
                          "responsedescription");
    $href->appendTextNode(_href($resource));
    $status->appendTextNode("HTTP/1.1 500 Internal Server Error");
    $responsedescription->appendTextNode("'$resource' ".
                                         "Cannot be deleted: ".
                                         "'$error'");
    return (500, $resource, $xml);
}

sub _deleteResource( $$ ){
    # If possible delete the passed resource.  It is a fatal error to
    # pass a collection resource
    
    # If it can be deleted return (204, $resource, "Deleted")

    my $principal = shift or confess;
    my $resource = shift;
    defined($resource) or die; # "" is root collection 
    $LOGLEVEL>2 and _LOG("_deleteResource($principal,  $resource)");
    _isCollection($resource) and 
        die "_deleteResource passed a collection resource: '$resource'";

    # Check for locks.  If this is locked it will not be deleted.  If
    # the user passed an invalid token (_getLock returns 0) return 412
    # Precondition Failed if _isLocked returns -1 no token was passed
    my @ret = (); 
    if(_isLocked($resource)){
        my $_locked = _getLock($principal, $resource);
        if($_locked == -1){
            # No lock token passed
            $ret[0] = 423;
            $ret[2] = XML::LibXML::Element->new("$DAV_pfx:response");
            my $href = $ret[2]->addNewChild($DAV_ns, "href");
            my $status = $ret[2]->addNewChild($DAV_ns, "status");
            my $responsedescription = $ret[2]->addNewChild($DAV_ns, 
                                                           "responsedescription");
            $href->appendTextNode(_href($resource));
            $status->appendTextNode("HTTP/1.1 423 Locked");
            $responsedescription->appendTextNode("'$resource' is locked");
        }elsif($_locked == 0){
            # Invlid lock token
            $ret[0] = 412;
            $ret[2] = XML::LibXML::Element->new("$DAV_pfx:response");
            my $href = $ret[2]->addNewChild($DAV_ns, "href");
            my $status = $ret[2]->addNewChild($DAV_ns, "status");
            my $responsedescription = $ret[2]->addNewChild($DAV_ns, 
                                                           "responsedescription");
            $href->appendTextNode(_href($resource));
            $status->appendTextNode("HTTP/1.1 412 Precondition Failed");
            $responsedescription->appendTextNode("'$resource' is locked");
        }
    }elsif(!_authoriseResource($principal, $resource, "DELETE")){
        # If the principal does not have access to the resource,
        # do not delete it
        $ret[0] = 403;
        $ret[2] = XML::LibXML::Element->new("$DAV_pfx:response");
        my $href = $ret[2]->addNewChild($DAV_ns, "href");
        my $status = $ret[2]->addNewChild($DAV_ns, "status");
        my $responsedescription = $ret[2]->addNewChild($DAV_ns, 
                                                       "responsedescription");
        $href->appendTextNode(_href($resource));
        $status->appendTextNode("HTTP/1.1 403 Forbidden");
        $responsedescription->appendTextNode("'$resource' cannot be deleted ".
                                             "because the principal is ".
                                             "not authorised");

    }else{
        # It can be deleted
	$LOGLEVEL > 2&&_LOG("No error: $resource. Call delete_resource");
	@ret = ResourceStore::delete_resource($resource);
	@ret == 1 or die "ResourceStore::delete_resource did not return one-element array: join(\"\\n\", \@ret): ".join("\n", @ret);
	if($ret[0]->[0] == 500){
	    # Use a XML error message.  FIXME Why in just this case?
	    @ret = _cannotDeleteError($resource, $ret[0]->[2]);
	    @ret = (\@ret); 
	}
    }
    $LOGLEVEL > 2&&_LOG("_deleteResource('$resource') End");
    return @ret;
}

# sub _findAll( $ );
# sub _findAll( $ ){
#     # Passed a path find all files and directories beneath it
#     my $resource = shift;
#     defined($resource) or die; # "" is root collection 
#     $resource =~ /\/$/ or $resource .= "/";
#     my @ret = ();

#     opendir(my $dir, _resourceToPath($resource)) 
#         or die "$!: Cannot opendir for '$resource'";
#     my @dir = 
#         grep {$_ ne '.' and $_ ne '..'} readdir($dir) or 
#         die "$!: Cannot readdir '$dir'";

#     # assumption is collections are 
#     my @files = 
#         map{$resource.$_ }grep{-f _resourceToPath($resource.$_) } @dir;
#     push(@ret, @files);
#     my @dirs = map{$resource.$_ } grep{-d _resourceToPath($resource.$_)}@dir;
#     push(@ret, @dirs);
#     push(@ret, map{_findAll($_)}@dirs);
#     return @ret;
# }

sub _getDescendants( $ ){

    # # Returns all resources that are descendants of the passed resource
    # # (excluding the resource itself)
    my $resource = shift;
    return ResourceStore::get_descendants($resource);
    # defined($resource) or die; # "" is root collection 

    # my @ret = ();
    # # If the resource is not a collection return an empty array
    # if(_isCollection($resource)){
    #     my $fh = _lockResourcePathTable();

    #     my($tableref, $NOCARE, $DONOTCARE) = _readResourcePathTable_fh($fh);
    #     @ret = grep{/^$resource\//}sort keys %$tableref;
    #     close($fh) or die "$! '$RESOURCE_PATH_FN'";
    # }
    # return @ret;
}

sub _resourceExists( $ ){
    # Return 1 if the resource is known and exists in the file system.
    # Else 0
    my $resource = shift;
    defined($resource) or confess;
    return ResourceStore::resource_exists($resource);
    # my $ret = 0;
    # my $path = _resourceToPath($resource);
    # defined($path) and -e $path and $ret = 1;
    # return $ret;
}

sub _getChild( $ ){
    # Passed a resource gets the last piece of it (the child).  If
    # passed "/" it returns "" FIXME: Should it return undef in that
    # case?
    my $resource = shift;
    defined($resource) or die; # "" is root collection 
    my $parent = _getParentCollection($resource);
    $resource =~ /$parent(.*)$/;
    my $ret = "";
    defined($1) and $ret = $1;
    return $ret;
}
sub _getParentCollection( $ ){
    # Returns the parent collection of the passed resource.  If there
    # is no parent (the resource is root) return an empty string.  If
    # the parent does not exist return undef.  FIXME Should die as
    # that is an error
    my $resource = shift or confess;
    my $ret;

    if($resource =~ /^(.+)\/([^\/]+)\/?$/){
        if(_isCollection($1)){
            $ret = $1;
        } # Else the path was invalid so no parent
    }elsif($resource =~ /^$ROOT\/?/){
        $ret = "";
    }else{
        confess "Cannot understand resource '$resource'";
    }
    return $ret;
}

sub _ancestorsExist( $ ){
    my $resource = shift;
    defined($resource) or die; # "" is root collection 
    # Return true if all the ancestors exist
    my $parent = _getParentCollection($resource);
    my $ret;
    if(!defined($parent)){
        # It is an orphan
        $LOGLEVEL > 2&&_LOG("_ancestorsExist('$resource') has undefined parent");
        $ret = 0;
    }elsif($parent eq ""){
        # The collection is root
        $resource ne $ROOT and confess "_ancestorExist('$resource') has no parent";
        $ret = 1;
    }else{
        $ret = _isCollection($parent);
    }
    return $ret;
}


sub _listCollection( $;$ ){
    my $resource = shift;
    # defined($resource) or die; # "" is root collection 
    # # If $level is non zero only return the immediate children
    my $level = shift;
    return ResourceStore::list_collection($resource, $level);
    # defined($level) or $level = 0;
    # my $fh = _lockResourcePathTable();
    
    # my ($tableref, $typesref, $last) = _readResourcePathTable_fh($fh);
    
    # my @ret = grep{/^$resource\//} keys %$tableref;
    # $level and 
    #     # Only top level ones
    #     @ret = grep{/$resource\/[^\/]+\/?$/} @ret;
    # close($fh) or die "$! '$RESOURCE_PATH_FN'";
    # return @ret;
}

sub _copyCollection( $$$ ){
    # Copy a complete collection from one place to another
    my $principal = shift or confess;
    my $source = shift or confess;
    my $destination = shift or confess;

    # Build a list of all the operations that we will do.
    # This avoids infinite loops when copy like a/ -> a/b/

    my @source = ($source);
    push(@source, sort {$a cmp $b}  _listCollection($source));

    my @_s =  @source;  # Perl, you plonker!  The map{} below
    # changes the source array!
    my @destination = map{s/^$source/$destination/; $_} @_s;

    # The arrays must be the same length by construction
    @source == @destination or die "Reality interrupt";

    # Now copy the resources one at a time.  Copy the
    # collections first then the non-collections
    my @collection_copy = 
        map{[$source[$_], $destination[$_]]} 
    grep{_isCollection($source[$_])} 0..$#source;

    my @non_collec_copy = 
        map{[$source[$_], $destination[$_]]} 
    grep{!_isCollection($source[$_])} 0..$#source;

    my @ret = map{_copyResource($principal, $_->[0], $_->[1])}
    @collection_copy;

    push(@ret, map{_copyResource($principal, $_->[0], $_->[1])}
         @non_collec_copy);

    return @ret;
}

sub _preambleCopyMove( $ ){
    # COPY and MOVE have various conditions in common that must be
    # satisfied before they can proceed and define the source and
    # destination in similar ways.  This function is passed the
    # principal and it deduces the source and destination (which it
    # returns) as well as checking the authorisations and relevant
    # lock states of the resources.  If the COPY/MOVE is unautorised
    # or locked it "dies"

    my $principal = shift or die "500 Server Error: No principal";
    my $source = _getResource($principal);

    my $destination = _getHeader("DESTINATION") or 
        die "400 Bad Request: Destination header is missing";
    $destination = _cleanDestination($destination);

    $destination = _localResourceName($principal, $destination);
    my $_desc = " '$source' -> '$destination'";

    _authoriseResource($principal, $source, "COPYFROM") or 
        die "403 Forbidden: Unauthorised COPYFROM $_desc";
    _authoriseResource($principal, $destination, "COPYTO") or 
        die "403 Forbidden: Unauthorised COPYTO $_desc";
    # Check destination's ancestors all exist
    if(!_ancestorsExist($destination)){
        die "409 Conflict: Not All Ancestors of '$destination' Exist";
    }
	
    if(_isLocked($destination)){
        my $_lock = _getLock($principal, $source, $destination);
        # FIXME  Need a body for theresult
        $_lock == -1 and 
            die "423 Locked: Destination locked: $_desc";
        $_lock == -1 and 
            die "412 Precondition Failed: Destination locked: $_desc";
    }

    if(!ResourceStore::resource_exists($source)){
	# my $srcFN = _resourceToPath($source);
	# defined($srcFN) or die "404 Not Found: Source not in system $_desc";
	# -e $srcFN or die "404 Not Found: Source does not exist $_desc";
	die "404 Not Found: Source does not exist $_desc";
    }

    # Set overwrite, that defaylts to TRUE
    my $overwrite = _getHeader("Overwrite");
    defined($overwrite) or $overwrite = "T";
    $overwrite = uc($overwrite); # Case insensitive value Dusseault pg 135

    $LOGLEVEL > 2 and 
        _LOG("CMD ".$ENV{REQUEST_METHOD}.
             " '".(defined($source)?$source:"").
             " -> ".(defined($destination)?$destination:"")."' ".
             "Overwrite: ".
             (defined(_getHeader("Overwrite"))?_getHeader("Overwrite"):"").
             " Depth: ".(defined(_getDepth())?_getDepth():""));

    # my $dstFN = _resourceToPath($destination);
    # defined($dstFN) and -e $dstFN and $overwrite eq "F" and  
    if(ResourceStore::resource_exists($destination) and $overwrite eq "F"){
        die "412 Precondition Failed: Destination exists and cannot ".
        "be overwritten $_desc";
    }
    my $src_type = _getResourceType($source);
        die "404 Not Found:Cannot find type of: '$source'";
    # my $dst_type = ResourceStore::get_resource_type($destination);

    # The following block was commented out 20150910.  

    # Dusseault pg 133 says that the destination header is the
    # complete destination.  SO if a COPY or a MOVE has a
    # non-collection resource as the source and a collection resource
    # as the destination (it must exist otherwise we could not know
    # that it is a collection) the collection is DELETEed and replaced
    # with the resource renamed according to the destination header


    # defined($dst_type) and $dst_type ne $src_type and
    # 	# FIXME 20150810 This is my addition.  If the types do not
    # 	# match there are ambiguities in the implementation of the
    # 	# protocol
    # 	die "403 Forbidden: Types for '$source' and '$destination' do ".
    # 	"not match: $_desc";



    # If we get to here the COPY or MOVE can proceed.  It is not
    # guaranteed to succeed as there are more conditions for MOVE
    # (write access to delete source) and there may be collections
    # involved with members that have different permissions/locks
    my @ret = ($source, $destination);
    return @ret;
}

sub _207COPY_MOVE( $$ ){
    my $errorref = shift or confess;
    my $resource = shift;
    defined($resource) or die; # Empty string is a valid resource"
    #my $resource = shift or confess;
    my @_errors = @$errorref;
    my $xml  = XML::LibXML::Element->new("$DAV_pfx:multistatus");
    $xml->setNamespace( $DAV_ns,  $DAV_pfx, 1);
    foreach my $e (@_errors){
        ref($e) eq "ARRAY" or die "Error is wrong type";
        my $resp = $xml->addNewChild($DAV_pfx, "response");
        my $href = $resp->addNewChild($DAV_pfx, "href");
        # FIXME Is this correct?
        _addNode($href, _rootURI().$e->[0]);
        my $status = $resp->addNewChild($DAV_pfx, "status");
        _addNode($status, 'HTTP/1.1 '.$e->[1]." ".$e->[2]);
    }
    my @ret = (207, $xml);
    return @ret;
}

sub _copyResource( $$$ ){

    # Copy a single resource.  A collection (make the new directory if
    # it does not exist and copy the properties) or a file resource
    # FIXME What when individual resources can be something other than
    # files?

    # Return [$resource, $code]

    my $principal = shift or confess;
    my $source = shift or confess;
    my $destination = shift or confess;
    my $ret;

    eval {

        # Check authorisation first
        _authoriseResource($principal, $source, "COPY_FROM") or
            die "403 Forbidden:".
            "'$principal' is not authorised to COPY_FROM '$source'";

        _authoriseResource($principal, $destination, "COPY_TO") or
            die "403 Forbidden:".
            "'$principal' is not authorised to COPY_TO '$destination'";

        # If the method is MOVE this gets called
        # FIXME: Should I be using this global like this?  Is there a
        # better way?
        if($ENV{REQUEST_METHOD} eq "MOVE"){
            _authoriseResource($principal, $source, "MOVE") or
                die "403 Forbidden:".
                "'$principal' is not authorised to MOVE '$source'";

            if(_isLocked($source)){
                my $_lock = _getLock($principal, $source);
                # FIXME  Need a body for the result
                $_lock == 0 and 
                    die "412 Precondition Failed: Source locked Invalid token";
                $_lock == -1 and 
                    die "423 Locked: Source locked";
            }		
        }

        # FIXME: As I write I ensure these are the same.  But if the
        # destination is being created it will be same as source any
        # way.

	## Check permissions

	# If a COPY request has an Overwrite header with a value of
	# "F", and a resource exists at the Destination URL, the
	# server MUST fail the request.  This is the purpose of the
	# third parameter to ResourceStore::copy_resource $over_write
	my $over_write = 0;
	defined(_getHeader("Overwrite")) and _getHeader("Overwrite") eq "T" and $over_write = 1;

	if(!_authoriseResource($principal, $source, "COPYFROM")){
	    # 403 Forbidden - The property cannot be viewed without
	    # appropriate authorisation.
	    $ret = [$source, 403, "Unauthorised copy from"];
	}elsif(!_authoriseResource($principal, $source, "COPYFROM")){
	    # 403 Forbidden - The property cannot be viewed without
	    # appropriate authorisation.
	    $ret = [$source, 403, "Unauthorised copy from"];
	}elsif(!_authoriseResource($principal, $destination, "COPYTO")){
	    $ret = [$destination, 403, "Unauthorised copy to"];
	}else{
	    if(_isLocked($destination)){
		my $_lock = _getLock($principal, $source, $destination);
		# FIXME  Need a body for the result
		if($_lock == -1){
		    # No token passed
		    die "423:destination locked";
		}elsif($_lock == 0){
		    # Invalid token
		    die "412: Precondition Failed: Destination locked, invalid token";
		}
	    }
	    if(!_isCollection($source) and _isCollection($destination)){
		# Permitted as of 20150910 13:40
		_handle_DELETE($principal, $destination);
	    }elsif(ResourceStore::get_resource_type($source) eq 'collection' and ResourceStore::resource_exists($destination)){
		# Permitted as of 20161110 13:05
		_handle_DELETE($principal, $destination);
	    }
	    $ret = ResourceStore::copy_resource($source, $destination, $over_write);
	}
    };
    if($@){
        my ($code, $message) = (500, "Server Error");
        if($@ =~ /(\d{3})\s([^:]+):/){
            $code = $1;
            $message = $2;
        }else{
            _LOG($@);
        }
        $ret = [$source, $code, $message];
        # FIXME  Check for the case of running out of disc space
        # if(_tooLittleDiscSpace()){
        # $ret = [$destination, 507, "Insufficient Storage"];
        # }
    }    
    return $ret;
}


sub _getResourceType( $ ){
    
    # # Get the type of the passed rsource.  If the resource does not
    # # exist return undef

    my $resource  = shift;
    defined($resource) or confess;
    my $ret = XML::LibXML::Element->new("resourcetype");
    $ret->setNamespace($DAV_ns, $DAV_pfx, 1);
    if(ResourceStore::get_att($resource, 'resource_type') eq 'collection'){
        $ret->addNewChild($DAV_ns, "collection");
    }
    return $ret;
}

sub _isCollection( $ ){
    # Returns true if the passed resource is for a collection.  It
    # MUST be the ONLY place that the test for collections exists as
    # the implementation will change.

    # If the resource does not exist return false as it is not a
    # collection

    my $resource  = shift;
    defined($resource) or die;
    my $type = ResourceStore::get_resource_type($resource);
    my $ret = 0; 
    if(defined($type)){

        if($type eq "collection"){
            $ret = 1;
        }elsif($type eq "resource"){
            $ret = 0;
        }else{
            die "Type: '$type' not recognised";
        }
    }
    return $ret;
}


sub _generateETAG( $ ){
    # See section 8.6 RFC4918
    my $resource = shift;
    defined($resource) or confess; # "" is root collection 
    return ResourceStore::generate_etag($resource);
    # # FIXME: Is this too resource intensive?  We could generate this
    # # when we write the file and avoid re reading it here
    # my $path = _resourceToPath($resource) or 
    #     die "500 Server: Error. Resource '$resource' has no path";
    # -r $path or die "$!: Cannot read '$path'";
    # my $etag;
    # my $raw = $path;
    # my @_stat = stat($path) or die "$!: Cannot stat '$path'";
    # $raw .= $_stat[9]; # Last modify tie in hires
    # $etag = unix_md5_crypt($raw, "SALT");
    # my $ret =  "etag:$etag";
    # return $ret;

}
sub _getProtocol( ) {
    # Some times the PROTOOL ('HTTP' or 'HTTPS' probably) is in
    # $ENV{REQUEST_SCHEME} but that is not allways true.  Default to HTTPS
    my $ret = undef;
    defined($ret) or $ret = $ENV{REQUEST_SCHEME};
    defined($ret) or $ret = $ENV{SERVER_PROTOCOL} =~ /(HTTP.?)\/\d+\.\d+/?$1:undef;
    #_LOG("Protocol: $ret");
    defined($ret) or $ret = 'HTTP';
    $ret;
}
sub _getURL( ) {
    # Set the URL defined to access the resource

    # At this point the server under us (nginx or apache) has decoded
    # the URL, which has .  I do not want that, I want it encoded

    defined($PROTOCOL) or $PROTOCOL = $ENV{'REQUEST_SCHEME'} or $PROTOCOL = 'http'; ## FIXME Why?
    
    my $ret = $PROTOCOL . '://' . 
        $ENV{HTTP_HOST}.$ENV{DOCUMENT_ROOT}.$ENV{REQUEST_URI};
    $ret =~ s/\/\//\//g; # Change '//' to '/' # FIXME Why?

    #$ret = _myEscapeURL($ret);
    
    return $ret;
}

sub _cleanDestination( $ ){
    # takes a destination and checks if it is relative (e.g.,
    #   CollectionTwo/Child) or absolute (e.g.,
    #   https://localhost/webdav/CollectionTwo/Child)
    # Returns relative path

    my $destination = shift or confess;

    #FIXME  This is for apache.  Generalise
    my $sn = $ENV{SCRIPT_NAME};
    $destination =~ s/^.+$sn//;
    # Get rid of leading and trailing slashes
    $destination =~ s/^\s*\///;
    $destination =~ s/\/\s*$//;
    $destination = _myUnescapeURL($destination);
    return $destination;

}

sub _requestURI( ) {
    # Generate the 'request-URI' from %ENV and return it.
    return $ENV{PATH_INFO};
}

sub _checkProps( $$;$ ){

    # Ensure that the %$propstatREF has an entry for the passed code.  

    my $code = shift or confess; # HTTP 3-digit code
    my $propstatREF = shift or confess;

    # Can be undefined when called from handle_PROPPATCH
    my $propREF = shift; # or die; 

    if(!defined($propstatREF->{$code})){
        $propstatREF->{$code} = 
            XML::LibXML::Element->new("$DAV_pfx:propstat");
        $propstatREF->{$code}->setNamespace($DAV_ns, $DAV_pfx, 1);
        my $status = 
            $propstatREF->{$code}->addNewChild("$DAV_ns", "status");
        my $msg = $HTTP_CODE_MSG{$code};
        if(!defined($msg)){
            _LOG("Failed to find a HTTP message for code '$code'");
            $msg = "";
        }
        _addNode($status, "HTTP/1.1 $code $msg");
        
        # FIXME what is this all about?  It adds a empty node to the
        # propstat/code and to prop/code.
        defined($propREF) and $propREF->{$code} = 
            $propstatREF->{$code}->addNewChild($DAV_ns, "prop");
    }
}

sub _handle_propname( $$$ ){

    # handle a <propname> request in a PROPFIND  method
    my $resource = shift;
    defined($resource) or die; # Empty string is a valid resource"
    #my $resource = shift or confess;
    # This functions works on the %propstat and %prop hashes in
    # _propfindResource that are passed by reference
    my $propstatREF = shift or confess;
    my $propREF = shift or confess;

    # return the names of all properties

    my %properties = _listProperties($resource);
    my @lnames = @{$properties{LIVE}};
    my @dnames =  @{$properties{DEAD}};

    # Ensure we have a place to put the names
    _checkProps(200, $propstatREF, $propREF);

    # @lnames which is names of live propertues is
    # plain text of node names that are in the DAV: anmespace
    foreach my $n (@lnames) { 
        # FIXME Namespaces for contained properties
        my $node = XML::LibXML::Element->new($n);
        $node->setNamespace($DAV_ns, $DAV_pfx, 1);
        $propREF->{200}->addChild($node);
    }
    
    # @dnames are names from dead properties and are all made from the
    # XML::LibXML::Element::toString function
    my $parser = XML::LibXML->new();
    foreach my $n (@dnames) { 
        # FIXME Namespaces for contained properties
        my $node = $parser->parse_balanced_chunk($n);
        $propREF->{200}->appendChild($node);
    }
}

sub _handle_prop( $$$$ ){
    # handle a <prop> request in a PROPFIND  method
    my $resource = shift;
    defined($resource) or die; # Empty string is a valid resource"
    #    my $resource = shift or confess;
    my $xml = shift or confess;
    # This functions works on the %propstat and %prop hashes in
    # _propfindResource that are passed by reference
    my $propstatREF = shift or confess;
    my $propREF = shift or confess;

    # The $xml has children that name the properties we need to
    # return
    my @properties = $xml->nonBlankChildNodes();

    my $parser = XML::LibXML->new();


    foreach my $p (@properties){
        # Is it live? FIXME  Document why this works.
        my ($name, $value) = ($p->localname(), "");
        my $live;
	if(defined($p->namespaceURI()) and
	   $p->namespaceURI() eq "DAV:"){
	    $live = ResourceStore::property_type($resource, $name);
	}else{
	    $live = undef;
	}

	#_LOG("_handle_prop $name '".$p->namespaceURI()."' ");
        if($live eq 'LIVE'){
	    # _LOG("_handle_prop $name is LIVE");
	    my $_a = $LIVE_PROPERTIES{$name};
	    my $fn = $_a->[3];
	    #$LOGLEVEL>2 and _LOG("_readProperty \n\$resource $resource\n\$name  $name\nLIVE");
            defined($fn) and $value = &$fn($resource);
        }else{
            # Because of the way(FIXME: Which way?)  the Perl library
            # handles elements with no namespace it is important to
            # construct the name of the property in the same manner it
            # is constructed by PROPPATCH
            my $name = $p->localName();
            my $ns = $p->namespaceURI();
            defined($ns) or $ns = "";
            my $pfx = $p->prefix();
            defined($pfx) or $pfx = "";
            $name = XML::LibXML::Element->new($name);
            $name->setNamespace($ns, $pfx, 0);
            $value = _readProperty($resource, $name->toString(), "DEAD");
	    #_LOG("_handle_prop $name is  NOTLIVE \$value: '$value'");
        }
	#$LOGLEVEL>2 and _LOG("_handle_prop: \$name $name \$value $value \$live $live \$value ".defined($value)?$value:"<undef>");
        if(defined($value)){

	    # What deos this do?
            _checkProps( 200, $propstatREF, $propREF);

            if(ref($value) eq ''){
                # $value is a scalar.  Is it valid XML
                # string?
                eval{
                    $value = 
                        $parser->parse_string($value);
                    $propREF->{200}->addChild($value->documentElement());
                };
                if($@){
                    # Not valid XML.  Just add it as text
                    # FIXME: Is this ever executed?
                    my $_pNS = defined($p->namespaceURI())?
                        $p->namespaceURI():"";
                    my $_p = $propREF->{200}->addNewChild( 
                        $_pNS, 
                        $p->nodeName());
                    _addNode($_p, $value);
                }
            }elsif(ref($value) eq 'XML::LibXML::Element'){
                # FIXME: Is this code ever reached?  Can I
                # write a test to reach it?
                $propREF->{200}->addChild($value);
            }else{
                die "500 Server Error:Property value is type: '".
                    ref($value)."' which is wrong";
            }
            
        }else{
            # Property not found
            _checkProps( 404, $propstatREF, $propREF);

            if(ref($p) eq "XML::LibXML::Element"){
                $propREF->{404}->addChild($p);
            }else{
                die "500 Server Error:Property value is type: '".
                    ref($p)."' which is wrong";
            }
        }
    }
}

sub _handle_allprop( $$$$ ){
    # handle a <allprop> request in a PROPFIND  method

    my $resource = shift;
    defined($resource) or confess; # Empty string is a valid resource"

    my $xml = shift;
    # Can be an empty string.  Unimplemented.  See comments below
    defined($xml) or confess;  

    # This functions works on the %propstat and %prop hashes in
    # _propfindResource that are passed by reference
    my $propstatREF = shift or confess;
    my $propREF = shift or confess;

    # Return the values of all dead properties and all properties
    # defined in RFC4918/RFC2518.  The $xml may contain an <include>
    # element for additional LIVE properties (described in other
    # RFCs).  As of DAV:1 complience there are none, so it is ignored
    # for now.

    # FIXME This code is duplicated from <propname> handling
    my %properties = _listProperties($resource);

    my @lnames = @{$properties{LIVE}};
    my @dnames =  @{$properties{DEAD}};
    my $parser = XML::LibXML->new();
    
    foreach my $name (@lnames){

        my $node = XML::LibXML::Element->new($name);
        $node->setNamespace($DAV_ns, $DAV_pfx, 1);
        my $value = &{$LIVE_PROPERTIES{$name}->[3]};#, "LIVE");
        if(defined($value)){
            _checkProps(200, $propstatREF, $propREF);
            # Is $value a valid XML string?
            eval{
                $value = 
                    $parser->parse_string($value);
                $propREF->{200}->addChild($value->documentElement());
            };
            if($@){
                # Not valid XML.  Just add it as text in a element
                # named $name.  FIXME  Is this valid by RFC4918?
                my $_p = $propREF->{200}->addNewChild( 
                    $DAV_ns, $name);
                _addNode($_p, $value);
            }
        }else{
            # Property not found
            _checkProps(404, $propstatREF, $propREF);
            # FIXME: Is $p really some times a scalar and
            # sometimes an object?
            $propREF->{404}->addNewChild("$DAV_ns", $name);
        }
    }
}

sub _propfindResource( $$$$ ){

    # Implement PROPFIND for a single resource

    my $principal = shift or confess;
    my $resource = shift;
    defined($resource) or confess; # "" is root collection 
    my $name = shift or confess; 
    my $xml = shift or confess;
    # $LOGLEVEL>2 and _LOG("_propfindResource: \$resource $resource \$name $name");
    if(!_authoriseResource($principal, $resource, "PROPFIND")){
        # Not authorised
        # This is unspecified behaviour.  Another bug in the specification
	$LOGLEVEL > 2 and _LOG("Authorisation failed $principal, $resource, PROPFIND");
        die "403 Forbidden:Cannot access properties on '$resource'";
    }

    # There are possibly many properties for the one resource, LIVE
    # and/or DEAD that are being asked for.  The response must be
    # grouped by status, inside propstat elements.  There is no way to
    # tell which propstat elements are needed before the function has
    # done its work.  So build the propstat elements as needed in this
    # hash and build the response element before this function returns

    my %propstat = ();  # key by status code

    # Inside a <propstat> there is a single <prop> element that holds
    # all the values.  So for each entry in %propstat the underlying
    # <prop> element is in %prop (as well as in the XML tree) keyed by
    # status code
    my %prop = ();  

    if($name eq "propname"){
        _handle_propname($resource, \%propstat, \%prop);
    }elsif($name eq "prop"){
        _handle_prop($resource, $xml, \%propstat, \%prop);
    }elsif($name eq "allprop"){
        _handle_allprop($resource, $xml, \%propstat, \%prop);
    }else{
        die "400 Bad Request:".
            "Invalid child of <propfind> '$name'.  ";
    }

    # Value to return
    my $response = XML::LibXML::Element->new("response");
    $response->setNamespace($DAV_ns, $DAV_pfx, 1);
    my $href = $response->addNewChild("$DAV_ns", "href");
    _addNode($href, _href(_cannonicalResourceName($principal, $resource)));
    foreach my $c (values %propstat){
        $response->addChild($c);
    }

    return $response;
}
sub _handle_DELETE( $$ );
sub _handle_DELETE( $$ ){
    # The recursive version of handle_DELETE

    # Passed principal (arg 1) and resource (arg 2)
    my $principal = shift or confess;
    my $resource = shift;
    defined($resource) or confess; # "" is root collection 

    # If everything goes well then return this; Else an array of error
    # nodes
    my @ret; # = ([204, "No Content"]); # Success is default
    
    # First check if resource is in tables.  If not return 404
    if(!ResourceStore::resource_exists($resource)){
	$LOGLEVEL>2 and _LOG("Error here $resource does not exist");
	@ret = ([404, "Not Found"]);
    }elsif(_isLocked($resource) and _getLock($principal, $resource) != 1){
	$LOGLEVEL>2 and _LOG("Error here $resource.  Is locked and cannot get lock");
        # The root of that which we wish to delete is locked and we
        # cannot get the lock
        @ret = ([423, "Locked"]);
    }elsif(!_isCollection($resource)){
	$LOGLEVEL>2 and _LOG("Not error here $resource");
        # This is a file resource so delete it if we can

        my @_ret = _deleteResource($principal, $resource);
        @ret = (\@_ret);
    }else{
        # We will recursivly delete every resource contained

        # If there are errors in deleting any descendants then this
        # collection ($resource) cannot be deleted.  
	$LOGLEVEL>2 and _LOG("Not error here: $resource is a collection");

        my @descendants = grep{/^$resource\/[^\/]+\/?$/} 
        _getDescendants($resource);
        my @res = ();
        foreach my $_d (@descendants){
            push(@res, _handle_DELETE($principal, $_d));
        }

        # Filter out all results that were OK.  If nothing left every
        # thing is OK
        @res = grep{$_->[0] != 204} @res;
        if(@res){
            # There was an error.  Return error nodes in @ret
	    $LOGLEVEL>2 and _LOG("Error here $resource: ".join("\n", @res));

            @ret = @res;
        }else{
            # There was no error.  Every resource under $resource is
            # deleted so we can delete the collection
	    $LOGLEVEL > 2&&_LOG("_handle_DELETE('$resource')");
            @ret = ResourceStore::delete_resource($resource);
            # # FIXME Assuming there is a 1-1 maping between the
            # # resource hierarchy and file system
            # my $path = _resourceToPath($resource);
            # if(defined($path) and -d $path){ 
            #     if(!(rmdir($path))){
            #         my @_ret = _cannotDeleteError($resource, $!);
            #         @ret = (\@_ret);
            #     }else{
            #         _removeResourceProperty($resource, "LIVE");
            #         _removeResourceProperty($resource, "DEAD");
            #         _removeResourceFromTables($resource);
            #         @ret = ([204, $resource, "", ""]);
            #     }
            # }else{
            #     @ret = ([400, $resource, "Resource: '$resource' is a ".
            #              "collection but not a directory in the file system"]);
            # }
        }
    }
    return @ret;	    
}
# End of resource/property functions
#===================================
# Method Handlers
sub handle_UNLOCK( $ ){
    my $principal = shift or confess; 

    # Unlock $resource.  The lock-token is passed in header 'Lock-Token'
    my @ret = (204, 'No Content');


    eval{
        my $resource = _getResource($principal);
        my $token = _getHeader('Lock-Token');
        $LOGLEVEL > 2 and _LOG("CMD UNLOCK $resource '$token' ");
        defined($token) or  die "400 Bad Request";
        $token =~ s/^<// or die "Token not understood";
        $token =~ s/>$// or die "Token not understood";
        # FIXME Correct status codes
        _authoriseResource($principal, $resource, "UNLOCK") or die "403 Forbidden";
        _resourceExists($resource) or  die "400 Error";
        _removeLock($resource, $token);
        
    };
    if($@){
        my ($status, $message, $error) = (500, "Server Error", '');
        if($@ =~ /(\d{3})\s([^:]+):?/){
            $status = $1;
            $message = $2;
            $error = defined($3)?$3:'';
        }else{
            _LOG($@);
        }
        @ret = ($status, $message, $error);
    }
    return @ret;
}
sub handle_LOCK( $ ){

    # FIXME Comment - What does this return? 

    my $principal = shift or confess; 
    my @ret;
    my $RET = XML::LibXML->createDocument( "1.0", "utf-8" );
    my ($status, $message) = (200, 'OK');
    eval {
        # Get the resource 
        my $resource = _getResource($principal);

        # If resource does not exist in the file system then the
        # status will be 201.  
	if(ResourceStore::resource_exists($resource)){
            $status = 201;
            $message = 'Created';
        }

        # Reset $refresh if this is a new lock and set it if it is a
        # refresh
        my $refresh;

        # Depth defaults to infinity
        my $depth = _getDepth();
        (defined($depth) and $depth) or $depth = 'infinity';

        $LOGLEVEL > 2 and _LOG("CMD LOCK $resource ");


        # 9.10.1.  Creating a Lock on an Existing Resource

        # A LOCK request to an existing resource will create a lock on the
        # resource identified by the Request-URI, provided the resource is not
        # already locked with a conflicting lock.  The resource identified in
        # the Request-URI becomes the root of the lock.  LOCK method requests
        # to create a new lock MUST have an XML request body.  The server MUST
        # preserve the information provided by the client in the 'owner'
        # element in the LOCK request.  The LOCK request MAY have a Timeout
        # header.

	# 5.  Each lock is identified by a single globally unique lock
	#     token (Section 6.5).
        # If this is a refresh it will be read from the If header, if
        # this is asking for a new lock we will create it
        my $token;
        
        # Get XML input
        my ($scope, $type, $owner, $lockroot); 
        
        my $dom = _readXMLInput(); # Can be undefined for a refresh

        # The root node of the XML tree.  Must be DAV::lockinfo.
        # FIXME Check that we cannot refactor this code with code from
	my $has_lock_info = 0;
        if($dom){
            my $node = $dom->documentElement();
            ($node->localName() eq "lockinfo" and 
             $node->namespaceURI() eq $DAV_ns) or 
                die "400 Bad Request:Root node in PROPPATCH named ".
                $node->nodeName();
            my @children = $node->nonBlankChildNodes();
            # RFC4918 14.11 Has 'lockscope' and 'locktype' children,
            # and possibly 'owner'
            foreach my $child(@children){
                $child->namespaceURI() eq $DAV_ns or die "400 Bad Request:".
                    "Invalid XML passed to LOCK: '$dom->toString()' ".
                    "Wrong namespace";
                my $name = $child->localname();
                if($name eq "lockscope"){
                    # If the lock is to be shared or exclusive.  One child...
                    my @_scope = $child->nonBlankChildNodes();
                    scalar(@_scope) == 1 or 
			die "Invald children of lockscope node";
                    $_scope[0]->namespaceURI() eq $DAV_ns or 
                        die "400 Bad Request:Incorrect namespace: '".
                        $_scope[0]->namespaceURI()."' for lockscope child";
                    # Here we get the scope.  We know it must be "shared"
                    # or "exclusive"
                    $scope = $_scope[0]->localName();
                    $scope eq 'shared' or $scope eq 'exclusive' or 
			die "400 Bad Request:Invalid scope '$scope'";
                    if(_isLocked($resource)){

                        # The resource is locked

                        # If  the scope is exclusive it is incompatible
                        $scope eq 'exclusive' and die "423 Locked:";
                        
                        # $scope is shared.  Check there are no
                        # exclusive locks on the resource
                        my @tokens = _getLockTokens($resource);
                        my @locks = map{_getLocks($_)} @tokens;
                        foreach my $l (@locks){
			    # FIXME: Why is this error message in HTML
			    # and others are not?
                            $l->{scope} eq 'exclusive' and 
                                die "423 Locked:<no-conflicting-lock><href>".
                                _href($l->{root}).
				"</href></no-conflicting-lock>";
                        }
                    }	

                }elsif($name eq 'locktype'){
                    # only type is 'write'
                    my @type = $child->nonBlankChildNodes();
                    scalar(@type) == 1 or die "Invald children of type node";
                    $type[0]->namespaceURI() eq $DAV_ns or 
                        die "400 Bad Request:Incorrect namespace: '".
                        $type[0]->namespaceURI()."' for type child";
                    # Here we get the type.  We know it must be "write"
                    $type = $type[0]->localName();
                    $type eq 'write' or
			die "400 Bad Request:Invalid type '$type'";
                }elsif($name eq 'owner'){
                    # This s an XML identifier of the individual
                    # requesting the lock.  It is a dead property (but not
                    # a reqource property) so we just store it as supplied
                    $owner = $child;
                }else{
                    die "400 Bad Request:Bad node '$name' in 'lockinfo'";
                }
            }
            
            #  Create a new token
            $token = _makeLockToken($principal, $resource);

            # Lastly reset $refresh 
            $refresh = 0;

        }# if($dom)
        else{
            # This must be a refresh as no other data sent
            # Lock token must be in If header
            my %ifHeaderRes = _if($principal, $resource);
            
            if(%ifHeaderRes){
                #  RFC4918 section 9.10.2 states that " A lock is
                # refreshed by sending a LOCK request to the URL of a
                # resource within the scope of the lock.  This request
                # MUST NOT have a body and it MUST specify which lock
                # to refresh by using the 'If' header with a single
                # lock token (only one lock may be refreshed at a
                # time).  The request MAY contain a Timeout header,
                # which a server MAY accept to change the duration
                # remaining on the lock to the new value.
                
                # So the If header can have other etags in it, even
                # other lock tokens ("only one lock may be refreshed
                # at a time" does not preclude more resources that ask
                # for refresh).  So we will refresh the lock for the
                # resource passed and the token in the If header

                # Get the locks in the If header for this resource by
                # filtering on 'urn:uuid' that we put at the start of
                # locks.  (We put 'etag:' at start of entity tags)

                my @_hr = grep{/^<urn:uuid/} @{$ifHeaderRes{$resource}};

                # Exactly one lock per resource.  If we get zero or
                # more than one we fail with a user error
                if(@_hr != 1){
                    die "400 Bad Request:".scalar(@_hr).
                        " lock tokens for resource '$resource' in If header";
                }
                # Got the token to refresh
                $token = $_hr[0];
                $token =~ s/^<(.+)>$/$1/ or die "400 Bad Request: Bad lock token: '$_hr[0]'";

                # Get the values we need for the <activelock> tag
                my $_lockData = _getLocks($token) or die "500 Server Error:".
                    "No lock data for refreshing lock '$token' for ".
                    "resource '$resource'";
                my %_lockData = %$_lockData;
                # Get the resource that holds this lock.  Not
                # necessarily the one passed in.  But if not then the
                # reource passed in must be an ancestor, in which case
                # $depth will be 'infinity'
                my ($_res, $depth);
                ($_res, $depth, $scope, $type, $owner, $lockroot) = 
                    ($_lockData{resource},
                     $_lockData{depth},
                     $_lockData{scope},
                     $_lockData{type},
                     $_lockData{owner},
                     $_lockData{root});
                my $_p = $resource;
                if($_res ne $resource){
                    # If this is valid then $_res is an ancestor of
                    # $resource and is the locked resource we need to
                    # refresh
                    do{
                        $_p = _getParentCollection($_p);
                        if($_p eq ''){
                            # Cannot find $_res in ancestors of $resource
                            die "400 Bad Request: Bad lock token: '$token'.  The token locks '$_res' which is not an ancestor of '$resource'";
                        }
                        $_res eq $_p and $resource = $_res;
                    }while($_res ne $resource);
                }
            }else{
                
                die "412 Precondition Failed:".
                    "lock-token-matches-request-uri Resource: '$resource'";
            }
            $refresh = 1;
        }

        # Get the time out if one is sent.  Is sent as header

        # Time out.  we are allowed to override the time out requested
        # by the client. FIXME One day we need a way of creating a
        # policy for time outs but for now we accept any time out
        # passed by client if it is a number (we say that is seconds).
        # If they pass 'Infinite' I will let them have a week.  If
        # there is no time out passed then they can have a day.
        # Totally arbitrary.  Only paying attention to the first
        # Timeout
        my $timeoutH = _getHeader('Timeout');
        my $timeout = undef;
        if(!defined($timeoutH)){
            # Passed  no timeut
            $timeout = 24*60*60; # FIXME This should be a configurable global
            $timeoutH = "Second-$timeout"; # We pass this back
        }elsif($timeoutH =~ /^Second-(\d+)/){
            #  Number passed
            $timeout = $1;
        }elsif($timeoutH eq 'Infinite'){
            $timeout = 7*24*60*60; # FIXME This should be a
            # configurable global
        }else{
            die "400 Bad Request:Timeout header '$timeout' badly formated";
        }

        # Process depth:

        # If the Depth header is set to infinity, then the resource
        # specified in the Request-URI along with all its members, all
        # the way down the hierarchy, are to be locked.  A successful
        # result MUST return a single lock token.  Similarly, if an
        # UNLOCK is successfully executed on this token, all
        # associated resources are unlocked.  Hence, partial success
        # is not an option for LOCK or UNLOCK.  Either the entire
        # hierarchy is locked or no resources are locked.

        # We store a depth infinity lock on a collection and all its
        # resources with one record for the collection that includes
        # its depth

        # If the lock cannot be granted to all resources, the server
        # MUST return a Multi-Status response with a 'response'
        # element for at least one resource that prevented the lock
        # from being granted, along with a suitable status code for
        # that failure (e.g., 403 (Forbidden) or 423 (Locked)).
        # Additionally, if the resource causing the failure was not
        # the resource requested, then the server SHOULD include a
        # 'response' element for the Request-URI as well, with a
        # 'status' element containing 424 Failed Dependency.


        # If no Depth header is submitted on a LOCK request, then the
        # request MUST act as if a "Depth:infinity" had been
        # submitted.
        my @errors = (); # Collect errors about the resources we
        my @resourcesToLock = ($resource);
        if(!$refresh){
            $depth eq '0' or $depth eq 'infinity' or 
                die "400 Bad Request:Depth: '$depth' not understood";
            # cannot lock in here
            if($depth eq 'infinity'){
                # Check that we can lock all children
                push(@resourcesToLock, _getDescendants($resource));
                foreach my $_resource (@resourcesToLock){
                    my $msg = _cannotLock($principal, $_resource, $scope, 
                                          $token);
                    if($msg){
                        # Failed to lock the $resource.  Recover....
                        push(@errors, [$resource, $msg]); #@ FIXME  Should this be $_resource?
                    }
                }
            }

        }	

        
        if($refresh or !@errors){
            # Grant the lock

            _lock($principal, $resource, $resource, $scope,
                  $type, $token, $depth, $owner,
                  $timeout, $refresh);

            # Build XML response  RFC4918 9.10.1
            my $root = $RET->createElement("prop");
            $root->setNamespace($DAV_ns, $DAV_pfx, 1);
            $RET->setDocumentElement( $root );
            my $lockdiscovery = _lockdiscovery($resource); #$root->addNewChild($DAV_ns, 'lockdiscovery');
            my $activeLock = $lockdiscovery->addNewChild($DAV_ns, 'activelock');
            my $scopexml = $activeLock->addNewChild($DAV_ns, 'lockscope');
            
            $scope eq 'shared' or $scope eq 'exclusive' or 
                confess "Do not know what scope: '$scope' is";
            $scopexml->addNewChild($DAV_ns, $scope);

            my $typexml = $activeLock->addNewChild($DAV_ns, 'locktype');
            $type eq 'write' or 
                confess "Do not know what type: '$type' is";
            $typexml->addNewChild($DAV_ns, $type);

            if(defined($depth)){
                # A lock refresh ignores depth RFC4918 9.10.2
                my $depthxml = $activeLock->addNewChild($DAV_ns, 'depth');
                _addNode($depthxml, $depth);
            }

            my $lockrootxml = $activeLock->addNewChild($DAV_ns, 'lockroot');
            my $href = $lockrootxml->addNewChild("$DAV_ns", "href");
            # In a refresh we read the lockroot from the records, if
            # it is new $lockroot will be undefined so it is $resource
            _addNode($href, _href(defined($lockroot)?$lockroot:$resource));

            # FIXME This is debugging code
            my $ownerxml = $activeLock->addNewChild($DAV_ns, 'owner');
            _addNode($ownerxml, 'litmus test suite');

            my $timeoutxml = $activeLock->addNewChild($DAV_ns, 'timeout');
            _addNode($timeoutxml, $timeoutH);

            my $tokenxml = $activeLock->addNewChild($DAV_ns, 'locktoken');
            my $tokhtml = $tokenxml->addNewChild($DAV_ns, 'href');
            _addNode($tokhtml, $token);
        }else{
            # We cannot grant the lock

            # Build XML response  RFC4918 9.10.3
            my $root = $RET->createElement("multistatus");
            $root->setNamespace($DAV_ns, $DAV_pfx, 1);
            $RET->setDocumentElement( $root );

            # For each error let the user know why.  But we also check
            # to see if the main resource has an error reported and if
            # not we add a <response> for it with a status of "424
            # Failed Dependency"
            my $resFound = 0;
            foreach my $err (@errors){
                scalar(@$err) == 2 and defined($err->[0]) and defined($err->[1]) 
                    or confess "500 Server Error: \$resource: '$resource' "
                    ."Do not understand error locking";
                my ($res, $msg) = @$err;
                $msg =~ /^(\d{3}) ([^\:]+):(.*)\s*$/ or 
                    confess "500 Server Error: \$msg '$msg' not understood";
                my ($_status, $_message, $_explaination) = ($1, $2, $3);

                $res eq $resource and $resFound = 1;
                my $response = $root->addNewChild($DAV_ns, "response");
                #$response->setNamespace($DAV_ns, $DAV_pfx, 1);
                my $href = $response->addNewChild("$DAV_ns", "href");
                _addNode($href, _href($res));
                my $statusxml = $response->addNewChild("$DAV_ns", "status");
                _addNode($statusxml, "HTTP/1.1 $_status $_message");
            }
            # If $resource was not in errors add it
            if(!$resFound){

                my $response = $root->addChild("response");
                #$response->setNamespace($DAV_ns, $DAV_pfx, 1);
                my $href = $response->addNewChild("$DAV_ns", "href");
                _addNode($href, _href($resource));
                my $statusxml = $response->addChild("status");
                _addNode($statusxml, "HTTP/1.1 424 Failed Dependency");
            }
            $status = 207;
            $message = "Multi-Status";
        }
        @ret = ($status, $message, $RET, $token);

    };
    if($@){
        my ($status, $message) = (500, "Server Error");
        if($@ =~ s/(\d{3})\s([^:]+)://){
            $status = $1;
            $message = $2;
        }else{
            _LOG($@);
        }
        @ret = ($status, $message, $RET);
    }
    $ret[2] and $ret[2] =~ s/[\n\r]//g; # FIXME  Is this necessary?   Yuck!
    return @ret;
}
sub handle_MOVE( $ ){
    my $principal = shift or confess; 

    my @ret;

    eval {
        # Source is the resource
        my ($source, $destination) = _preambleCopyMove($principal);
        my $collection = _isCollection($source);
        
        if($collection){
            # Section 9.9.2 of RFC4918
            my $depth = _getDepth();
            (defined($depth) and $depth =~ /\S/) or $depth = "infinity";
            $depth eq "infinity" or 
                die "400 Bad Request:  A client MUST NOT submit a ".
                "Depth header on a   MOVE on a collection with any value ".
                "but \"infinity\" It is '$depth'";
            @ret = _copyCollection($principal, $source, $destination);
        }else{
            # MOVE a non-collection
            my $ret = _copyResource($principal, $source, $destination);
            @ret = ($ret);
        }
        # The results of the COPIES are all in @ret

        # Parse the return to see if there were any errors.  If so we
        # have collections that we must not delete
        my @_errors = grep{$_->[1] !~ /^20[14]/} @ret;

        # There cannot be more errors than results
        (@ret >= @_errors) or die "Reality interupt!";

        # Hold results from DELETEs to examine for errors.  @ret is
        # set by the MOVE and unless there are errors is not
        # influenced by the delete
        my @_tst = (); 

        if(@_errors and  @ret > 1){
            # There is at least one error and more than one in @res.
            # So list all the collections under $source and if they
            # match an error in @_errors do not delete that
            # collection.  Delete the others
            my @collections = grep{_isCollection($_)} _listCollection($source);
            my @delRet = ();
            while( my $c = shift @collections){
                my $f = 1;
                map{$_->[0] =~ /^$c/ and $f = 0} @_errors;
                if($f){
                    # $c is not an ancestor of a collection we could
                    # not copy, so we can delete it
                    push(@delRet, _handle_DELETE($principal, $c));
                    # Remove all descendants of $c in @collections
                    @collections = grep{$_ !~ /^$c/} @collections;
                }
            }

            # Store any errors that we got from _handle_DELETE
            push(@_tst, grep{$_->[1] !~ /^20[14]/} @delRet);

        }elsif(@ret and $ret[0]->[1] =~ /^20[14]/){

            # No errors so do delete...
            @_tst = _handle_DELETE($principal, $source);
        }

        # The deleteing of the source side is done.  Check for errors
        my @_errors_del = grep{$_->[0] !~ /^20[14]/} @_tst;

        # There cannot be more errors than results
        (@_tst >= @_errors_del) or die "Reality interupt!";
        
        # Combine the errors from the COPY (@_errors) with the errors
        # from the DELETE (@_errors_del) for reporting a 207
        push(@_errors, @_errors_del);

        # Combine the returns from the COPY (@res) and DELETE (@_tst)
        # operations for reporting 207
        push(@ret, @_tst);

        if(@_errors and  @ret > 1){
            # There is at least one error in the COPY or DELETE
            # operations and more than one in @_tst.  Make a 207
            # return.

            @ret = _207COPY_MOVE(\@_errors, $source);
        }elsif(@_errors == 1 or @ret){
            # One error (implies @ret <= 1) or more than zero results
            # (implies @_errors == 0).  Output $res[0] that is the
            # error or the result of the first container move FIXME Is
            # this correct?
            @ret = @{$ret[0]};

            # FIXME This is out of control.  When is the $source at
            # the front of @ret and when not?
            $ret[0] eq $source and 
            $ret[0] eq $source and 
                shift(@ret); # Extra parameter, $source, at the front

            # FIXME Another reason we are out of control.  If $ret[0]
            # is not 207 the caller insists on two parameters exactly
            # in the returned array
            scalar(@ret) > 1 or 
                die "500 Server Error:Too few elements in \@ret";
            @ret =  ($ret[0], $ret[1]);
        }else{
            die "Do not understand move result";
        }

        
    };
    if($@){
        my ($code, $message) = (500, "Server Error");
        if($@ =~ s/(\d{3})\s([^:]+)://){
            $code = $1;
            $message = $2;
        }else{
            _LOG($@);
        }
        @ret = ($code, $message);
    }
    return @ret;
}

sub handle_COPY( $ ){
    my $principal = shift or confess; 
    my @ret;

    eval {
        # Source is the resource
        my ($source, $destination) = _preambleCopyMove($principal);
        my $collection = _isCollection($source);

        # RFC4918 Section 9.8.3
        my $depth = _getDepth();
        (defined($depth) and $depth =~ /\S/) or $depth = "infinity";

        if($collection and $depth eq "0"){
            # The COPY method on a collection without a Depth header
            # MUST act as if a Depth header with value "infinity" was
            # included.  A client may submit a Depth header on a COPY
            # on a collection with a value of "0" or "infinity".
            # Servers MUST support the "0" and "infinity" Depth header
            # behaviors on WebDAV-compliant resources.

            # A COPY of "Depth: 0" only instructs that the collection
            # and its properties, but not resources identified by its
            # internal member URLs, are to be copied.

            # This will not do a resursive copy
            my $ret = _copyResource($principal, $source, $destination);
            @ret = ($ret);
        }elsif($collection and $depth eq "1"){
            # $depth cannot be 1 for a collection
            die "400 Bad Request: ".
                "COPY a collection with a depth of 1 is not allowed";
        }elsif(!$collection or $depth eq "infinity"){
            # Either copying a normal resource or a collection tree
            @ret = _copyCollection($principal, $source, $destination);

        }else{
            die "COPY '$source' (which is ".($collection?"":" not ").
                " a collection to '$destination' with a depth of '$depth' ".
                "failed for some unknown reason";
        }

        # Parse the return to see if there were any errors.  If so we
        # have to return multi-status, if not we return $ret[0]
        my @_errors = grep{$_->[1] !~ /^20[14]/} @ret;

        # There cannot be more errors than results
        (@ret >= @_errors) or die "Reality interupt!";

        if(@_errors and  @ret > 1){
            # There is at least one error and more than one in @res.
            # Make a 207 return. (@ret >= @_errors)
            @ret = _207COPY_MOVE(\@_errors, $source);
        }elsif(@_errors == 1 or @ret){
            # One error (implies @res == 1) or more than zero results
            # (implies @_errors == 0).  Output $res[0] that is the
            # error or the result of the first container copy FIXME Is
            # this correct?
            @ret = @{$ret[0]};
            shift(@ret); # Extra parameter, $source, at the front
        }else{
            die "Do not understand copy result";
        }
    };
    if($@){
        my ($code, $message) = (500, "Server Error");
        if($@ =~ s/(\d{3})\s([^:]+)://){
            $code = $1;
            $message = $2;
        }else{
            _LOG($@);
        }
        @ret = ($code, $message);
        # FIXME  Check for the case of running out of disc space
        # if(_tooLittleDiscSpace()){
        #     @ret = (507, "Insufficient Storage", $@);
        # }
    }

    return @ret;
}



sub handle_MKCOL( $ ){
    my $principal = shift or confess; 

    # Default return values
    my $code = 201;
    my $message = "Created";
    
    eval {
        # Get the resource 
        my $resource = _getResource($principal);

        if(_resourceExists($resource)){
            # If the Request-URI is already mapped to a resource, then
            # the MKCOL MUST fail. RFC4918 Section 9.3
            # Respond with 403 (Forbidden)
            die "405 Method Not Allowed: Resource Exists";
        }elsif(!_ancestorsExist($resource)){
            # If the ancestors of the resource do not all exist (and
            # are collections), then the MKCOL MUST fail with a 409
            # (Conflict) status code. RFC4918 Section 9.3
            die "409 Conflict: Not All Ancestors Exist";
        }
        my $content = _readSTDIN();

        if($content =~ /\S/){
            # A MKCOL request message may contain a message body.  The
            # precise behavior of a MKCOL request when the body is
            # present is undefined, but limited to creating
            # collections, members of a collection, bodies of members,
            # and properties on the collections or members.  If the
            # server receives a MKCOL request entity type it does not
            # support or understand, it MUST respond with a 415
            # (Unsupported Media Type) status code.  If the server
            # decides to reject the request based on the presence of
            # an entity or the type of an entity, it should use the
            # 415 (Unsupported Media Type) status code.

            # I do not accept bodies.  Keep It Simple Stewart
            
            # Fail with 415 (Unsupported Media Type) status code.
            die "415 Unsupported Media Type:MKCOL does not support content: '$content'";
        }

        # Check that the user is authorised to access the parent
        # collection
        my $parent = _getParentCollection($resource);
        $parent or die "409 Conflict:".
            "Parent collection of '$resource' does not exist";
        if(!_authoriseResource($principal, $parent, "MKCOL")){
            die "403 Forbidden";
        }
        # Check the parent is not locked

        if(_isLocked($parent)){
            die "423 LOCKED";
        }	    

        # Time has come to create the collection
        ResourceStore::create_collection($resource);
    };
    if($@){
        ($code, $message) = (500, "Server Error");
        if($@ =~ s/(\d{3})\s([^:]+)://){
            $code = $1;
            $message = $2;
        }else{
            #_LOG($@);
        }
	_LOG($@);
    }
    my @ret =  ($code, $message);
    return @ret;
}
sub handle_PROPPATCH( $ ){

    # According to RFC4918 PROPPATCH patches dead properties.  It says
    # nothing about live propertues, but since they are by definition
    # set by the server here only dead properties will be handled

    # According to Dusseault (page 152) properties may or may not be
    # XML.

    # Looking at the structure of PROPPATCH the property will always
    # be XML with the root element being the property name.
    
    # FIXME handle_PROPPATCH and handle_PROPFIND handle error
    # conditions diferently.  One returns directly the other by
    # "die"ing and returning from handler if $@

    my $principal = shift or confess; 
    my $DOC = XML::LibXML->createDocument( "1.0", "utf-8" );

    my $resource = _getResource($principal);

    # If the resource does not exist return a 404.  This behaviour
    # is not specified in rfc4918 bu clients expect it.  It is
    # reasonable too and may well be specified by other rfcs....
    if(!_resourceExists($resource)){
        return(404, "Not Found");
    }

    # Check that the principal is autherised to use PROPPATCH on this
    # resource
    if(!_authoriseResource($principal, $resource, "PROPPATCH")){
        # Get out now with an error
        return (403, "Forbidden");
    }

    if(_isLocked($resource)){
        my $_locked = _getLock($principal, $resource);
        if($_locked == -1){
            # Cannot PROPPATCH a locked resource
            if($_locked == -1){
                # FIXME Bad style returning here, and there needs to
                # be a body(?)
                return (423, "Locked");
            }elsif($_locked == 0){
                return (412, "Precondition Failed");
            }
        }
    }
    my $dom = _readXMLInput();

    # Deprecating XPathContext
    # my $xc = XML::LibXML::XPathContext->new($dom);
    # $xc->registerNs('D', 'DAV:');

    # XML to return
    my $xml = XML::LibXML::Element->new("$DAV_pfx:multistatus");
    $xml->setNamespace($DAV_ns, $DAV_pfx, 1);

    # RFC 4918 section 9.2 says: Instructions MUST either all be
    # executed or none executed.  Thus, if any error occurs during
    # processing, all executed instructions MUST be undone and a
    # proper error result returned.
    
    # So we store the actions up, then do them all at once ready to
    # undo them
    my @actions = ();

    # For the names of properties in the returned XML we need to get
    # the namespaces right.  Oh my aching head!!  So for each action
    # push the namespace and prefix tab delimited into this...
    my @namespaces = ();


    # If we delete or over write a value (the root elements are the
    # same) put the deleted or over ridden data into @changed so if
    # there is an error it can be restored
    my @changed = ();

    # Set up return XML to use if there are no fatal errors
    my $response = $xml->addNewChild($DAV_ns, "response");
    my $href = $response->addNewChild($DAV_ns, "href");
    _addNode($href, _href($resource)); 
    # FIXME <status> element

    # Do a lot of XML...
    my $parser = XML::LibXML->new();

    eval {
        my $node = $dom->documentElement();
        ($node->localName() eq "propertyupdate" and 
         $node->namespaceURI() eq $DAV_ns) or 
            die "400 Bad Request:Root node in PROPPATCH named ".
            $node->nodeName();
        my @children = $node->nonBlankChildNodes();
        foreach my $child(@children){
            $child->namespaceURI() eq $DAV_ns or die "400 Bad Request:".
                "Invalid XML passed to PROPPATCH: '$dom->toString()' ".
                "Wrong namespace";
            my $name = $child->localname();
            if($name eq "remove"){
                # Properties to remove. $child will have exactly one child
                # node, a <D:prop> node, and it can have arbitrarily many
                # child nodes (at least one) each is empty, it just names
                # a property to delete
                my @prop = $child->nonBlankChildNodes();
                scalar(@prop) == 1 or die "Invald children of remove node";
                my @_prop = $prop[0]->nonBlankChildNodes()->get_nodelist();
                # Translate the namespace prefixes
                my @names = ();
                foreach my $prop (@_prop){
                    #my $name = $prop->nodeName();
                    my $pfx = defined($prop->prefix())?$prop->prefix():"";
                    my $ns = defined($prop->namespaceURI())?
                        $prop->namespaceURI():"";

                    # Make $prop self contained
                    $prop->setNamespace($ns, $pfx, 0); 

                    push(@names, $prop);
                    push(@namespaces, "$pfx\t$ns"); # Why?
                }

                # Put these into the @actions
                push(@actions, map{"Delete:".$_->toString()} @names);
                scalar(@actions) == scalar(@namespaces) 
                    or die "There are a different number of namespaces ".
                    "than actions";
            }elsif($name eq "set"){
                # Properties to set.  $child will have exactly one child
                # node, a <D:prop> node and each element in there will
                # contain the name of the property 

                my @prop = $child->nonBlankChildNodes();
                scalar(@prop) == 1 or die "Invalid children of set node";
                my @_prop = $prop[0]->nonBlankChildNodes()->get_nodelist();

                # Translate the namespace prefixes
                my @names = ();
                my @values = ();
                foreach my $prop (@_prop){
                    # FIXME This is a very roundabout way of reading
                    # the input XML and converting it to text then
                    # back to XML and then it will be converted to
                    # text to be in the file
                    my $name = $prop->localname();
                    my $pfx = $prop->prefix();
                    defined($pfx) or $pfx = "";
                    my $ns = $prop->namespaceURI();
                    defined($ns) or $ns = "";
                    #$name =~ s/^$pfx/$ns/;
                    $name = XML::LibXML::Element->new($name);
                    $name->setNamespace($ns, $pfx, 0);
                    push(@names, $name);
                    push(@namespaces, "$pfx\t$ns");
                    
                    # Make $prop self contained
                    $prop->setNamespace($ns, $pfx, 0); 

                    push(@values, $prop);
                }


                # FIXME Are the name are in the values?  At least some times
                # FIXME Write a test for this.  How????
                scalar(@values) == scalar(@names) 
                    or die "There are a different number of names and values";

                # Join up names and values
                my @nv = map{$names[$_]->toString().
                                 "\t".$values[$_]} 0..$#values;

                # Put these into the @actions
                push(@actions, map{"Set:$_"} @nv);
                
                scalar(@actions) == scalar(@namespaces) 
                    or die "There are a different number of namespaces than actions"
            }else{
                die "Unknown child of D:propertyupdate: '$name'";
            }
        }

        # @actions contains the actions on the properties that we have to
        # do
        
        # For actions that succeed put the data to undo them in here.  If
        # we fail we stop and use this to undo the actions
        my @undo = ();

        # After the loop if $i == scalar(@actions) then we have completed
        # all actions.  If not we have to undo all the actions in @undo
        my $i = 0; 

        my $fh = _lockProperties("DEAD");

        # We are allowed only one error. Store the message and the
        # property name here
        my $errorMsg = undef;
        my $errorProperty = undef;

        for(; $i < @actions; $i++){
            my $action = $actions[$i];
            if($action =~ /^Set:([^\t]+)\t(.+)$/){
                eval{
                    my $deleted = _addPropertyf($resource, $1, $2, $fh);
                    my $_undo = "Set:$1\t";
                    $_undo .= defined($deleted)?$deleted:"";
                    push(@undo, $_undo);
                };
                if($@){
                    # Failed.  That property was not added
                    # Break out of loop to undo 
                    _LOG($@);
                    last;
                }
            }elsif($action =~ /^Delete:(.+)$/){
                eval{
                    my $deleted = _deletePropertyf($resource, $1, $fh);

                    # If we attempted to delete a property that did
                    # not exist then we have succeeded, but we have
                    # nothing to undo
                    if(defined($deleted)){
                        push(@undo, "Delete:$1\t$deleted");
                    }
                };
                if($@){
                    # Failed.  That property was not deleted
                    # Break out of loop to undo 
                    _LOG($@);
                    last;
                }
            }else{
                die "Unknown action: '$action'";
            }
        }
        close($fh) or die $!;

        if($i != @actions){
            # Failed, so undo 
            scalar(@undo) == $i or die "Wrong number of undo actions: ".
                scalar(@undo)." in undo, \$i is $i";

            while(my $action = pop(@undo)){
                if($action =~ /Set:([^\t]+)\t(.*)$/){
                    if(defined($2)){
                        # A property was replaced
                        _addProperty($resource, $1, $2, "DEAD");
                    }else{
                        # A new property was added 
                        _deleteProperty($resource, $1, "DEAD");
                    }
                }elsif($action =~ /Deleted:([^\t]+)\t(.*)$/){
                    if(defined($2)){
                        # A property was successfully deleted
                        _addProperty($resource, $1, $2, "DEAD");
                    }
                }else{
                    # Reality has taken a holiday!  This cannot happen...
                    die "Action: '$action' makes no sense";
                }
            }

            # For the response the action that failed needs its own
            # <propstat> element with the reason it failed.  The rest
            # of the property names go in a <propstat> with a "424
            # Failed Dependency"
            defined($errorMsg) or die "500 Server Error:\$errorMsg not defined and PROPPATCH failing";
            defined($errorProperty) or die "500 Server Error:\$errorProperty not defined and PROPPATCH failing";
            $errorMsg =~ /^(\d{3}\s[^\:]+)/ or die "500 Server Error:\$errorMsg '$errorMsg' is wrong format";
            my $msg = $1;
            my $propstat;

            $propstat = $response->addNewChild($DAV_ns, "propstat");
            my $status = $propstat->addNewChild($DAV_ns, "status");
            _addNode($status, "HTTP/1.1 $msg");
            my $prop = $propstat->addNewChild("", "prop");	
            $prop->setNamespace($DAV_ns, $DAV_pfx, 1);

            eval{
                $errorProperty = $parser->parse_string($errorProperty)->documentElement();
            };
            if($@){
                die "500 Server Error:\$errorProperty '$errorProperty' ".
                    "not valid XML.  '$@'";
            }
            $prop->addChild($errorProperty);	
            
            # $actions[$i] is the action that failed.  (FIXME: Why did
            # I need $errorProperty then?).  So get all the other
            # properties from @actions missing $actions[$i]
            $propstat = $response->addNewChild($DAV_ns, "propstat");
            $status = $propstat->addNewChild($DAV_ns, "status");
            _addNode($status, "HTTP/1.1 424 Failed Dependency");
            for(my $j = 0; $j < @actions; $j++){
                $j == $i and next; # Skip failed property
                my $prop = $propstat->addNewChild("", "prop");	
                $prop->setNamespace($DAV_ns, $DAV_pfx, 1);
                $actions[$j] =~ /^[a-zA-Z]+:([^\t]+)/ or die "500 Server Error:\$actions[\$j] '$actions[$j]' is wrong format";
                
                eval{
                    $errorProperty = $parser->parse_string($1)->documentElement();
                };
                if($@){
                    die "500 Server Error:\$errorProperty '$errorProperty' not valid XML.  '$@'";
                }
                $prop->addChild($errorProperty);	
            }
            
        }else{
            # Success
            # Make a <propstat> for each change we had to make
            
            # For each status code put the propstats in here
            my %propstat = ();

            foreach my $i (0..$#actions){
                my $a = $actions[$i];
                my $ns = $namespaces[$i];

                $ns !~ /\S/ or  # Empty namespace is valid
                    $ns =~ /^(\S*)\t(\S+)$/ or
                    die "Invalid namespace: '$ns'";
                my $pfx = defined($1)?$1:"";
                my $namespace =  defined($2)?$2:"";

                $a =~ /^[^:]+:([^\t]+)\t?/ or die "Unknown action: '$a'";
                
                _checkProps(200, \%propstat);
                my $prop = $propstat{200}->addNewChild($DAV_ns, "prop");	
                $namespace and
                    $prop->setNamespace($namespace, $pfx, 0);
                my $value = $parser->parse_balanced_chunk($1);
                $prop->appendChild($value);	
            }
            # Asemble the <D:propstat> elements in the <D:response>
            foreach my $p (values %propstat){
                $response->addChild($p);
            }
        }
    };
    my @_ret;
    if($@){
        if($@ =~ /^(\d{3})\s([^\:]+):(.*)/){
            @_ret = ($1, $2, $3);
        }else{
            _LOG($@);
            @_ret = (500, "Server Error", $@);
        }
    }else{
        @_ret = (207, "multi-status", $xml);
    }
    return @_ret;

}
sub handle_PROPFIND( $ ){
    # Section 7.2 Dusseault
    # Section  9.1 RFC 4918

    # Returns a <multistatus> element

    my $principal = shift or confess; 
    #_LOG("PROPFIND \$principal '$principal'");

    # The return value
    my $xml = XML::LibXML->createDocument( "1.0", "utf-8" );
    my $root = $xml->createElement("multistatus");
    $root->setNamespace($DAV_ns, $DAV_pfx, 1);
    $xml->setDocumentElement( $root );

    # Fill this with all the uesource URLs we will use.  Declared
    # outside the eval block to facilitate debugging a end of function
    my @resources = ();
    eval {

        # The root resource  of the PROPFIND
        my $resource = _getResource($principal);

	#$LOGLEVEL>2 and _LOG("Resource: $resource:");
        # If the resource does not exist return a 404.  This behaviour
        # is not specified in rfc4918 bu clients expect it.  It is
        # reasonable too and may well be specified by other rfcs....
        if(!_resourceExists($resource)){
            die "404 Not Found:\$resource '$resource' cannot be found on server";
        }

        # Depth controls how many child resources we retrieve
        # properties for
        my $depth = _getDepth();
        (defined($depth) and $depth =~ /\S/) or $depth = "infinity";
        #defined($depth) or $depth = "infinity";

        # Using depth calculate which resources we are getting
        # properties for.  DEPTH != 0 for a non-collection resource is
        # interpreted as DEPTH 0
        @resources = ($resource);
        if(_isCollection($resource)){
            if($depth eq "1"){
                # Get the immediate children
                push(@resources, _listCollection($resource, 1));
            }elsif($depth eq "infinity"){
                # All children
                push(@resources, _listCollection($resource));
            }elsif($depth eq "0"){
                # Do nothing as we have $reource in @resources
            }else{
                die "400 Bad Request:Invalid DEPTH header: '$depth'";
            }
        }

        # Get the PROPFIND command
        my @children = (); # The propfind commands
        my $dom = _readXMLInput(); # What the client sent
	$LOGLEVEL>2 and _LOG("PROPFIND $resource ".$dom->toString());
        if(defined($dom)){
            my $docE = $dom->documentElement();
            unless($docE->localname() eq "propfind" 
                   and $docE->namespaceURI eq $DAV_ns){
                die "400 Bad Request:".
                    "Invalid XML '$dom->toString()'.  Wrong root element";
            }
            @children = $docE->nonBlankChildNodes();
        }
        #    RFC4918 Section 9.1: A client may choose not to submit a
        #    request body.  An empty PROPFIND request body MUST be
        #    treated as if it were an 'allprop' request.

        if(@children == 0){
            my $ap = XML::LibXML::Element->new("allprop");
            $ap->setNamespace($DAV_ns, $DAV_pfx, 1);
            push(@children, $ap);
        }

        # For <propname> and <prop> there is just one child of
        # <propfind>.  For <allprop> there can be an <include> element


        if(@children == 1){
            # Can be any of the three....
            my $child = $children[0];
            # FIXME The namespace could be wrong.  It should be
            # checked (and tested with incorrect namespaces)
            my $name = $child->localname();
	    #$LOGLEVEL > 2 and _LOG("handle_PROPFIND \$principal $principal  \$name $name \$child $child");
            foreach my $r (@resources){
                my $response = _propfindResource($principal, $r, $name, $child);
		$LOGLEVEL > 2 and _LOG(" \$r $r \$response: ".$response->toString());
		$root->appendChild($response);
            }
        }elsif(@children == 2){
            # <allprop> and <include>
            my $child0 = $children[0];
            my $name0 = $child0->localname();

            my $child1 = $children[1];
            my $name1 = $child1->localname();

            my ($arg1, $arg2, $arg3); 
	    $LOGLEVEL > 2 and _LOG("handle_PROPFIND \$child0 $child0 \$name0 $name0  \$child1 $child1 \$name1 $name1"); 
            if($name0 eq "allprop"){
                ($arg1, $arg2) = ($name0, $child1);
            }elsif($name1 eq "allprop"){
                ($arg1, $arg2) = ($name1, $child0);
            }else{
                die "400 Bad Request:Invalid XML: '$dom->toString()'";
            }

            # Collect the properties one resource at a time
            foreach my $r (@resources){
		$LOGLEVEL > 2 and _LOG("handle_PROPFIND \$principal $principal \$r $r \$arg1 $arg1 \$arg2 $arg2");
                my $response = 
                    _propfindResource($principal, $r, $arg1, $arg2);
                $root->appendChild($response);
            }
        }else{
            die "400 Bad Request:Invalid XML: '$xml'";
        }	    
    };

    my @ret;
    if($@){
        @ret = (500, "Server Error", $@);
        if($@ =~ /^(\d{3})([^:]+):?(.*)/){
            # We have detected a user error
            @ret = ($1, $2, $3);
	    $LOGLEVEL>2 and _LOG("User error: ".join("\n", @ret));
        }else{
            _LOG($@);
        }
    }else{
        @ret = (207, "multi-status", $xml);
    }
    return @ret;
}
sub handle_PUT( $ ){
    my $principal = shift or confess; 

    # Default message to return
    my ($code, $message);# = (201, "Created");


    eval {

        # Get the resource from the URL.  Note there is a many to one
        # mapping from URLs to resources.  Also the resource may
        # depend on the principal

        my $resource = _getResource($principal);

        !_authoriseResource($principal, $resource, "PUT") 
            and die "403 Forbidden:Not authenticated";

        # Check for If headers
        my %_if = _if($principal, $resource);
        if(!$_if{'_if'}){
            # Failed a conditional
            die "412 Precondition Failed";
        }

        if(_isLocked($resource)){
            my $_locked = _getLock($principal, $resource);
            if($_locked == -1){
                # Section 11.3 RFC4918
                die "423 Locked:Failed to get write lock for '$resource'";
            }elsif($_locked == 0){
                die "412 Precondition Failed:Failed to get write lock for '$resource'";
            }
        }

        # Either not locked or could get lock

        # If $url is to a collection then we fail as that is
        # undefined
        _isCollection($resource) and 
            die "405 Method Not Allowed: It is a collection";

        # If the parent collection does not exist fail (Section 9.7.1
        # RFC4918) FIXME Is this correct?
        _getParentCollection($resource) or die 
            "409 Confilict:Resource:".
            "'$resource' is not part of an existing collection";
        
	# If resource does not exist create it
	if(!ResourceStore::resource_exists($resource)){
	    _authoriseResource($principal, $resource, "CREATE") or
                die "401 Unauthorised:Failed authentication for '$resource'";
	    # There are two live properties that are not "MUST
	    # PROTECTED" and can be best set when the file arrives in
	    # a PUT.  They are: contentlanguage and contenttype

	    $LOGLEVEL > 2 and _LOG("");
	    
	    my $content_language = _getHeader("Content-Language"); 
	    $content_language or $content_language = "en-UK";

	    $LOGLEVEL > 2 and _LOG($ENV{CONTENT_TYPE});
            _createResource($resource, {getcontentlanguage=>$content_language}) or
                die "500 Server Error:$! Could not create resource '$resource'";
        }
	($code, $message) = ResourceStore::put($resource);

    };
    if($@){
        if($@ =~ /^([^:]+):?/){
            my $error = $1;
            if($error =~ /^(\d{3})\s*(.+)$/){
                $code = $1;
                $message = $2;
            }else{
                $message = "Server Error";
                $code = 500;
                _LOG($@);
            }
        }
        if($code == 423){
            # FIXME See section 11.3 of RFC4918
        }
    }
    return ($code, $message);
}

sub handle_DELETE( $ ){
    my $principal = shift or confess; 

    # RFC2616, Section 9.7: to "delete the resource identified by the
    # Request-URI".

    # Section 9.7 of RFC4918: A server processing a successful DELETE
    # request: MUST destroy locks rooted on the deleted resource MUST
    # remove the mapping from the Request-URI to any resource.

    # Section 9.6.1 of RFC4918: DELETE for Collections: MUST act as if
    # a "Depth: infinity" header was used on it.  

    # ...the collection specified in the Request-URI and all resources
    # identified by its internal member URLs are to be deleted.  

    # If any resource identified by a member URL cannot be deleted,
    # then all of the member's ancestors MUST NOT be deleted, so as to
    # maintain URL namespace consistency.  

    # Any headers included with DELETE MUST be applied in processing
    # every resource to be deleted.  

    # When the DELETE method has completed processing, it MUST result
    # in a consistent URL namespace.

    # If an error occurs deleting a member resource (a resource other
    # than the resource identified in the Request-URI), then the
    # response can be a 207 (Multi-Status).  Multi-Status is used here
    # to indicate which internal resources could NOT be deleted,
    # including an error code, which should help the client understand
    # which resources caused the failure.  For example, the
    # Multi-Status body could include a response with status 423
    # (Locked) if an internal resource was locked.

    # The server MAY return a 4xx status response, rather than a 207,
    # if the request failed completely.

    # 424 (Failed Dependency) status codes SHOULD NOT be in the 207
    # (Multi- Status) response for DELETE.  They can be safely left
    # out because the client will know that the ancestors of a
    # resource could not be deleted when the client receives an error
    # for the ancestor's progeny.  Additionally, 204 (No Content)
    # errors SHOULD NOT be returned in the 207 (Multi-Status).  The
    # reason for this prohibition is that 204 (No Content) is the
    # default success code.

    my $resource;
    my @ret; 

    eval {
        $resource = _getResource($principal);
        
        # Test for the resource existence first
        # my $path = _resourceToPath($resource);
        # if(!defined($path)){
	if(!ResourceStore::resource_exists($resource)){
	    # Not in the system.
            die "404 Not Found:$resource does not exist";
	}
	
	if(_isCollection($resource)){
            # FIXME Check DEPTH header if the resource for deletion is a
            # collection.  It must be absent or "infinity".  RFC4918
            # Section 9.6.1
	    my $depth = _getDepth();
	    defined($depth) and lc($depth) ne "infinity" and 
		die "400:Bad Request:Depth is not infinity: '$depth'";
	}

        my @response = _handle_DELETE($principal, $resource);
        
        # The @response array will allways exist and have at least one
        # entry
        @response or die "Reality interupt";

        #  $response[0] == 204 then all is good, otherwise there is an
        # error and @response is an array of array refs

        if(@response == 1 and ($response[0]->[0] == 204 or
                               $response[0]->[0] == 423)){
            # Default "204:No Content" or "423:Locked"
            @ret = @{$response[0]};
        }elsif(@response){
            my $xml = XML::LibXML::Element->new("$DAV_pfx:multistatus");
            foreach my $resp (@response){
                if(@$resp > 2){
                    ref($resp->[2]) eq "XML::LibXML::Element" or die
                        "500 Server Error: Wrong type in handle_DELETE response: '".
                        ref($resp->[2])?ref($resp->[2]):"Scalar: ".$resp->[2]."'";
                    $xml->addChild($resp->[2]);
                }
            }
            my $DOC = XML::LibXML->createDocument( "1.0", "utf-8" );
            $DOC->addChild($xml);
            @ret = (207, $resource, $DOC);
        }else{
            die "500 Server Error:".
                "Unexpected resposponse code from handle_DELETE: '".
                $response[0]."'";
        }
    };
    if($@){
        if($@ =~ /^(\d{3})\s+([^:]+):?(.*)/){
            my $error = $1;
            my $res = $2;
            my $message = $3;
            @ret = ($error, $res, undef, $message);
        }else{
            @ret = (500, $resource, undef, "Server Error");
            _LOG($@);
        }
    }
    return @ret;
}
sub handle_OPTIONS( $ ){
    # my $principal = shift or confess; 
    # my $DOC = XML::LibXML->createDocument( "1.0", "utf-8" );
    # return $DOC;

    # Unused.  Handled in main loop
}

sub _handle_GET( $$;$ ){

    # This is used by HEAD too

    my $principal = shift or confess;
    my $resource = shift;
    defined($resource) or die; # "" is root collection 
    my $HEAD = shift;
    defined($HEAD) or $HEAD = 0;
    my @ret = qw"200 OK"; # Default

    # my $path = _resourceToPath($resource);    
    # unless(defined($path) and -e $path){
    if(!ResourceStore::resource_exists($resource)){	
        # Resource does not exist.  Return a 404
        @ret = (404, "Not Found");
    }

    # It does exist
    if(!_isLocked($resource, "READ")){
        # If it is write locked we can still get it.  RFC4918 Section
        # 7 Write Lock
        my $content = ""; # Content to return Empty for HEAD
        if(!$HEAD){
	    $content = ResourceStore::get_resource($resource);
        }
        push(@ret, $content);
    }else{
        @ret = qw|423 Locked|;
    }
    # Append to @ret a hash of headers to put into the return value
    my %headers = ();
    my $fn;
    $fn = $LIVE_PROPERTIES{"getcontentlength"}->[3];
    my $content_length = &$fn($resource);
    defined($content_length) and $headers{"content-length"} = $content_length;
    $LOGLEVEL>2 and _LOG("_handle_GET $resource \$content_length: $content_length: Getting etag");
    my $etag = &{$LIVE_PROPERTIES{"getetag"}->[3]}($resource); #_readProperty($resource, "getetag", "LIVE");
    defined($etag) and $headers{"ETag"} = $etag;
    $LOGLEVEL>2 and _LOG("_handle_GET $resource \$content_length: $content_length:  Etag: $etag");
    my $getlastmodified = &{$LIVE_PROPERTIES{"getlastmodified"}->[3]}($resource);
    defined($getlastmodified) and $headers{"Last-Modified"} = $getlastmodified;
    my $resourcetype = _getResourceType($resource);
    defined($resourcetype) and $headers{"content-type"} = $resourcetype;
    push(@ret, \%headers);
    return @ret;
}    

sub handle_GET( $ ){
    my $principal = shift or confess; 
    my @ret;
    my $resource = _getResource($principal);

    if(_authoriseResource($principal, $resource, "GET")){
        @ret = _handle_GET( $principal, $resource);
    }else{
        @ret = qw|403 Forbidden|;
    }
    return @ret;
}

sub handle_HEAD( $ ){
    my $principal = shift or confess; 
    my $resource = _getResource($principal);
    my @ret;
    if(_authoriseResource($principal, $resource, "HEAD")){
        @ret = _handle_GET($principal, $resource, 1);
    }else{
        @ret = (407, "Forbidden");
    }
    return @ret;
}
sub handle_POST( $ ){
    my $principal = shift or confess; 
    my $DOC = XML::LibXML->createDocument( "1.0", "utf-8" );
    return $DOC;
}
# End of method handlers
#===================================


#===================================
# Authentication
sub _authenticate( ){
    # Called in the main loop before handlers.  Will use the URL or
    # HEADERS to authenticate principals.  Returns the principals
    # name.  This must not be leaked, it is not necessarily known to
    # users.

    # Always use opaque toks for authentication.  Never use cookies
    # because they are not 100% opaque.

    # Using HTTP Digest authentication from Apache.  It handles all
    # the password bollocks and puts user name in here....
    my $ret = $ENV{REMOTE_USER};
    # foreach my $k (sort keys(%ENV)){
    #     _LOG("ENV{$k}\t".$ENV{$k});
    # }
    return $ret;
}

sub _authoriseResource( $$$ ){
    # This is not locking as in chapter 6 RFC4918.  This is to check
    # that the principal has access to the resource by the server's
    # own logic


    my $principal = shift or confess;
    my $resource = shift;
    defined($resource) or die; # "" is root collection 
    my $method = shift or confess;

    # For now (20150825) using a file.  The format of the file is one
    # tab delimited record per line.  The fields are (in order):

    # <principal> <resource> <method> [10]

    # If there is a line in the file with the first three fields
    # matching the passed principal, resource, method 3-tupple then
    # the fourth field gives the return value of the autherisation.
    # If there is no matching line in the file return 1

    my $match = "$principal\t$resource\t$method";
    open(my $fh, "<", $AUTHORISATION_FN) or die "$!: '$AUTHORISATION_FN'";
    flock($fh, LOCK_EX) or die "$!: Cannot lock '$AUTHORISATION_FN'";
    my $ln = 0;
    my $ret = 1;
    while(my $line = <$fh>){
        $ln++;
        chomp $line;
        if(substr($line, 0, length($match)) eq $match){
            # Found the line
            $line =~ /(\d)$/ or die "Line $ln: Invalid (matching): '$line'";
            $ret = $1;
            last;
        }
    }
      
    #$LOGLEVEL>2 and _LOG("authorise($principal, $resource, $method) -> $ret");
    return $ret;
}

# End of authenticaton
#=================================
# Start of miscellaneous utilities

sub _isNumeric( $ ){

    # Oh my aching fucking head!  Why oh why after 30 years have the
    # powers that be in Perl not realised that this function must be
    # built in!
    my $t = shift;
    defined($t) or confess;

    no warnings;
    my $a = $t;
    $a+=0;
    return $a eq $t;
}


sub _cmpXML( $$ ){
    # Pass in two XML nodes and return 0 if they are the same, else
    # lexographically compare the namespaces fist then the node names
    # (with out prefexes)
    my $n1 = shift or confess;
    my $n2 = shift or confess;
    my $parser = XML::LibXML->new();
    eval{
        ref($n1) or $n1 = $parser->parse_string($n1)->documentElement();
        ref($n2) or $n2 = $parser->parse_string($n2)->documentElement();
    };
    if($@){
        die "500 Server Error: _cmpXML must be passed two XML nodes or the textual representation of two nodes";
    }
    my $ns1 = $n1->namespaceURI();
    defined($ns1) or $ns1  = "";
    my $ns2 = $n2->namespaceURI();
    defined($ns2) or $ns2  = "";
    $ns1 ne $ns2 and return $ns1 cmp $ns2;

    my $nn1 = $n1->localname();
    my $nn2 = $n2->localname();
    return $nn1 cmp $nn2;
}


sub _std207Response( $ ){
    my $xml = shift;
    defined($xml) or die; # an be empty string
    my $out = "Status: 207 Multi-Status\r\n".
        "Content-Length: ".length($xml)."\r\n".
        "Content-type: text/xml; charset=\"utf-8\"\r\n".
        "\r\n". $xml;
    return $out;
}

sub _stdResponseNew( $$$$;$ ){
    # 20150818 I changed the pattern of the return arrays.  This is a
    # hack till  get tem all using the new format
    my ($code, $resource, $xml, $txt, $headersRef) = @_;
    defined($code) or die;
    defined($resource) or die;
    defined($xml) or die;
    defined($txt) or die;
    
    my $out = "Status: $code \r\n";
    defined($headersRef) and 
        map{$out .= "$_:".$headersRef->{$_}."\r\n"} keys %$headersRef;
    $out .= "\r\n";
    $xml and $out .= $xml;
    return $out;
}

sub _stdResponse( $;$$$ ){
    my $code = shift or confess; # 0 is an invalid code
    my $message = @_?shift:"";
    my $content = @_?shift:"";
    my $headersRef = shift;
    my $out = "Status: $code $message\r\n";
    if(defined($headersRef)){ 
        my @_k = keys %$headersRef;
        foreach my $_k (@_k){
            $out .= "$_k:";
            if(defined($headersRef->{$_k})){
                $out .= $headersRef->{$_k};
            }else{
                $LOGLEVEL > 2 and _LOG("Header '$_k' unknown");
            }

            $out .= "\r\n";
        }
    }
    $out .= "\r\n".($content?$content:"");
    return $out;
}

sub _stdNoContentResponse( $;$ ){
    my $code = shift or confess; # 0 is an invalid code
    my $message = @_?shift:"";
    my $out = _stdResponse($code, $message);
    $LOGLEVEL > 2&&_LOG("\$code $code \$message $message \$out $out");
    return $out;
}


sub _checkPath ( $ ) {
    # If the passed path contains any dangerous characters die with
    # the reason.  FIXME This is VERY important.  Used to ensure
    # passed paths are not harmful
    my $path = shift or confess;
    $path =~ /\/\.\.\// and die "'$path' is rejected";
    $path =~ /^\.\.\// and die "'$path' is rejected";
    $path =~ /\/\.\.$/ and die "'$path' is rejected";

    # From perlsec man page.  Slightly modified
    if ($path =~ /^([-\w.\/]+)$/) {
        $path = $1;                     # $path now untainted
    } else {
        die "Bad path in '$path'";  
    }
}


sub href( $$$ ){

    # FIXME  WTF is this for?

    my $_h = shift or confess;
    my $xc = shift or confess;
    my $root = shift or confess;
}
sub _getDepth(){
    my $ret = _getHeader("depth");
    
    if(defined($ret)){
        $ret eq "infinity" or $ret eq "0" or $ret eq "1"
            or die "400 Bad Request: DEPTH header, '$ret' is invalid";
    }
    $LOGLEVEL>2 and _LOG("_getDepth(): -> ".(defined($ret)?$ret:'undef'));
    return $ret;
}

sub _getHeader( $ ){
    # Passed a header name get it out of %ENV.  Return undef if it
    # does not exist
    my $h = shift or die "No header";
    $h = uc($h);

    # '-' changed to '_' in %ENV
    $h =~ s/-/_/g;

    my $ret = $ENV{"$h"};
    defined($ret) or $ret =  $ENV{"HTTP_$h"};

    return $ret;
}

sub _readXMLInput(){
    # Get the XML input
    if($ENV{"HTTP_CONTENT_LENGTH"} and
       $ENV{"HTTP_CONTENT_LENGTH"} > $READLIMIT){
        die "413 Payload Too Large:Keep XML to less than $READLIMIT bytes";
    }
    my $dom;
    my $content = _readSTDIN();
    if($content){
        # Parse the XML in $content
        my $parser = XML::LibXML->new();
        eval {
            $dom = $parser->parse_string($content);
        };
        if($@){
            die "400 Bad Request:Invalid XML '$content' '$@'";
        }
    }
    return $dom;
}

# Create the root collection
#_createCollection("");
# Assume it exists
_isCollection($ROOT) or confess "Root collection does not exist";

sub runServer( ){
    my $c = 0;
    #$LOGLEVEL > 2 and _LOG("Setup.  Ready to go....");
    my $method; # Set in loop
    my $request = FCGI::Request();
    while(1) {
        #$LOGLEVEL > 2&&_LOG("Wait for connection...");
        my $_acc = $request->Accept();
	#$LOGLEVEL > 2&&_LOG("\$_acc $_acc");	
        $_acc >= 0 or last;
        $LOGLEVEL > 2&&_LOG("...got connection.");
        # Who is using this?
        my $principal;
        eval{
            $principal = _authenticate( );
        };
        if($@){
            $LOGLEVEL > 2&&_LOG("Authentication Failed");
            print _stdNoContentResponse(500, $HTTP_CODE_MSG{500});
            _LOG($@);
            next;
        }
        if(!defined($principal)){
            $LOGLEVEL > 2&&_LOG("No principal.  Returning 421");
            print _stdNoContentResponse(421, "Unauthorized");
            next;
        }

        my $out = '';

        eval {
            # CGI Loop

            # The string we build to return to the client

            # Initialise the lock system
            _setLockFH();
	    if($LOGLEVEL > 2){
		# my $headers = join("\n", map{$_." => ". $ENV{$_}} sort keys %ENV);
		# _LOG($headers);
	    }

            # If this is a litmus test let the log know
            my $X_Litmus = _getHeader('X-Litmus');
            defined($X_Litmus) and $LOGLEVEL > 2 and _LOG("X_Litmus: '$X_Litmus'");

            # apache only?
            $PROTOCOL = $ENV{'REQUEST_SCHEME'};

            # The METHOD determins how we act
            $method = $ENV{REQUEST_METHOD};

            $LOGLEVEL > 0 and _LOG(" ' command: '$method'".
				   " URL: '"._getURL().
				   " 'Resource: '".
                                  _getResource($principal)."'");

            if($method eq "PROPFIND"){
                my @res = handle_PROPFIND($principal);
                if($res[0] == 207){
                    $out = _std207Response($res[2]);
                }else{
                    $out = _stdNoContentResponse($res[0], $res[1]);
                }
            }elsif($method eq "PROPPATCH"){
                my @res = handle_PROPPATCH($principal);
                if($res[0] == 207){
                    $out = _std207Response($res[2]);
                }else{
                    $out = _stdNoContentResponse($res[0], $res[1]);
                }
            }elsif($method eq "GET"){
                my @res =  handle_GET($principal);
                $out = _stdResponse($res[0], $res[1], $res[2], $res[3]);
            }elsif($method eq "HEAD"){
                my @res =  handle_HEAD($principal);
                $out = _stdResponse($res[0], $res[1], $res[2], $res[3]);
            }elsif($method eq "POST"){
                my $xml =  handle_POST($principal)->toString(1);
                $out = _std207Response($xml);
            }elsif($method eq "MKCOL"){
                my @ret =  handle_MKCOL($principal);
                $out = _stdNoContentResponse($ret[0], $ret[1]);
            }elsif($method eq "DELETE"){
                my @res =  handle_DELETE($principal);
                if($res[0] == 207){
                    $out = _std207Response($res[2]->toString(1));
                }elsif(scalar(@res) == 2){
                    $out = _stdNoContentResponse($res[0], $res[1]);
                }elsif(scalar(@res) == 4){
                    $out = _stdResponseNew($res[0], $res[1], $res[2], $res[3]);
                }else{
                    die "Do not understand handle_DELETE response: '".
                        join(", ", @res)."'";
                }
            }elsif($method eq "PUT"){
                my ($code, $message) =  handle_PUT($principal);
                $out = _stdNoContentResponse($code, $message);
            }elsif($method eq "MOVE"){
                my @ret  =  handle_MOVE($principal);
                if($ret[0] == 207){		  
                    $out = _std207Response($ret[1]);
                }else{
                    scalar(@ret) == 2 or 
                        die "Wrong number of parameters returned for MOVE";
                    $out = _stdNoContentResponse($ret[0], $ret[1]);
                }
            }elsif($method eq "COPY"){
                my @ret  =  handle_COPY($principal);
                scalar(@ret) == 2 or 
                    die "Wrong number of parameters returned for COPY:\nscalar(\@ret) == ".scalar(@ret);
                
                if($ret[0] == 207){		  
                    $out = _std207Response($ret[1]);
                }else{
                    scalar(@ret) == 2 or 
                        die "Wrong number of parameters returned for COPY";
                    $out = _stdNoContentResponse($ret[0], $ret[1]);
                }
            }elsif($method eq "LOCK"){
                my @ret = handle_LOCK($principal);


                
                if($ret[0] == 207){
                    $out = _std207Response($ret[2]);
                }else{
                    $out = _stdResponse($ret[0], $ret[1],$ret[2],
                                        # FIXME Why do I have to add these
                                        # angle brackets?
                                        {'Lock-Token' => $ret[3],
                                             'Content-Type' => 'text/xml; charset="utf-8"'
                                        });
                }
            }elsif($method eq "UNLOCK"){
                my @ret = handle_UNLOCK($principal);
                my $xml = undef;
                if(@ret == 3){
                    $xml = XML::LibXML::Element->new("$DAV_pfx:error");
                    # FIXME  Comment what handle_UNLOCK returns 
                    $xml->addNewChild($DAV_ns, defined($ret[2])?$ret[2]:"");
                    $xml = $xml->toString();
                }
                $out = _stdResponse($ret[0], $ret[1], $xml);
                
            }elsif($method eq "OPTIONS"){
                # FIXME Do some thing like:
                $out = "Allow: OPTIONS, GET, HEAD, DELETE, ".
                    "LOCK, UNLOCK, PROPFIND, PROPPATCH, COPY,MOVE, PUT, MKCOL\r\n".
                    "DAV: 1,2\r\n".
                    "Content-Length: 0\r\n".
                    "\r\n";
            }else{
                die "Ignored method: '$method'";
            }

        };
        if($@){
            _LOG("Died in main loop.  Method: '$method', Error: '$@'");
            if($@ =~ s/^(\d\d\d[^:]+):?//){
                my $error = $1;
                $error =~ /^(\d{3})\s*(.+)$/;
                my $code = $1;
                my $message = $2;
                $out = _stdNoContentResponse($code, $message);
            }else{
                $out = _stdNoContentResponse(500, "Server Error");
            }		
        }
	$LOGLEVEL>1 and _LOG(length($out)<1048?$out:length($out));
        print $out;

        # Make sure all locks released
        _releaseLocks();
    }
}
sub _appendNewChild( $$ ) {
    my $root = shift or confess;
    my $name = shift or confess;
    my $child = XML::LibXML::Element->new($name);
    $root->addChild($child);
    return $child;
}
sub _addNode( $$ ) {
    my ($parent, $child) = @_;
    if(ref($child) eq 'XML::LibXML::Element'){
        $parent->appendChild($child);
    }elsif(ref($child) eq ''){
        $parent->appendTextNode($child);
    }else{
        die "'_addNode child is type: ".ref($child).
            " Do not know what to do with that";
    }
    return $parent;
}




# sub handle_default {
#     $LOGLEVEL > 1 and _LOG("");
#     my $contentRef = shift or confess;
#     my $content = $$contentRef;
#     open(my $out, ">>/tmp/caldav.log") or die $!;
#     flock($out, LOCK_EX) or die "$!: Cannot lock file";
#     binmode($out, ":utf8");
#     print $out "handle_default\n";
#     print $out join("\n", map{$_.' -> '.$ENV{$_}} sort keys %ENV)."\n";
#     print $out "$content\n\n";
#     close $out or die $!;

#     my $ret = "Content-type: text/xml\r\n\r\n\"". 
#         '<?xml version="1.0" encoding="utf-8" ?>';
#     return $ret;
# }

sub allProperties( $ ) {
    #FIXME Unimplemented yet
    my $resource = shift;
    defined($resource) or die; # "" is root collection 
    my @ret = ();
    return @ret;
}





sub allPropertyNames( $ ) {
    # FIXME Unimplemented
    my $resource = shift;
    defined($resource) or die; # "" is root collection 
    my @ret = ();
    return @ret;
}

sub failed {
    my $message = shift;
    print STDERR $message;
    return -1;
}

sub lockOwner {
    my $resource = shift;
    defined($resource) or die; # Empty string is a valid resource"
    #    my $resource = shift or die "No resource to query lock owner";
    # Here is the lock discovery magic
    my $ret = undef;

    # Test code.  Pretending there is an owner
    #my $selector = rand();
    # if($selector < 0.3333){
    #     return "John Smith";
    # }elsif($selector >= 0.3333 and $selector < 0.6667){
    #     return undef;
    # }else{
    #     my $ret =  XML::LibXML::Element->new("$DAV_pfx:href");
    #     $ret->appendTextNode('http://johnsmith.com');
    #     return $ret;
    # }
    return $ret;
}

sub supportedReportSet {
    my $resource = shift;
    defined($resource) or die; # Empty string is a valid resource"
    #    my $resource = shift or die "No resource to query supported report set";
    # Currently no supported reports.
    return undef;
}

sub calendar_query( $ ){
    my $node = shift or confess;
}


sub _comp( $$ ){
    my $filter = shift;
    my $node = shift;
    $node->nodeName() eq "$CDAV_pfx:comp" or die 
        "_comp called with a node named: '".$node->nodeName()."'";

    # <!ELEMENT comp ((allprop | prop*), (allcomp | comp*))>
    # <!ATTLIST comp name CDATA #REQUIRED>

    # $filter is an array ref onto which we need to push the <comp>
    # element description
    my $name = $node->getAttribute('name');
    my $comp = {COMP=>$name};
    push(@$filter, $comp);

    # The children
    my @cnodes = $node->childNodes();

    # Set this to 1 when we encounter a allcomp or comp child, so we
    # know there are no more props.  Allows us to detect errors in XML
    my $finishedProp = 0; 

    #  Set this to 1 if we find an allprop, -1 if we find a prop.  For
    #  detecting errors
    my $prop = 0;

    #  Set this to 1 if we find an allcomp, -1 if we find a comp.  For
    #  detecting errors
    my $compFlag = 0;

    foreach my $n(@cnodes){
        if($n->nodeName() eq "$CDAV_pfx:allprop"){
            $finishedProp and 
                die "All the <prop> and <allprop> elements ".
                "must be before <comp> or <allcomp>.  XML: ".
                $node->toString(2)."'";
            if($prop != 0){

                my $error = $prop == 1?"Cannot have > 1 allprop":
                    "Cannot have both <prop> and <allprop>";
                my $xmlStr = $node->toString(2);
                die "Malformed input XML: '$xmlStr'. '$error'";
            }
            $comp->{PROP} = 1;
            $prop = 1;
            
        }elsif($n->nodeName() eq "$CDAV_pfx:prop"){
            #       <!ELEMENT prop EMPTY>
            #       <!ATTLIST prop name CDATA #REQUIRED
            #                      novalue (yes | no) "no">
            $finishedProp and 
                die "All the <prop> and <allprop> elements ".
                "must be before <comp> or <allcomp>.  XML: ".
                $node->toString(2)."'";
            if($prop == 1){
                my $error = 
                    "Cannot have both <prop> and <allprop>";
                my $xmlStr = $node->toString(2);
                die "Malformed input XML: '$xmlStr'. '$error'";
            }
            my $pname = $n->getAttribute('name') or 
                die "Malformed input XML: '".$n->toString(2)."' No name";
            my $novalue = $n->getAttribute('novalue');
            defined($novalue) or $novalue = "no";
            # FIXME Are the node attribute names case insensitive?
            # Assuming so for now, but CHECK!!  If they are case
            # sensitive then remove the lc($novalue) and just use
            # $novalue
            if(lc($novalue) eq 'yes'){
                $pname = "!$pname";
            }elsif(lc($novalue) ne 'no'){
                die "Invalid attribute value for 'novalue'.  XML: '".
                    $n->toString(2);
            }

            defined($comp->{PROP}) or $comp->{PROP} = [];
            push(@{$comp->{PROP}}, $pname);
            $prop = -1;
        }elsif($n->nodeName() eq "$CDAV_pfx:allcomp"){
            $comp == -1 and 
                die "Cannot have <comp> and <allcomp> mixedXML: ".
                $node->toString(2)."'";
            $comp = 1;
            $finishedProp = 1;
            # Nothing to do to $filter
        }elsif($n->nodeName() eq "$CDAV_pfx:comp"){
            $comp == 1 and 
                die "Cannot have <comp> and <allcomp> mixedXML: ".
                $node->toString(2)."'";
            $comp = -1;
            $finishedProp = 1;
            # Lucky us, we get to recurse!
            my $_comp = [];
            $_comp = _comp($_comp, $n);
            defined($comp->{PROP}) or $comp->{PROP} = [];
            push(@{$comp->{PROP}}, @$_comp);	    
        }
    }

    return $filter;

}
sub supportedCalComponentSet( $ ) {
    my $resource = shift;
    defined($resource) or die; # Empty string is a valid resource"
    #    my $resource = shift or die "No resource to query supported report set";
    # Support all components
    my $scs =  
        XML::LibXML::Element->new("$DAV_pfx:supported-calendar-component-set");
    my $comp = XML::LibXML::Element->new("$CDAV_pfx:comp");
    # Support all components VEVENT, VTODO, VJOURNAL and VFREEBUSY
    my $vevent = $comp->cloneNode();
    $vevent->setAttribute('name', 'VEVENT');
    $scs->appendChild($vevent);
    my $vtodo = $comp->cloneNode();
    $vtodo->setAttribute('name', 'VTODO');
    $scs->appendChild($vtodo);
    my $vjournal = $comp->cloneNode();
    $vjournal->setAttribute('name', 'VJOURNAL');
    $scs->appendChild($vjournal);
    my $vfreebusy = $comp->cloneNode();
    $vfreebusy->setAttribute('name', 'VFREEBUSY');
    $scs->appendChild($vfreebusy);
    return $scs;
}

sub getctag {
    my $resource = shift;
    defined($resource) or die; # Empty string is a valid resource"
    #    my $resource = shift or die "getctag needs a resource";
    # FIXME Unimplemented as yet
    return crypt($resource, "SALT");
}


sub pnode {
    my $r = shift;
    defined($r) or return;
    my $level = shift;
    defined($level) or $level = 0;
    for(1..$level){
        print STDERR   "  ";
    }
    print STDERR   $r->nodeName();
    my @att = $r->attributes();
    map{print STDERR  "\t$_"} grep{defined} @att;
    print STDERR  "\n";
    if(defined($r->nodeValue())){
        for(1..($level + 1)){
            print STDERR   "  ";
        }
        print STDERR  $r->nodeValue()."\n";
    }
    
    my @childs = $r->childNodes();
    map{&pnode($_, $level+1)} @childs;
}

1;
