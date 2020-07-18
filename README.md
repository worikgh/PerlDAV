# WebDAV server written in Perl

There are many `WebDAV` implementations.  This is another one.

`WebDAV` is a Internet protocol for network storage of files.  It is
defined primarily in [`RFC4918`.](https://tools.ietf.org/html/rfc4918
"WebDAV RFC"). This obsoleted `RFC2518`.

It has protocols for storing resources and collections of resources.
Analogous to files and directories.

RFC [`RFC3744`.](https://tools.ietf.org/html/rfc3744 "WebDAV Access
Control Protocol") is also implemented 

# Features

    . Pure Perl.  No need for `mysql` or other database software
    . Simple.  Implemented as a translation of the applicable RFCs
    
# Limitations

This will not scale.  It works well for a small installation with not
many users or much traffic.  The system works by storing data about
resources in flat text files.  These text files are unstructured and
must be searched from beginning to end each time they are used.  This
is a simple approach, and it is light on resources, other than time.
It is a slow approach that will not scale.

# Installation

These are instructions for installing this with lighttpd an a `Ubuntu`
server.  This software is implemented as `FastCGI`.

`lighttpd` is a lightweight and fast http server. It is perfect for a
application such as this where the software is delivered with `CGI`.

## Requirements

+ Perl 
++ XML::LibXML.pm (`libxml-libxml-perl`)
++ Data::Validate::URI (`libdata-validate-uri-perl`)
++ URI::Escape::XS (`liburi-escape-xs-perl`)
++ Crypt::PasswdMD5 (`libcrypt-passwdmd5-perl`)
+ lighttpd (`lighttpd`) [Lighttpd's Website](https://redmine.lighttpd.net "lighttpd information")

Install these packages.  (Perl is installed by default on `Ubuntu`)

## Configure `lighttpd`

In the lighttpd configuration file in the `server.modules` statement
add `"mod_fastcgi"`, and `mod_auth`.  `mod_fastcgi` is needed to run
`PerlDAV` as fast CGI.  `mod_auth` is required to place a
user/password between the wild world and the `PerlDAV`

server.modules = (<br/>
&nbsp;        "mod_access",<br/>
&nbsp;        "mod_alias",<br/>
&nbsp;        "mod_compress",<br/>
&nbsp;        "mod_redirect",<br/>
&nbsp;        <b>"mod_auth",</b><br/>
&nbsp;        <b>"mod_fastcgi",</b><br/>
)

At the end of lighttpd configuration file add the following two lines:
* auth.backend = "htdigest"
* auth.backend.htdigest.userfile = "<Path to password file (See `$PASSWORD_FN` below)>"

Next turn on authentication for `/dav/`, the path to the server. (I
would like it if declaring variables was allowed in a lighttpd
configuration file...)

auth.require = ( "/dav/" =><br/>
&nbsp;&nbsp;(<br/>
&nbsp;&nbsp;&nbsp;&nbsp;"method" => "digest",<br/>
&nbsp;&nbsp;&nbsp;&nbsp;"realm" => "WebDAV",<br/>
&nbsp;&nbsp;&nbsp;&nbsp;"require" => "user=USER_NAME"<br/>
&nbsp;&nbsp;)<br/>
)

[FastCGI Mod Documentation](https://redmine.lighttpd.net/projects/lighttpd/wiki/Docs_ModFastCGI "mod_fastcgi docs")


fastcgi.server = (<br/>
&nbsp;"/WebDAV" =><br/>
&nbsp;&nbsp;( "WebDAV" =><br/>
&nbsp;&nbsp;&nbsp; (<br/>
&nbsp;&nbsp;&nbsp;&nbsp; "socket" => "/var/run/lighttpd/webdav.socket",<br/>
&nbsp;&nbsp;&nbsp;&nbsp; "bin-path" => "<Path to PerlDAV/WebDAV>"<br/>
&nbsp;&nbsp;&nbsp;&nbsp; )<br/>
&nbsp;&nbsp;&nbsp; )<br/>
&nbsp;&nbsp;)<br/>

This will mean paths with the prefix `/WebDAV` will use our `FastCGI` programme.

## Configure `PerlDAV`

Check out the archive using git and clone (As I write I have yet to
have a anonymous git access).  Make sure that it is *not* unpacked
into the web server's `DocumentRoot`.  That would expose configuration
and data files that are best kept in the same directory, or a tree
rooted in the same directory, as `PerlDAV` itself.

### Required Files


The required file names are all hard coded into `WebDAV.pm`.  The
tiles are:

* `$RESOURCE_PATH_FN` File for storing resource definitions (File or collection).  Defaults to `._tr`
* `$AUTHORISATION_FN` File for handling authorisation for WebDAV operations by a user on a resource. Defaults to `._authorise`
* `$LOCK_FN` File for handling WebDAV locks. Defaults to `._DAV_LOCKS`
* `$LIVE_PROPERTIESDBFN` File that stores live properties for resources.  Defaults to `._PropertiesLive`
* `$DEAD_PROPERTIESDBFN` File that stores dead properties for resources.  Defaults to `._PropertiesDead`
* `$PASSWORD_FN` Not used directly by `PerlDAV`, but controls access
  to the resources at a web server level.  Defaults to `DavPassword`
* `$DATADIR` Directory where the resources are stored. Defaults to `DATA/` 


The five  files (other  than `$PASSWORD_FN`) must  be created  as empty
files,  and the  data directory  created  as a  empty directory.   The
little Perl  script `initialiseDAV` will prepare  a blank installation
with the above default values.

The password file can be managed with `htdigest` from the `Apache `
distribution.  `addwebDAV_user` (see next section) also adds entries
into the `$PASSWORD_FN`.

## Adding Users

`add_webDAV_user` creates an account for a user.

Takes as arguments a user name, password, root collection and quota
(storage area size) for a new user.  If any of these do not exist it
prompts on stdin/stdout for them

It checks that the user does not exist and that the root
collection does not exist as a resource.  (It is OK for two users
to share a route collection)

# PelrDAV
