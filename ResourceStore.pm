package ResourceStore;

# Provide translation between resource identifiers and the file system

use Carp;
use strict;
use warnings;
use Crypt::PasswdMD5; # Generate ETAGs and tokens
use File::MimeInfo;
use WDGlobals qw(
    $ROOT
    $LOGLEVEL
    %LIVE_PROPERTIES
    $RESOURCE_PATH_FN
    $DATADIR
    $DEAD_PROPERTIESDBFN
    $LIVE_PROPERTIESDBFN
    $MAXSTORAGE
    $READLIMIT
    $PROTOCOL
    _LOG
    _readSTDIN
);
use Fcntl qw(:flock SEEK_END); # For accessing properties

sub initialise(){
    foreach my $_fn ($RESOURCE_PATH_FN, , $DATADIR, $LIVE_PROPERTIESDBFN, $DEAD_PROPERTIESDBFN){
	-r $_fn or die "$!: '$_fn' not readable";
	-w $_fn or die "$!: '$_fn' not writable";
	
    }
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
# <D:quota-available-bytes>596650</D:quota-available-bytes>
# <D:quota-used-bytes>403350</D:quota-used-bytes>
sub get_available_bytes( $ ){
    _LOG("FIXME:  Quota manegement unimplemented");
    return $MAXSTORAGE/2;
}
sub get_used_bytes( $ ){
    _LOG("FIXME:  Quota manegement unimplemented");
    return 1000;
}


sub property_type( $$ ) {
    my $resource = shift;
    defined $resource or confess; # "" is a valid resource.  It is ROOT
    my $name = shift or confess;
    ## FIXME Are there corner cases where the server can be pushed
    ## into asking for the property type type of a empty string?

    #_LOG("property_type $resource $name");
    my $ret;
    if(defined($LIVE_PROPERTIES{$name})){
	$ret = 'LIVE';
    }elsif(defined(read_property($resource, $name, "DEAD"))){
	$ret = 'DEAD';
    }else{
	# Return empty string so the result of this can be compared to
	# 'LIVE' or 'DEAD' even in case where property does not exist
	$ret = "";
    }
    return $ret;
}
sub _setLiveProperties( $ ) {

    # For the passed resource that is being newly added, moved or
    # copied to the DAV system so calculate the LIVE properties
    my $resource = shift;
    defined($resource) or die; # "" is root collection 

    # Content type using Mime detection
    my $content_type = mimetype(resource_to_path($resource));
    add_property($resource, "getcontenttype", $content_type, "LIVE");

    
}
sub _lockProperties( $ ){
    # Place an exclusive lock on the properties database returning the
    # file handle.  Each call to _lockProperties must be matched by a
    # call to _unlockProperties or close the file handle

    my $which = shift or confess;
    my $mode = '+<';
    my $fh;
    if($which eq "LIVE"){
        -e $LIVE_PROPERTIESDBFN or $mode = '+<';
        open($fh, $mode, $LIVE_PROPERTIESDBFN) 
            or die "$!: '$LIVE_PROPERTIESDBFN'";
    }elsif($which eq "DEAD"){
        -e $DEAD_PROPERTIESDBFN or $mode = '+<';
        open($fh, $mode, $DEAD_PROPERTIESDBFN) 
            or die "$!: '$DEAD_PROPERTIESDBFN'";
    }else{
        die "_lockProperties called with which: '$which'.  Not corrct";
    }

    binmode($fh, ":utf8") or die "$!";
    flock($fh, LOCK_EX) or die "$!: '$LIVE_PROPERTIESDBFN'";
    return $fh;
}
sub _unlockProperties( $ ){
    # Passed the file handle for properties
    my $fh = shift or confess;
    my $ret = flock($fh, LOCK_UN) or die "$!: '$LIVE_PROPERTIESDBFN'";
    return $ret;
}

sub _storeResourcePathTable_fh( $$$ ){
    my $tableRef = shift or confess;
    my %table = %$tableRef;
    my $typesRef = shift or confess;
    my %types = %$typesRef;
    my $fh = shift or confess; # File handle that has been locked
    # initialise content of $RESOURCE_PATH_FN
    truncate($fh, 0) or die "$!: Cannot truncate '$RESOURCE_PATH_FN'";
    seek($fh, 0, 0) or die "$!: Cannot rewind '$RESOURCE_PATH_FN'";
    my $text = '';
    foreach my $k (sort keys %table){
        defined($types{$k}) or die "Resource '$k' in \%table but not \%types";
        my $path = $table{$k};
        my $type = $types{$k};
        $text .= "$k\t$path\t$type\n";
    }
    print($fh $text);
}

sub _readResourcePathTable_fh( $ ){
    # Read the file that maps resources to paths and types.  Return
    #  two HASH refs and a scalar: %table, %types and $last.  %table
    #  maps resource sements to paths, types and %types maps resource
    #  paths to the type of resource and $last is the last path
    #  segment allocated (so we can allocate another that does not
    #  clash)

    # Translation table is %table, the types of each entry are in
    # %types.  Each part of a resource (the bits between "/"
    # characters) has an entry.  So "foo/bar/fletch" would by...

    # $table{ROOT/foo} = <path>
    # $table{ROOT/foo/bar} = <path>
    # $table{ROOT/foo/bar/fletch} = <path>
    # $types{ROOT/foo} = "Collection"
    # $types{ROOT/foo/bar} = "Collection"
    # $types{ROOT/foo/bar/fletch} = "Resource"

    # If there is a resource in %table it must be in %types
    
    # If there is a resource: "a/b/c" in %table (and %types) then so
    # must resources "a/b" and "a", ane the types of "a/b" and "a"
    # must be collection

    my $fh = shift or confess;  # Pass in file handle so calling
    # functions can ensure it is locked

    # Debugging code...
    my @_d = stat($fh);

    my %table = ();
    my %types =();

    # The names are of form [a-z]+.  The first will be "a" then 25
    # through "z" then "aa" and 25 through "az" then "aaa" and through
    # "aaz" then "aba" through "abz" and so on.  The last one
    # decleared in the file is stored in $last so we can make the next
    my $last = undef;
    
    my $_compare_names = sub( $$ ){
        # Comparing the names we give files for total ordering

        # Return -1 if $a is before $b
        # Return  1 if $a is after  $b
        # Return 0 if $a is same as $b
        my $a = shift or confess;
        my $b = shift or confess;

        # If a name is longer than another it is after it in the
        # ordering
        my $an = length($a);
        my $bn = length($b);
        $an != $bn and return $an <=> $bn;

        # Names same length
        return $a cmp $b;
    };
    seek($fh, 0, 0) or die "$!: cannot rewind";
    my @_fh = <$fh>;

    map{
        # Line has a resource followed by a tab followed by a path
        # followed by a tab followed by type identifier
        chomp;
        /^\s*([^\t]+)\t([^\t]+)\t([^\t]+)\s*$/ or die "Badly formed line '$_'";
        my ($resource, $path, $type) = ($1, $2, $3);

        # Check sanity of $type
        $type ne "collection" and $type ne "resource" and 
            die "type: '$type' not understood";

        # Check sanity of path
        $path =~ /^[a-z\/]*$/i or die "path: '$path' not understood";

        # FIXME A check of resource using a regex is needed here.  It
        # must be a valid segment of a URL: URI module?

        # Keep $last updated.  
        my @path = $path?split(/\//, $path):();

        # If the path has a leading slash remove the empty element
        # from the front of @path
        $path =~ /^\// and shift(@path);
        foreach my $p(@path){
            if(!defined($last) or &$_compare_names($p, $last) == 1){
                $last = $p;
            }
        }
        
        # Update hash tables
        $table{$resource} = $path;
        $types{$resource} = $type;
	#_LOG("\$resource $resource \$path $path \$type $type");
    }grep{
        # Comment lines rejected and must contain non-white-space
        /^\s*[^\#\s]/
    }@_fh;


    # Sanity check
    my @keys_tab = sort keys %table;
    my @keys_typ = sort keys %types;

    scalar(@keys_tab) == scalar(@keys_typ) or 
        confess "Sanity check of resource/path/type tables has ".
        "failed: There is not the same number of entries in ".
        "\%table as in \%types";

    for(my $i = 0; $i < @keys_typ; $i++){
        $keys_tab[$i] ne $keys_typ[$i] and 
            die "Resources differ (at position $i) for \%table and \%types.";

        # Check that for every resource a/b/c in tables a/b exists
        my $k = $keys_tab[$i]; # The resource to examine this time
        my @k = split(/\//, $k); # The pieces to work with
        my @_k = (); # The intermediate stages
        foreach my $r (@k){
            push(@_k, $r);
            my $_r = join("/", @_k); # intermediate resource to check
            $_r eq 'ROOT' and next; # All reources have 'ROOT/' prepended
            exists($table{$_r}) or 
                die "Resource: '$_r' does not exist in \%table";
            exists($types{$_r}) or 
                die "Resource: '$_r' does not exist in \%types";
            if($_r ne $k){
                # This is an intermediate resource so must be a
                # collection
                $types{$_r} eq "collection" or 
                    die "Intermediate resource '$_r' s not a ".
                    "'Collection', it is: '".$types{$_r}."'";
            }
        }
    }
    return (\%table, \%types, $last);
}


sub _lockResourcePathTable(){
    # Get a file handle to $RESOURCE_PATH_FN opened for append and
    # locked exclusively
    open(my $fh, "+<", $RESOURCE_PATH_FN) or die "$!: '$RESOURCE_PATH_FN'";
    flock($fh, LOCK_EX) or die "$!: Cannot lock '$RESOURCE_PATH_FN'";
    return $fh;
}

sub list_collection( $;$ ) {

    # List the elements in a collection.  The immediate children if
    # `level` (optional second parameter) is missing or 0, elase all
    # children
    
    my $resource = shift;
    defined($resource) or die; # "" is root collection 
    # If $level is non zero only return the immediate children
    my $level = shift;
    defined($level) or $level = 0;
    my $fh = _lockResourcePathTable();
    
    my ($tableref, $typesref, $last) = _readResourcePathTable_fh($fh);
    
    my @ret = grep{/^$resource\//} keys %$tableref;
    $level and 
        # Only top level ones
        @ret = grep{/$resource\/[^\/]+\/?$/} @ret;
    close($fh) or die "$! '$RESOURCE_PATH_FN'";
    return @ret;
}

sub get_descendants( $ ) {

    # Returns all resources that are descendants of the passed resource
    # (excluding the resource itself)
    my $resource = shift;
    defined($resource) or die; # "" is root collection 

    my @ret = ();
    # If the resource is not a collection return an empty array
    if(is_collection($resource)){
        my $fh = _lockResourcePathTable();

        my($tableref, $NOCARE, $DONOTCARE) = _readResourcePathTable_fh($fh);
        @ret = grep{/^$resource\//}sort keys %$tableref;
        close($fh) or die "$! '$RESOURCE_PATH_FN'";
    }
    return @ret;
}

sub clean_tables() {
    # Ensure that the tables that translate resources to paths and
    # store the types are in a good state.

    # Do this by reading the tables and examine the paths in the file
    # system.  The file system is the gold standard, if a document is
    # in the tables but not in the file system delete it from the the
    # tables.  If a document/directory is in the file system but not
    # in the tables log but ignore.


    # FIXME Should this be called here?
    initialise_resource_property($ROOT);
    _setLiveProperties($ROOT);

    my $fh = _lockResourcePathTable();
    my ($tableref, $typesref, $last) = _readResourcePathTable_fh($fh);

    # Check each file and directory in table exists
    my @paths = sort keys %$tableref;
    map{
        my $path = $tableref->{$_};
        unless(-e $DATADIR.'/'.$path){
            _LOG("Path: '$path' for resource: '".$_.
                 "' type: '".$typesref->{$_}.
                 "' not in file system.  Deleting from tables");
            delete($tableref->{$_});
            delete($typesref->{$_});

        }
    } @paths;

    # Log any entries in file system that are not in the tables

    @paths = grep {$_ ne $DATADIR} map{chomp; $_} `find $DATADIR`;
    $? and die "'$DATADIR' $?"; # Error in shell
    foreach my $p  (grep{/\S/} @paths){
        $p =~ s/^$DATADIR\/?// or die "Path '$p' not understood";
        if(!grep{$_ eq $p} values %$tableref){
            _LOG("Entry in file system: '$p' not in tables");
        }
    }
    # # Ensure that the root resource is in the tables
    # $$tableref{$ROOT} = $DATADIR;
    # $$typesref{$ROOT} = 'collection';
    _storeResourcePathTable_fh($tableref, $typesref, $fh);	
    close($fh) or die "$! '$RESOURCE_PATH_FN'";
}

sub _irp( $$ ){
    my $resource = shift;
    defined($resource) or die; # "" is root collection 
    my $which = shift or confess;
    my $fh = _lockProperties($which);
    my $ret =  _initialiseResourcePropertyf($resource, $fh);
    close($fh)  or die "$!: '$LIVE_PROPERTIESDBFN'";
    return $ret;
}
sub _initialiseResourcePropertyf( $$ ){
    # Add the resources into the properties database with no
    # properties.

    # If the resource already exists issue a warning but do not die
    # (FIXME ???? Is this the right thing to do????)
    my $resource = shift;
    defined($resource) or die; # "" is root collection 

    # All resources have the trailing slash removed.  This is a
    # problem for the root collection that is then an empty string,
    # and we cannot put it in the file.
    # 20161111 Changed root resource to "ROOT"
    # $resource eq "" and $resource = "/";

    my $fh = shift or confess;
    my $state = 0; # Control reading file and check this after the
    # eval loop for success at finding the resource
    eval{
        seek($fh, 0, 0) or die "$!: Cannok seek on properties";
        my $newf = '';
        foreach my $line (<$fh>){

            $line =~ /^\s*#/ and next; # Comments
            chomp $line;
            
            if($state == 0){
                # When we are looking for the start of the block for this
                # resource
                if($line eq $resource){ 

                    # Found a block with this resource
                    $state = 2;

                }elsif($line =~ /\S/){
                    # this is a block with different resource
                    $state = 1;
                }
            }elsif($state == 1){
                $line =~ /^\s*$/ and $state = 0;
            }
            $newf .= $line."\n";
        }

        if($state == 0){
            # The resource is not in the database already
            $newf .= "$resource\n\n";
        }
        # Write the new contents
        truncate($fh, 0) or die "$!: Cannot truncate properties";
        seek($fh, 0, 0, ) or die "$!: Cannot seek in properties database";
        print($fh $newf) or die "$!: Cannot write out new properties";
    };
    if($@){
        _LOG($@);
    }
} 
sub initialise_resource_property( $ ) {
    # Create a resource in the database with no properties
    my $resource = shift;
    defined($resource) or confess; # "" is root collection    
    _irp($resource, "LIVE");
    _irp($resource, "DEAD");
}
sub _removeResourcePropertyf( $$ ){
    my $resource = shift;
    defined($resource) or die; # "" is root collection 

    # All resources have the trailing slash removed.  This is a
    # problem for the root collection that is then an empty string,
    # and we cannot put it in the file.
    # 20161111 Changed root resource to "ROOT"
    # $resource eq "" and $resource = "/";
    

    my $fh = shift or confess;
    my $state = 0; # Control reading file and check this after the
    # eval loop for success at finding the resource
    eval{
        seek($fh, 0, 0) or die "$!: Cannok seek on properties";
        my $newf = '';
        foreach my $line (<$fh>){
            chomp $line;
            $line =~ /^\s*#/ and next; # Comments
            
            if($state == 0){
                # When we are looking for the start of the block for this
                # resource
                if($line eq $resource){
                    # Found the block with this resource
                    $state = 1; 
                    next; # Do not add this line
                }elsif($line =~ /\S/){
                    # This block is a different resource
                    $state = -1; 
                }
            }elsif($state == -1){
                # in the wrong resource block
                if($line =~ /^\s*$/){
                    # Finished this resource block.  Reset state to start
                    # looking for the correct resource block again
                    $state = 0;
                }
            }elsif($state == 1){
                # in the correct resource block.  
                if($line =~ /^\s*$/){
                    # Finished with this 
                    $state = 0;
                }
                next;
            }
            $newf .= $line."\n";
        }
        # Write the new contents
        truncate($fh, 0) or die "$!: Cannot truncate properties";
        seek($fh, 0, 0, ) or die "$!: Cannot seek in properties database";
        print($fh $newf) or die "$!: Cannot write out new properties";
    };
    if($@){
        _LOG($@);
    }
} 

sub remove_resource_property( $$ ) {
    my $resource = shift;
    defined($resource) or die; # "" is root collection 
    my $which = shift or confess; #LIVE or DEAD    
    my $fh = _lockProperties($which);
    my $ret = _removeResourcePropertyf($resource, $fh);
    close($fh)  or die "$!";
    return $ret;
}

sub remove_resource( $ ) {
    # Remove the passed resource from the database but not the file system
    # Return 1 if successful.  0 if not
    my $resource = shift;
    defined($resource) or die; # "" is root collection 
    my $fh = _lockResourcePathTable();
    my ($tableref, $typesref, $last) = _readResourcePathTable_fh($fh);
    my $ret = 1;
    if(defined($tableref->{$resource})){
	delete($tableref->{$resource});
    }else{
	_LOG("\$resource '$resource' is undefined in `tableref`");
	$ret = 0;
    }
    if(defined($typesref->{$resource})){
	delete($typesref->{$resource});
    }else{
	_LOG("\$resource '$resource' is undefined in `typesref`");
	$ret = 0;
    }
    _storeResourcePathTable_fh($tableref, $typesref, $fh);
    close($fh) or die "$! '$RESOURCE_PATH_FN'";
    return $ret;
}

sub add_resource( $$ ) {
    # Passed a resource name and a resource type enter that resource
    # into the database with a path for it.  Return the path in the file
    # system local to the working directory

    # Call from a evall block as dies for many errors

    my $resource = shift or confess; 
    if($resource eq $ROOT){
        confess "Cannot call add_resource for '$resource'";
    }
    my $type = shift or confess;
    $type eq 'resource' or $type eq 'collection' or 
        confess "Type: '$type' missunderstood";
    my $parent = get_parent($resource);
    defined($parent) or
        die "409 Conflict:Parent of resource '$resource' does not exst";
    my $parent_path = resource_to_path($parent);
    defined($parent_path) or 
        die "409 Conflict:Parent collection ('$parent') for '$resource' not defined in database";
    
    my $ret = undef;

    # We do not want the actual path in the file system, just where it
    # is in the directory we use to store data
    $parent_path =~ s/^$DATADIR\/?// or die "'$parent_path' not understood";


    my $fh = _lockResourcePathTable();
    my ($tableref, $typesref, $last) = _readResourcePathTable_fh($fh);
    my %table = %$tableref;
    my %types = %$typesref;

    # $last will need to be incremented if we add a resource to %table
    # and %types
    my $_increment_last = sub(){
        if(!defined($last)){
            $last = "A";
        }else{
            $last =~ /([A-Z])$/ or die "last: '$last' invalid";
            my $one = $1;
            if($one eq "Z"){
                $last .= "A";
            }else{
                my $_next = chr(ord($one) + 1);
                $last =~ s/$one$/$_next/;
            }
        }
        return $last;
    };

    # We must add this resource First check that the parent
    # resource is there and is a collection

    my $path;
    if(defined($table{$resource})) {
	$path = $table{$resource};
	$ret = $DATADIR.'/'.$path;
    }else{
	$path = &$_increment_last();
	
	$parent_path and $path = $parent_path . "/$path";
	$table{$resource} = $path;
	$types{$resource} = $type;

	$ret = $DATADIR.'/'.$path;
	-e $ret and 
	    die "500 Server Error:Path for resource '$resource' ".
	    "exists in file system";
	
	_storeResourcePathTable_fh(\%table, \%types, $fh);

	# Modified resource/path tables
    }
    close($fh) or die "$! '$RESOURCE_PATH_FN'";
    return $ret; 
}

sub copy_properties( $$ ){
    my $source = shift or confess;
    my $destination = shift or confess;

    my %_p = _listProperties($source);
    my @dead = @{$_p{DEAD}};
    my @live = @{$_p{LIVE}};

    # For dead properties we just do a straight copy
    foreach my $d(@dead){
        my $p = _readProperty($source, $d, "DEAD");
        defined($p) or die "500 Server Error: Dead property '$d' ".
            "not found for resource '$source'";
        add_property($destination, $d, $p, "DEAD");
    }

    # Each live property has to be considered separately
    my $state = 0; # Control reading file and check this after the
    foreach my $k (sort keys %LIVE_PROPERTIES){
        my $what = $LIVE_PROPERTIES{$k}->[1];
        if($what == 1){
            # Recalculate the live property
            my $fn = $LIVE_PROPERTIES{$k}->[3];
            my $v = &$fn($destination);
            add_property($destination, $k, $v, "LIVE");
        }elsif($what == 0){
            # Copy the property

            my $srs = _readProperty($source, $k, "LIVE");
            defined($srs) or die "Cannot read LIVE property ".
                "'$k' for '$source'";
            add_property($destination, $k, $srs, "LIVE") ;
        }else{
            # Can only be 1 or 0
            die "Property '$k' has a COPY value of '$what' ".
                "in \%LIVE_PROPERTIES";
        }
    }
}
sub copy_resource( $$$ ){

    # Copy a single resource.  A collection (make the new directory if
    # it does not exist and copy the properties) or a file resource
    # FIXME What when individual resources can be something other than
    # files?

    # Return [$resource, $code]

    # PRCONDITION: The caller has all the necessary permissions.
    # There are no checks here

    my $source = shift or confess;
    my $destination = shift or confess;
    my $over_write = shift;
    defined($over_write) or confess;

    # The types
    my $src_type = ResourceStore::get_resource_type($source) or 
	die "404 Not Found:Cannot find type of: '$source'";
    my $dst_type = ResourceStore::get_resource_type($destination);
    # Commented out 20160204  If the type is unknown then so be it.
    # defined($dst_type) or $dst_type = $src_type;

    my $src_fn = resource_to_path($source) or die; # Will succeed FLW
    my $dst_fn = resource_to_path($destination);
    
    my $ret;
    if(defined($dst_fn)  and -e $dst_fn and !$over_write){
	# The destination exists and there is no permission to over write it
	$ret = [$destination, 412, 
		"Destination exists and cannot be overwritten"];	
    }elsif(!defined($src_fn)){
	# This either is a server error or a client error.  Blame
	# the client! This will never happen
	$ret = [$source,  404, "No path for '$source'"];
    }elsif(!defined($src_type)){
	# This either is a server error or a client error.  Blame
	# the client!
	$ret = [$source,  404, "No type for '$source'"];

	# The following block was commented out 20150910.  

	# Dusseault pg 133 says that the destination header is the
	# complete destination.  SO if a COPY or a MOVE has a
	# non-collection resource as the source and a collection
	# resource as the destination (it must exist otherwise we
	# could not know that it is a collection) the collection
	# is DELETEed and replaced with the resource renamed
	# according to the destination header

	# }elsif(defined($dst_type) and $dst_type ne $src_type){
	#     # FIXME 20150810 This is my addition.  If the types do not
	#     # match there are ambiguities in the implementation of the
	#     # protocol
	#     $ret = [$source, 403, 
	# 	    "Types for '$source' and '$destination' do not match"];

    }elsif(!-e $src_fn){
	# Not in RFC4918 for COPY.  An oversight!
	$ret = [$source,  404, "Source non-existent"];
    }elsif(defined($dst_fn) and -e $dst_fn 
	   and defined(_getHeader("Overwrite"))
	   and _getHeader("Overwrite") eq "F"){
	# If a COPY request has an Overwrite header with a value
	# of "F", and a resource exists at the Destination URL,
	# the server MUST fail the request.
	$ret = [$destination, 412, 
		"Destination exists and cannot be overwritten"];
    }else{
	# The copy operation is permitted so long as we can get a
	# lock on the destination.  FIXME Write a _testGetLock(..)
	# function for this



	# return code for success

	# 201 (Created) - The source resource was successfully
	# copied.  The COPY operation resulted in the creation of
	# a new resource.

	# 204 (No Content) - The source resource was successfully
	# copied to a preexisting destination resource.
	my $code = 204;
	(defined($dst_fn) and -e $dst_fn) or $code = 201;
	
	if(!is_collection($source)){
	    #  It is a not a collection copy the file.
	    #  Copy the file
	    open(my $fh, "<", $src_fn) or die "$!: '$src_fn'";
	    flock($fh, LOCK_EX) or die "$!: Cannot lock '$src_fn'";		
	    my $content = join("", <$fh>);
	    close($fh) or die "$!: '$src_fn'";

	    $code == 201 and # Creating destination
		$dst_fn = create_resource($destination);

	    open($fh, ">", $dst_fn) or die "$!: '$dst_fn'";		
	    flock($fh, LOCK_EX) or die "$!: Cannot lock '$dst_fn'";
	    print($fh $content) or die "$!: '$dst_fn'";
	    close($fh) or die "$!: '$dst_fn'";

	    # Copy the dead properties and recalculate the live
	    # ones

	    copy_properties($source, $destination);	
	}else{
	    # The resource is a collection.  Copy the properties.  
	    
	    # This is not a fatal error.  The resource will get
	    # overwritten by the collection
	    # defined($path) and
	    #     -e $path and !-d $path and die "Resource '$destination' ".
	    #     "exists and is not a directory when it is supposed to ".
	    #     "be the destination for a collection";
	    if(!resource_exists($destination)){
		create_collection($destination);
	    }
	    
	    copy_properties($source, $destination);	
	}
	$LOGLEVEL > 2 and _LOG("Returning ($source, $code, Success) from ResourceStore::copy_resource");
	$ret = [$source, $code, "Success"];
    }
    defined $ret or confess "copy_resource($source, $destination) Return value undefined";
    return $ret;
}
sub resource_to_path( $ ) {
    my $resource = shift;
    defined($resource) or die; # "" is root collection 
    # 20161111 Changed root resource to "ROOT"

    # All resources are stored WITHOUT the trailing slash.
    $resource =~ s/\/\s*$//;

    $resource eq $ROOT and return $DATADIR; # Special case, the root

    my $fh = _lockResourcePathTable();
    my ($tableref, $typesref, $last) = _readResourcePathTable_fh($fh);

    my %table = %$tableref;
    my %types = %$typesref;

    my $ret = undef;
    if($table{$resource}){
        # Resource exists
        $ret = $DATADIR . '/'.$table{$resource};
    }
    $LOGLEVEL > 2&&!defined($ret)&&_LOG("resource_to_path($resource) undefined ");
    close($fh) or die "$! '$RESOURCE_PATH_FN'";
    return $ret;
}    

sub delete_resource( $ ) {
    my $resource = shift;
    defined($resource) or confess; # "" is root collection 

    # If everything goes well then return this; Else an array of error
    # nodes
    my @ret;# = ([204, "No Content"]); # Success is default

    # FIXME Assuming there is a 1-1 maping between the
    # resource hierarchy and file system
    $LOGLEVEL > 2&&_LOG("delete_resource($resource)");
    my $path = resource_to_path($resource);
    $LOGLEVEL > 2&&_LOG("\$resource $resource \$path $path ".
			"is_collection: ".is_collection($resource)?"Yes":"No");
    if(defined($path) and -d $path){
        # $resource is a collection
        is_collection($resource) or die "\$resource should be a collection";
	$LOGLEVEL > 2&&_LOG("delete_resource($resource)");
	if(rmdir($path)){
	    remove_resource_property($resource, "LIVE");
	    remove_resource_property($resource, "DEAD");
	    remove_resource($resource) or die "Failed to remove resource '$resource'";
	    @ret = ([204, $resource, "", ""]);
	}else{
	    $LOGLEVEL > 2&&_LOG("Error here");
	    @ret = ([500,$resource, $!]);
	}
    }elsif(is_collection($resource)){
	$LOGLEVEL > 2&&_LOG("delete_resource($resource)");
	@ret = ([400, $resource, "Resource: '$resource' is a ".
		 "collection but not a directory in the file system"]);
    }elsif(!-e $path){
	$LOGLEVEL > 2&&_LOG("delete_resource($resource)");
	@ret = ([400, $resource, "Resource: '$resource' is not".
		 " in the file system"]);
    }else{
	$LOGLEVEL > 2&&_LOG("delete_resource($resource)");
	if(unlink($path)){
	    remove_resource_property($resource, "LIVE");
	    remove_resource_property($resource, "DEAD");
	    remove_resource($resource);
	    @ret = ([204, "No Content"]);
	}else{
	    $LOGLEVEL > 2&&_LOG("delete_resource($resource) Error: $!");
	    @ret = ([400, $resource, "Could not delete resource file: '$!'"]);
	}
    }
    return @ret;
}

sub get_resource( $ ){
    my $resource  = shift;
    defined($resource) or confess;
    is_collection($resource) and return "";
    my $path = resource_to_path($resource);
    $LOGLEVEL > 2 and _LOG("Getting non-collection resourc: $resource from path $path");
    open(my $fh, "<", $path) or confess "$!: $path";
    return join "", <$fh>;
}

sub get_resource_type( $ ) {

    # Get the type of the passed resource.  If the resource does not
    # exist return undef

    my $resource  = shift;
    defined($resource) or die;

    # Secial case of root collection
    $resource eq $ROOT and  return "collection";

    my $ret = undef;
    my $fh = _lockResourcePathTable();

    my ($tableref, $typesref, $last) = _readResourcePathTable_fh($fh);
    my $type = $typesref->{$resource};
    if(defined($type)){
        $ret = $type;
    }
    close($fh) or die "$! '$RESOURCE_PATH_FN'";
    return $ret;
}    

sub get_last_modified( $ ){
    #$LOGLEVEL > 1 and _LOG("");
    my $resource = shift;
    defined($resource) or confess; # "" is root collection
    my $path = resource_to_path($resource) or 
        die "500 Server: Error. Resource '$resource' has no path";
    -r $path or die "500 Server: Error. Resource '$resource' has no path";
    my @stat = stat($path);
    my $ret = $stat[9];
    # _LOG("getlastmodified \$path: $path \$ret $ret \@stat ".join(", ", @stat));
    return $ret;
}    
sub get_getcontentlength( $ ){
    my $resource = shift;
    defined($resource) or die; # "" is root collection 
    my $path = resource_to_path($resource) or 
        die "500 Server: Error. Resource '$resource' has no path";
    -e $path or die "$!: Asking for 'getcontentlength' on ".
        "'$resource' at '$path' that does ot exist";
    my $collection = is_collection($resource);
    return $collection?0:-s $path;
}

sub get_getcontenttype( $ ) {
    my $resource = shift;
    defined($resource) or die; # "" is root collection 
    my $ret = ResourceStore::read_property($resource, "getcontenttype", "LIVE");
    defined($ret) or $ret = ""; # FIXME This should be a error or have a default
    return $ret;
}
sub get_getcontentlanguage( $ ) {
    my $resource = shift;
    defined($resource) or die; # "" is root collection 
    my $ret = ResourceStore::read_property($resource, "getcontentlanguage", "LIVE");
    # FIXME This default is set in handle_PUT
    defined($ret) or $ret = "en-UK";
    return $ret;
}    
sub get_displayname( $ ) {
    my $resource = shift;
    defined($resource) or die; # "" is root collection 
    $resource =~ /([^\/]+)$/ or die "\$resource '$resource' is invalid";
    my $ret = $1;
    return $ret;
}
sub get_creationdate( $ ) {
    my $resource = shift;
    defined($resource) or die; # "" is root collection
    return get_mod_time($resource);
}
sub get_mod_time( $ ){
    my $resource = shift;
    defined($resource) or die; # "" is root collection 
    my $path = resource_to_path($resource) or 
        die "500 Server Error: Resource '$resource' has ano path";
    -e $path or die "500 Server Error: Resource '$resource' has a ".
        "path: '$path' that does not exist";
    my @stat = stat($path);
    return $stat[9]; 
}    
    
sub get_att( $$ ){
    my $resource = shift;
    defined $resource or confess;
    my $att_name = shift or confess;
    $att_name =~ /[a-z_]+/ or confess "$att_name"; # This is evaluated    
    my $s = "get_$att_name(\$resource)";
    my $ret = eval $s;
    $@ and confess $@;
    return $ret;    

}

sub is_collection( $ ){
    my $resource = shift or confess;
    return get_resource_type($resource) eq 'collection';
}
sub _editPropertyf( $$$$ ){
    # Passed a resource (arg 1), a property name (arg 2) and a new
    # value (arg3) set the value of the property to the value.  If the
    # property does not exist create it.  If the resource does not
    # exist, die
    my $resource = shift;
    defined($resource) or die; # "" is root collection 
    my $name = shift or confess;
    my $value = shift;
    my $fh = shift or confess;

    _deletePropertyf($resource, $name, $fh);
    _addPropertyf($resource, $name, $value, $fh);

}
sub edit_property( $$$$ ){
    my $resource = shift;
    defined($resource) or die; # "" is root collection 
    my $name = shift or confess;
    my $value = shift;
    defined($value) or confess;
    my $which = shift or confess; #LIVE or DEAD
    my $fh = _lockProperties($which);

    my $ret = _editPropertyf($resource, $name, $value, $fh);
    close($fh)  or die "$!: '$LIVE_PROPERTIESDBFN'";
    return $ret;
    
}
sub _deletePropertyf( $$$ ){
    # Passed a resource and a property name delete the property and
    # its value from the database and return the property's old value
    # or undef if the property does not exist. 
    my $resource = shift;
    defined($resource) or die; # "" is root collection 

    # All resources have the trailing slash removed.  This is a
    # problem for the root collection that is then an empty string,
    # and we cannot put it in the file.
    # 20161111 Changed root resource to "ROOT"
    # $resource eq "" and $resource = "/";
    

    my $propertyName = shift or confess;
    my $fh = shift or confess;
    binmode($fh, ":utf8");

    my $ret = undef;
    
    my $state = 0; # Control reading file and check this after the
    # eval loop for success at finding the resource
    eval{
        seek($fh, 0, 0) or die "$!: Cannok seek on properties";
        my $newf = '';
        foreach my $line (<$fh>){
            $line =~ /^\s*#/ and next; # Comments
            chomp $line;
            
            if($state == 0){
                # When we are looking for the start of the block for this
                # resource
                if($line eq $resource){
                    # Found the block with this resource
                    $state = 1; 
                }elsif($line =~ /\S/){
                    # This block is a different resource
                    $state = -1; 
                }
            }elsif($state == -1){
                # in the wrong resource block
                if($line =~ /^\s*$/){
                    # Finished this resource block.  Reset state to start
                    # looking for the correct resource block again
                    $state = 0;
                }
            }elsif($state == 1){
                # in the correct resource block.
                if($line =~ /^\s*(\S[^\t]*)\s*\t\s*(.+)/){
                    # This line is a property line
                    if($1 eq $propertyName){
                        # Found the value to selete.
                        $ret = $2; # Store old value to return
                        # May be a multi-line value so set state to 3
                        $state = 3;
                        next; # Do not put line in new version of file
                    }
                }elsif($line =~ /^\s*$/){
                    # Blank line.  End of correct resource block
                    
                    # Set the state to 2 that is not detected so the rest
                    # of the properties will be added unchanged
                    $state = 2;
                }
            }elsif($state == 3){
                # Collecting a multi-line value
                if($line =~ /^\S/ or $line =~ /^\s*$/){
                    # Finished
                    $state = $line =~ /^\s*$/?0:1;
                }else{
                    $ret .= $line;
                    next; # Do not put back in file
                }
                if($line =~ /^\s*$/){
                    # Finished block.  Set state to 2 that is never
                    # tested for so the rest of the file is copied
                    $state = 2;
                }
            }
            $newf .= $line."\n";
        }

        # Write the new contents
        truncate($fh, 0) or die "$!: Cannot truncate properties";
        seek($fh, 0, 0, ) or die "$!: Cannot seek in properties database";
        print($fh $newf) or die "$!: Cannot write out new properties";
    };
    if($@){
        _LOG($@);
        $ret = undef;
    }

    # If state == 0 or -1 the resource was not found
    if($state != 2){
        die "400 Bad Request: Resource '$resource' not found in properties database";
    }
    return $ret;
}		    
sub delete_property( $$$ ) {
    my $resource = shift;
    defined($resource) or die; # "" is root collection 
    my $propertyName = shift or confess;
    my $which = shift or confess; #LIVE or DEAD
    my $fh = _lockProperties($which);
    my $ret = _deletePropertyf($resource, $propertyName, $fh);
    _checkForOrphanProperties();
    close($fh)  or die "$!: '$LIVE_PROPERTIESDBFN'";
    return $ret;
}
sub _readPropertyf( $$$ ){
    # Passed a resource and a property name return the property or
    # undef if the property does not exist.  
    my $resource = shift;
    defined($resource) or confess; # "" is root collection 

    # All resources have the trailing slash removed.  This is a
    # problem for the root collection that is then an empty string,
    # and we cannot put it in the file.
    # 20161111 Changed root resource to "ROOT"
    # $resource eq "" and $resource = "/";
    

    my $propertyName = shift or confess;
    my $fh = shift or confess; 
    binmode($fh, ":utf8");
    my $ret = undef;

    my $state = 0; # Control reading file and check this after the
    # eval loop for success at finding the resource
    eval{
        seek($fh, 0, 0) or die "$!: Cannok seek on properties";
        my $value; # Place to store (possibly multi-line)  value
        foreach my $line (<$fh>){
            chomp($line);
            $line =~ /^\s*#/ and next; # Comments
            
            if($state == 0){
                # When we are looking for the start of the block for this
                # resource
                if($line eq $resource){
                    # Found the block with this resource
                    $state = 1; 
                }elsif($line =~ /\S/){
                    # This block is a different resource
                    $state = -1; 
                }
            }elsif($state == -1){
                # in the wrong resource block
                if($line =~ /^\s*$/){
                    # Finished this resource block.  Reset state to start
                    # looking for the correct resource block again
                    $state = 0;
                }
            }elsif($state == 1){
                # in the correct resource block.
                if($line =~ /^(\S[^\t]*)\s*\t\s*(.*)/){
                    # This line is a property line
                    if($1 eq $propertyName){
                        # Found the value to select.
                        $value = $2; # Start storing value
                        $state = 3;
                        #last; # finished
                    }
                }elsif($line =~ /^\s*$/){
                    # Blank line.  End of correct resource block.
                    # Have not found value
                    $ret = undef;
                }
            }elsif($state == 3){
                # Building a multi-line value
                if($line =~ /^\S/ or $line =~ /^\s*$/){
                    # Finished
                    $ret = $value;
                    # only one value allowed for a property, so do not
                    # read any more
                    last;
                }else{
                    $value .= $line;
                }
            }else{
                die "Invalid state: \$state";
            }
        }
    };
    if($@){
        _LOG($@);
        $ret = undef;
    }

    return $ret;
}

sub read_property( $$$ ){
    my $resource = shift;
    defined($resource) or die; # "" is root collection 
    
    my $propertyName = shift or confess;
    my $which = shift or confess; #LIVE or DEAD
    my $fh = _lockProperties($which)  or die "$!: $which";
    my $ret = _readPropertyf($resource, $propertyName, $fh);
    close($fh)  or die "$!: '$which'";
    #$LOGLEVEL>1 and _LOG("read_property: \$resource $resource \$propertyName $propertyName \$which $which -> $ret");
    return $ret;
}

sub _addPropertyf( $$$$ ){

    # FIXME _addProperty and _deleteProperty share a lot of code.

    # For the resource (parameter 1), property name (parameter 2) and
    # property value (parameter 3) store the properties.

    # If the property existed return its old value, else undef.  On failure die
    my $resource = shift;
    defined($resource) or die; # "" is root collection 

    # All resources have the trailing slash removed.  This is a
    # problem for the root collection that is then an empty string,
    # and we cannot put it in the file.
    # 20161111 Changed root resource to "ROOT"
    # $resource eq "" and $resource = "/";
    

    my $name = shift or confess;
    my $value = shift;
    defined($value) or die; # May be 0 or ""
    my $fh = shift or confess;
    binmode($fh, ":utf8");

    # If $value is multi line then make insert a space at the start of
    # every line after the first one
    # FIXME Delete blank lines???  Can property values include blank
    # lines?
    $value =~ s/\n/\n /g;
    
    # 20150722 11:00 This is inefficient but simplifies my task as I
    # do not have to check if the property is there before I add it
    my $ret; # = _deletePropertyf($resource, $name, $fh);

    my $state = 0; # Control reading file and check this after the
    # eval loop for success at finding the resource
    eval{
        seek($fh, 0, 0) or die "$!: Cannok seek on properties";
        my $newf = '';
        foreach my $line (<$fh>){

            $line =~ /^\s*#/ and next; # Comments
            chomp $line;
            if($state == 0){
                # When we are looking for the start of the block for this
                # resource
                if($line eq $resource){
                    # Found the block with this resource
                    $state = 1; 
                }elsif($line =~ /\S/){
                    # In wrong block
                    $state = -1;
                }
            }elsif($state == -1){
                # in the wrong resource block
                if($line =~ /^\s*$/){
                    # Finished this resource block.  Reset state to start
                    # looking for the correct resource block again
                    $state = 0;
                }
            }elsif($state == 1){
                # in the correct resource block.  If the line is the
                # line describing this resource replace it .  If it is
                # a blank line then this is a new property so add it
                if($line =~ /^$name\t(.*)$/){
                    $line = "$name\t$value";
                    $ret = $1;
                    # State 2 is used to clean out the remains of a
                    # multy line value
                    $state = 2;
                }elsif($line !~ /\S/){
                    $newf .= "$name\t$value\n";
                    $state = 3;
                    # This state is never detected so the loop runs
                    # out to the end appending all the properties
                }
            }elsif($state == 2){
                # clean out the remains of a multy line value
                if($line =~ /^(\s.+)/){
                    # Maintian the old property value to return
                    $ret .= "\n$1";
                }else{
                    $state = 3; # Multi line value is finished
                }
            }
            $newf .= $line."\n";
        }
        # If state is 1 then the property URI was the last line in the file
        $state == 1 and $newf .= "\n$name\t$value\n";

        # Write the new contents
        truncate($fh, 0) or die "$!: Cannot truncate properties";
        seek($fh, 0, 0, ) or die "$!: Cannot seek in properties database";
        print($fh $newf) or die "$!: Cannot write out new properties";
        #$ret = 1;
    };
    if($@){
        _LOG($@);
    }
    # If state == 0 or -1 the resource was not found
    if($state == 0 or $state == -1){
        die "400 Bad Request: Resource '$resource' not found in properties databas.  State: '$state'";
    }
    return $ret;
} 
sub add_property( $$$$ ){
    my $resource = shift;
    defined($resource) or die; # "" is root collection 
    my $name = shift or confess;
    my $value = shift;
    defined($value) or die; # May be 0 or ""
    my $which = shift or confess; #LIVE or DEAD

    if($which eq "DEAD"){
        if(ref($name) =~ /^XML:LibXML:/){
            # An XML object
            $name = $name->toString();
            $LOGLEVEL > 2 and _LOG("FIXME _addProperty called with a name that is a ".
                                  "XML::LibXML object:'$name' \$value '$value'");
        }elsif(ref($name) ne ""){
            # A string is OK.  Anything else a type error
            die "500 Server Error:_addProperty Property name invalid. ".
                "ref(\$name) is '".ref($name)."' which is not understood";
        }
    }
    ref($name) eq "" or die "500 Server Error:\$name '$name' ref(\$name) '".
        ref($name)."'";
    my $fh = _lockProperties($which);
    my $ret = _addPropertyf($resource, $name, $value, $fh);
    close($fh)  or die "$!: '$LIVE_PROPERTIESDBFN'";
    return $ret;
}
sub _listPropertiesf( $$ ){
    my $resource = shift;
    defined($resource) or die; # "" is root collection 

    # All resources have the trailing slash removed.  This is a
    # problem for the root collection that is then an empty string,
    # and we cannot put it in the file.
    # 20161111 Changed root resource to "ROOT"
    # $resource eq "" and $resource = "/";
    

    my $fh = shift or confess;

    my @ret = ();
    my $state = 1; # Next line is a property
    while(my $line = <$fh>){
        chomp $line;
        $line =~ /^\s*\#/ and next; # Comment
        if($state == 1){
            # $line is a resource name
            $line eq $resource and $state = 2;
            next;
        }elsif($state == 2){
            # This is a property definition
            if($line =~ /^([^\t]+)\t/){
                push(@ret, $1);
                next;
            }elsif($line =~ /^\s*$/){
                $state = 1;
                next;
            }
        }
    }
    return @ret;
}
sub _createPath( $$ ){
    # Used by _createCollection and _createResource this gets a path
    # for the resource.  The path does not exist in the file system
    # when it is returned
    my $resource = shift;
    defined($resource) or confess; # Root allowed
    my $type = shift or confess;
    $type eq 'resource' or $type eq 'collection' or
        confess "Type: '$type' incorrect";
    my $ret =  _addToResourceTables($resource, $type);
    return $ret;
}

sub list_properties( $;$ ){
    my $resource = shift;
    defined($resource) or die; # "" is root collection 
    my $what = shift;
    defined($what) or $what = "";

    # If $what eq "" return a HASH.  2-keys: LIVE and DEAD.  Values
    # are ARRAY refs of property names.  If $what eq "LIVE" or $what
    # eq "DEAD" return an array of property names
    my @live;
    my @dead;
    $what and $what ne "LIVE" and $what ne "DEAD" and
        die "Unknown value of \$what: '$what'";
    

    if($what eq "" or $what eq "LIVE"){
        my $fh = _lockProperties("LIVE");
        @live = _listPropertiesf($resource, $fh);
        _unlockProperties($fh);
    }

    if($what eq "" or $what eq "DEAD"){
        my $fh = _lockProperties("DEAD");
        @dead = _listPropertiesf($resource, $fh);
        _unlockProperties($fh);
    }
    if($what eq ""){
        return (LIVE => \@live, DEAD => \@dead);
    }else{
        $what eq "LIVE" and return @live;
        $what eq "DEAD" and return @dead;
        die "Unknown value of \$what: '$what'";
    }
    die "Reality interuption...";
}
sub generate_etag( $ ) {
    my $resource = shift;
    defined($resource) or confess; # "" is root collection 

    # FIXME: Is this too resource intensive?  We could generate this
    # when we write the file and avoid re reading it here
    my $path = resource_to_path($resource) or 
        die "500 Server: Error. Resource '$resource' has no path";
    -r $path or die "$!: Cannot read '$path'";
    my $etag;
    my $raw = $path;
    my @_stat = stat($path) or die "$!: Cannot stat '$path'";
    $raw .= $_stat[9]; # Last modify tie in hires
    $etag = unix_md5_crypt($raw, "SALT");
    my $ret =  "etag:$etag";
    return $ret;
}    

sub resource_exists( $ ) {
    my $resource = shift or confess;
    my $fn = resource_to_path($resource);
    defined($fn) and -e $fn and return 1;
    return 0;
}
sub create_collection( $ ) {
    my $resource = shift or confess; 
    my $path = add_resource($resource, "collection");

    if(defined($path)){
        my $_r = mkdir($path) or confess "500 Internal Server Error: '$!' mkdir('$path')";
        
    }
    $resource eq $ROOT and _LOG("create_collection( '$ROOT' )");
    initialise_resource_property($resource);

    # FIXME Should this be called here?
    _setLiveProperties($resource);

    return $path;
}
sub get_parent( $ ){

    # Returns the parent collection of the passed resource.  If there
    # is no parent (the resource is root) return an empty string.  If
    # the parent does not exist return undef.  FIXME Should die as
    # that is an error
    my $resource = shift or confess;
    my $ret;
    $LOGLEVEL>2 and _LOG("get_parent($resource)");

    if($resource =~ /^(.+)\/([^\/]+)\/?$/){
	$LOGLEVEL>2 and _LOG("get_parent($resource) \$1 $1 \$2 $2");
	get_resource_type($1) eq 'collection' or confess "A parent is not a collection: '$1'";
	$ret = $1;
    }elsif($resource =~ /^$ROOT\/?/){
        $ret = "";
    }else{
        confess "Cannot understand resource '$resource'";
    }
    $LOGLEVEL>2 and _LOG("get_parent($resource) -> $ret");
    return $ret;
}
sub create_resource( $ ) {
    ## PRECONDITION: The caller is authorised to create this resource.
    ## No checks here!
    my $resource = shift or confess;
    resource_exists($resource) and return 1;
    # Create the resource as a empty file 
    my $path;
    eval {
	$LOGLEVEL>2 and _LOG("create_resource($resource)");
	$path = add_resource($resource, 'resource');
    };
    if($@){
	confess $@;
    }
    #my $path = resource_to_path($resource);
    open(my $_fh, ">$path") or confess "$! Cannot open '$path' for '$resource'";
    close($_fh) or confess "Cannot close '$path'  for '$resource'";
    $resource eq "$ROOT" and _LOG("create_resource( $ROOT )");
    initialise_resource_property($resource);
    $LOGLEVEL>2 and _LOG("create_resource($resource)");

    return 1;
}

sub put( $ ) {
    # Read a resource named in forst argument using subroutine passed
    # in second argument and store it. The resource is a file, the
    # principal is authenticated
    my $resource = shift or confess;

    my ($code, $message) = (201, "Created");
    create_resource($resource);
    my $fn = resource_to_path($resource);
    open(my $OUT, ">$fn") or die 
	"500 Internal Server Error:".
	"Could not PUT resource: '$resource': $!";
    flock($OUT, LOCK_EX) or die "$!: Cannot lock '$fn'";
    while(1){
	# Large payloads have to be read in sections.  _readSTDIN
	# will only read up to $READLIMIT bytes
	my $content = _readSTDIN();
	if($content){
	    print $OUT $content;
	}else{
	    last;
	}
    }
    
    close $OUT  or die 
	"500 Internal Server Error:".
	"Could not PUT resource: '$resource': $!";

    # Initialise properties
    _setLiveProperties($resource);
    return ($code, $message);
}   
sub get( $ ) {
    my $resource = shift or confess;
    my $content;
    if(!is_collection($resource)){
	# An ordinary resource (file) Read the content and
	# return it
	my $path = resource_to_path($resource);
	open(my $fh, "<", $path) or confess "$!: '$path'";
	flock($fh, LOCK_EX) or confess "$!: Cannot lock '$path'";
	$content = join("", <$fh>);
	close($fh) or confess "$!: '$path'";
    }else{
	# It is a collection.   Return that information.
	# FIXME  Do better:  A listing maybe
	$content = "$resource is a collection";
    }
    return $content;
} 

1;

