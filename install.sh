#!/bin/bash
set -x

# Set the location, in the container, of the WebDAV installation
PD_HOME=/home/dav/PerlDAV

# Build the container
lxc launch ubuntu:18.04 webdav


# Prepare a script that will wait for user `ubuntu` to be added
FN=/tmp/$$
echo Create $FN
cat <<EOF >$FN
A=''
until [ "\$A" ]
do
echo Try to find ubuntu user...
A=\`grep ubuntu /etc/passwd\`
if [ ! "\$A" ] 
then
sleep 1
fi
done
echo Found ubuntu user.
EOF
lxc file push  $FN webdav/tmp/testubuntu
rm $FN
lxc exec webdav -- chmod a+x /tmp/testubuntu


lxc exec webdav -- /tmp/testubuntu
lxc exec webdav -- rm /tmp/testubuntu

# Add the user to run PerlDAV as
lxc exec webdav -- useradd -m dav
lxc exec webdav -- usermod -a -G www-data dav

# Contents of PerlDAV.tgz
# WebDAV.pm
# webdav
# add_webDAV_user
# ResourceStore.pm
# WDGlobals.pm

lxc file push PerlDAV.tgz webdav/home/dav/
lxc exec webdav -- su - dav -c "mkdir $PD_HOME/"
lxc exec webdav -- su - dav -c "cd $PD_HOME ; tar xfzv /home/dav/PerlDAV.tgz "
lxc exec webdav -- su - dav -c "cd $PD_HOME ; ./initialiseDAV"

# Pause unitl the network is ready.  Prepare and execute a script that
# will wait for netwok interfaces to come up
FN=/tmp/$$
echo Create $FN
cat <<EOF >$FN
A=''
echo Network testing...
until [ "\$A" ]
do
echo Try network...
A=\`ip route\`
sleep 1
done
echo Network Established: \$A
EOF
lxc file push  $FN webdav/tmp/testnw
rm $FN
lxc exec webdav -- chmod a+x /tmp/testnw
lxc exec webdav -- cat /tmp/testnw
lxc exec webdav -- /tmp/testnw

# Update the system and install the software PerlDAV needs
lxc exec webdav -- apt update -y 
lxc exec webdav -- apt install aptitude -y
lxc exec webdav -- aptitude full-upgrade -y
lxc exec webdav -- aptitude install -y libxml-libxml-perl libdata-validate-uri-perl liburi-escape-xs-perl libcrypt-passwdmd5-perl libfile-mimeinfo-perl lighttpd

# Update lighttpd config file so it will run PerlDAV
lxc file pull webdav/etc/lighttpd/lighttpd.conf .
cat <<EOF >> lighttpd.conf
server.modules += ("mod_fastcgi",
                   "mod_accesslog",
                   "mod_auth")
accesslog.filename          = "/var/log/lighttpd/access.log"
server.breakagelog           = "/var/log/lighttpd/breakage.log"
fastcgi.server = (
 "/dav" =>
  ( "WebDAV" =>
    (
     "socket" => "/var/run/lighttpd/dav.socket",
     "bin-path" => "/home/dav/PerlDAV/webdav",
    )
  )
 )

auth.backend = "htdigest"
auth.backend.htdigest.userfile ="/home/dav/PerlDAV/$DAVPASSWD"
auth.require = ( "/" =>
    (
        "method"  => "digest",
        "realm"   => "WebDAV",
        "require" => "valid-user"
    ),
)
EOF

lxc file push lighttpd.conf webdav/etc/lighttpd/
rm lighttpd.conf

# Get PerlDAV running
lxc exec webdav -- service lighttpd restart

# Install the default user
lxc exec webdav -- su - dav -c "cd $PD_HOME ; ./add_webDAV_user dav dav PerlDAV"

# Create the file in the file system that fast CGI references.  I
# think this is a bug in fast CGI
lxc exec webdav -- touch /var/www/html/dav
