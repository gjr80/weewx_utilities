#!/bin/bash
##############################################################################
#
# Shell script to alert via email if weeWX-WD or weeWX may have died
#
# Copyright (C) 2016-2018 Gary Roderick             gjroderick<at>gmail.com
#
# This program is free software: you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License along with
# this program.  If not, see http://www.gnu.org/licenses/.
#
# Version: 0.1.3                                      Date: 17 February 2018
#
# Revision History
#   17 February 2018    v0.1.3
#       - minor reworking for change from Odroid to RPi
#       - fix outdated WEEWX_WD_FILE1 and WEEWX_WD_FILE2 settings
#   16 February 2017    v0.1.2
#       - minor reformatting of header info
#   6 May 2016          v0.1.1
#       - minor formatting changes
#   Unknown             v0.1
#       - initial implementation
#
##############################################################################
#
# Key points:
#   - checks the age of index.html (weeWX), testtags.php and clientraw.txt
#     (weeWX-WD)
#   - if any of the files is older than the MAX_AGE setting an email
#     notification is sent
#
# Returns:
#   - 0 if all files are younger than MAX_AGE minutes
#   - 1 if any file is younger than MAX_AGE minutes
#
# To install/setup:
#   - copy this script to /usr/share/scripts (or wherever else you want)
#   - make sure the script can be executed:
#       $ sudo chmod ugoa+x /usr/share/scripts/weewx_stale.sh
#   - create a cron entry to run the script as required:
#       $ sudo crontab -e
#       add a line similar to:
#       */15 * * * * /usr/share/scripts/weewx_stale.sh
#
##############################################################################


######
#
# Initialise some variables
#
######

# The files we care about
WEEWX_FILE=/var/www/html/weewx/weewx/index.html
WEEWX_WD_FILE1=/var/www/html/weewx/saratoga/data/testtags.php
WEEWX_WD_FILE2=/var/www/html/weewx/saratoga/data/clientraw.txt
# Maximum file age in minutes
MAX_AGE=10
# file to hold our email content as we construct it
EMAIL_FILE="/var/tmp/weewx_stale_mail_content"
# delete any old email content
rm -f $EMAIL_FILE > /dev/null
# start constructing our email message just in case
printf "To: gary@therodericks.id.au\nFrom: stormbird@therodericks.id.au\nSubject: Stormbird - Weewx Report Generation Problem\n\n" >> $EMAIL_FILE
# Initialise a flag to indicate whether we have anything to send. Start with nothing to send.
EMAIL_BODY=0

######
#
# Check our files and construct our email as required.
#
######

# Is our Weewx file too old?
if test `find "$WEEWX_FILE" -mmin +$MAX_AGE`
then
    # set our flag and add a line to our email message
    EMAIL_BODY=1
    printf "$WEEWX_FILE is older than $MAX_AGE minutes.\n" >> $EMAIL_FILE
fi

# Is our 1st Weewx-WD file too old
if test `find "$WEEWX_WD_FILE1" -mmin +$MAX_AGE`
then
    # set our flag and add a line to our email message    EMAIL_BODY=1
    EMAIL_BODY=1
    printf "$WEEWX_WD_FILE1 is older than $MAX_AGE minutes.\n" >> $EMAIL_FILE
fi

# Is our 2nd Weewx-WD file too old
if test `find "$WEEWX_WD_FILE2" -mmin +$MAX_AGE`
then
    # set our flag and add a line to our email message
    EMAIL_BODY=1
    printf "$WEEWX_WD_FILE2 is older than $MAX_AGE minutes.\n" >> $EMAIL_FILE
fi

######
#
# Send our email if necessary.
#
######

# If we have something to email then email and exit code 1
if [ "$EMAIL_BODY" -ne 0 ]
then
    /usr/sbin/ssmtp gary@therodericks.id.au < $EMAIL_FILE
    exit 1
fi
