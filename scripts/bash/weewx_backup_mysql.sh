#!/bin/bash
##############################################################################
#
# Shell script to backup Weewx MySQL databases
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
# Version: 0.2.0                                    Date: 20 February 2018
#
# Revision History
#   20 February 2018    v0.2.0
#       - minor reworking for change from Odroid to RPi
#       - system name now a variable
#       - replaced one instance of a hard coded email address with variable
#   6 May 2016      v0.1.1
#       - added install/setup instructions
#       - added a few more parameters rather than hard coding further down
#       - minor formatting changes
#       - change backup destination
#       - removed colon (:) from tar.gz file name
#   16 March 2016   v0.1.0
#       - initial implementation
#
##############################################################################
#
# Key points:
#   - uses mysqldump to do a dump of each database
#   - the dump file is then compressed
#   - compressed backups older than DELETE_AGE are deleted
#   - backup start/end and limited operational details are logged to LOG_FILE
#   - email is sent to listed email address upon completion.
#
# To install/setup:
#   - copy this script to /usr/share/scripts (or wherever else you want)
#   - make sure the script can be executed:
#       $ sudo chmod ugoa+x /usr/share/scripts/weewx_backup_mysql.sh
#   - make sure the location for our .tar.gz backup file exists or mounted if 
#     on a remote system
#   - create file /root/mysqlpw containing the weewx MySQL password and make
#     it so that only root can read it:
#       $ su
#       $ cat "password" > /root/mysqlpw
#       $ chmod u=rw /root/mysqlpw
#       $ exit
#   - create a cron entry to run the script as required:
#       $ sudo crontab -e
#       add a line similar to:
#       32 00 * * * /usr/share/scripts/weewx_backup_mysql.sh
#
##############################################################################

######
#
# Initialise some variables
#
######

# System name
SYSTEM_NAME=cockatoo
# System name initial capital
SYSTEM_NAME_CAP=Cockatoo
# Databases to backup
DATABASES=(weewx cmon weewxwd wdsupp)
# Get the mysql password to be used
PW=`cat /root/mysqlpw`
# Root part of our backup file name
BACKUP_FILE_ROOT=weewx_mysql_data_
# Get the time of our backup
BACKUP_TIME=`date +%Y-%m-%d_%H-%M-%S`
# File name format for our .tar.gz backup files
BACKUP_FILE_NAME=$BACKUP_FILE_ROOT$BACKUP_TIME
# folder to save our .tar.gz backup files
BACKUP_FOLDER=/mnt/magpie_backup/weewx_database
# path and name of our log file
LOG_FILE="/var/log/weewx_backup.log"
# number of days to keep our backups for, anything older than this many days will be deleted
DELETE_AGE="+180"
# flag to use during email construction
BACKUP_ERROR=false
# file to hold our email content as we construct it
EMAIL_FILE="/var/tmp/weewx_backup_mail_content"
# file to hold our email body text as we construct it
EMAIL_BODY="/var/tmp/weewx_backup_mail_body"
# email address to send notifications to
EMAIL_TO="gary@therodericks.id.au"
# email address that sends notifications
# probably can be anything, not really checked
EMAIL_FROM=${SYSTEM_NAME}@therodericks.id.au
# delete any old email content
rm -f $EMAIL_FILE > /dev/null
rm -f $EMAIL_BODY > /dev/null
# start constructing our email mesage
printf "To: $EMAIL_TO\nFrom: $EMAIL_FROM\n" >> $EMAIL_FILE

######
#
# Log our start time
#
######

echo $(date +'%b %d %H:%M:%S') $SYSTEM_NAME weewx_backup_mysql.sh[$$]: weewx data backup started >> $LOG_FILE;

######
#
# Do some housekeeping
#
######

# Create our temp backup dir if it does not exist
mkdir -p /var/tmp/backups

######
#
# Dump our MySQL databases
#
######

for DB in "${DATABASES[@]}"; do
    /usr/bin/mysqldump -uroot -p$PW $DB > /var/tmp/backups/$DB.sql
    if [[ $? -ne 0 ]]; then
        echo $(date +'%b %d %H:%M:%S') $SYSTEM_NAME weewx_backup_mysql.sh[$$]: mysqldump of [$DB] failed error[$?] >> $LOG_FILE;
        echo $(date +'%b %d %H:%M:%S') $SYSTEM_NAME weewx_backup_mysql.sh[$$]: weewx data backup failed >> $LOG_FILE;
        # tailor our email message
        BACKUP_ERROR=true
        printf "Weewx data backup attempted at $BACKUP_TIME encountered an error - dump of $DB did not complete successfully." >> $EMAIL_BODY
        continue
    fi
done
echo $(date +'%b %d %H:%M:%S') $SYSTEM_NAME weewx_backup_mysql.sh[$$]: mysqldump complete >> $LOG_FILE;
    
######
#
# Tar up our files and place the .tar in our backup location
#
######

tar zvcf $BACKUP_FOLDER/$BACKUP_FILE_NAME.tar.gz -C / var/tmp/backups > /dev/null 2>&1
if [[ $? -ne 0 ]]; then
    echo [$?];
    echo $(date +'%b %d %H:%M:%S') $SYSTEM_NAME weewx_backup_mysql.sh[$$]: tar failed error[$?] >> $LOG_FILE;
    echo $(date +'%b %d %H:%M:%S') $SYSTEM_NAME weewx_backup_mysql.sh[$$]: weewx data backup failed >> $LOG_FILE;
    # Set our error flag
    BACKUP_ERROR=true
    # tailor our email message and send it before exiting
    printf "Weewx data backup attempted at $BACKUP_TIME failed - tar error. No files backed up." >> $EMAIL_BODY
else
    echo $(date +'%b %d %H:%M:%S') $SYSTEM_NAME weewx_backup_mysql.sh[$$]: tar complete >> $LOG_FILE;
fi

######
#
# Delete the local copies of the sqldump files
#
######

rm /var/tmp/backups/*

######
#
# Delete any backups older than our max age in days
#
######

find $BACKUP_FOLDER/$BACKUP_FILE_ROOT*.tar.gz -mtime $DELETE_AGE -exec rm {} \;

######
#
# Construct our email message and send it
#
######

if [ "$BACKUP_ERROR" = false ] ; then
    printf "Subject: $SYSTEM_NAME_CAP Weewx Data Backup - Success\n\nWeewx data backup $BACKUP_TIME completed successfully." >> $EMAIL_FILE
else
    printf "Subject: $SYSTEM_NAME_CAP Weewx Data Backup - Error\n\n$EMAIL_BODY" >> $EMAIL_FILE
fi
/usr/sbin/ssmtp $EMAIL_TO < $EMAIL_FILE

######
#
# Log our finish time
#
######

if [ "$BACKUP_ERROR" = false ] ; then
    echo $(date +'%b %d %H:%M:%S') $SYSTEM_NAME weewx_backup_mysql.sh[$$]: weewx data backup complete >> $LOG_FILE;
else
    exit 1
fi
