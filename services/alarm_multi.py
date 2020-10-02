"""
alarm_multi.py

An example of a WeeWX service that implements multiple alarms.

This code is based on the multiple alarm service created by user William Phelps
in 2013 in the following WeeWX user group post:
https://groups.google.com/d/msg/weewx-user/-IGQC3CpXAE/ItUpebyZlL8Jalarm_multi.
The multiple alarm service was in turn based on the then example alarm service
included in WeeWX v2.3.x which is copyright (c) 2009-2015 Tom Keffer
<tkeffer@gmail.com>.

William Phelps original multiple alarm service was subsequently modified by
Gary Roderick as follows:
-   on 6 April 2017 to work under WeeWX v3.7.1
-   on 21 September 2020 to work under WeeWX 4.x and python 2/3 and to include
    mail transport changes incorporated in the original example alarm service
    since WeeWX v2.3.x

Further changes were made in October 2020 and version numbering adopted
starting at v2.0.0

Version: 2.0.0                                        Date: 1 October 2020

  Revision History
    1 October 2020      v2.0.0
        -   implemented the 'include_full_record' config item which controls
            whether the full archive record or an abbreviated archive record is
            included in the alarm email message body
        -   restructured alarm parsing removing the need for the 'count' config
            item
        -   restructured imports
        -   renamed class MyAlarm to AlarmMulti
        -   renamed some methods/variable to quieten pycharm complaints
        -   reformatted/rewrote lead in comments/instructions
        -   minor reformatting of email body
        -   reworked --help output

Abbreviated instructions for use:

To configure this service, add the following to the WeeWX configuration file
weewx.conf:

[Alarm]
    time_wait = 3600
    smtp_host = smtp.example.com
    smtp_user = myusername
    smtp_password = mypassword
    from = sally@example.com
    mailto = jane@example.com, bob@example.com
    expression.0 = "outTemp < 40.0"
    subject.0 = "Alarm message from WeeWX - Low temperature!"
    expression.1 = "outTemp > 90.0"
    subject.1 = "Alarm message from WeeWX- High temperature!"

In this example, if the outside temperature falls below 40, or rises above 90,
it will send an email to the the comma separated list specified in option
'mailto', in this case jane@example.com and bob@example.com.

The example assumes an SMTP email server at smtp.example.com that requires
login.  If the SMTP server does not require login, leave out the lines for
smtp_user and smtp_password.

Setting an email "from" is optional. If not supplied, one will be filled in,
but your SMTP server may or may not accept it.

Setting an email "subject" is optional. If not supplied, one will be filled in.

To avoid a flood of emails, emails will only be sent every 3600 seconds (one
hour).

The option include_full_record can be used to control whether the full archive
record is included in any alarm emails or whether to include only those fields
involved in the triggered alarm expression. Optional, set to True or False.
Default is true.


To enable this service:

1.  copy this file to the user directory

2.  modify the WeeWX configuration file by adding this service to the
"report_services" option, located in section [Engine] [[Services]], eg:

[Engine]
    [[Services]]
        ...
        report_services = weewx.engine.StdPrint, weewx.engine.StdReport, user.alarm_multi.MyAlarm

3.  restart WeeWX

Note:
    If you wish to use both this example and the lowBattery.py example, simply
    merge the two configuration options together under [Alarm] and add both
    services to report_services.
"""

# python imports
from __future__ import print_function

import smtplib
import socket
import threading
import time
from email.mime.text import MIMEText

# WeeWX imports
import weewx
import weewx.engine
import weeutil.weeutil

# import/setup logging, WeeWX v3 is syslog based but WeeWX v4 is logging based,
# try v4 logging and if it fails use v3 logging
try:
    # WeeWX4 logging
    import logging
    from weeutil.logger import log_traceback

    log = logging.getLogger(__name__)

    def logcrit(msg):
        log.critical(msg)

    def logdbg(msg):
        log.debug(msg)

    def logerr(msg):
        log.error(msg)

    def loginf(msg):
        log.info(msg)

    # log_traceback() generates the same output but the signature and code is
    # different between v3 and v4. We only need log_traceback at the log.error
    # level so define a suitable wrapper function.

    def log_traceback_error(prefix=''):
        log_traceback(log.error, prefix=prefix)

except ImportError:
    # WeeWX legacy (v3) logging via syslog
    import syslog
    from weeutil.weeutil import log_traceback

    def logmsg(level, msg):
        syslog.syslog(level, 'alarm: %s' % msg)

    def logcrit(msg):
        logmsg(syslog.LOG_CRIT, msg)

    def logdbg(msg):
        logmsg(syslog.LOG_DEBUG, msg)

    def logerr(msg):
        logmsg(syslog.LOG_ERR, msg)

    def loginf(msg):
        logmsg(syslog.LOG_INFO, msg)

    # log_traceback() generates the same output but the signature and code is
    # different between v3 and v4. We only need log_traceback at the log.error
    # level so define a suitable wrapper function.

    def log_traceback_error(prefix=''):
        log_traceback(prefix=prefix, loglevel=syslog.LOG_ERR)

ALARM_MULTI_VERSION = '2.0.0'


# Define the MyAlarm class which is inheritted from the base class StdService
class AlarmMulti(weewx.engine.StdService):
    """Service to send an email if any one of multiple expressions evaluate true."""

    # define the default record content if an abbreviated recor dis included in
    # the alarm email body
    default_manifest = ['dateTime', ]

    def __init__(self, engine, config_dict):
        # pass the initialization information on to my superclass
        super(AlarmMulti, self).__init__(engine, config_dict)

        try:
            alarm_config = config_dict['Alarm']
            # Dig the needed options out of the configuration dictionary.
            # If a critical option is missing, an exception will be raised and
            # the alarm will not be set.
            # get the minimum time between alarm emails, default to one hour
            self.time_wait = int(alarm_config.get('time_wait', 3600))
            # get the timeout when waiting for a server to respond, default
            # to 10 seconds
            self.timeout = int(alarm_config.get('timeout', 10))
            self.smtp_host = alarm_config['smtp_host']
            self.smtp_user = alarm_config.get('smtp_user')
            self.smtp_password = alarm_config.get('smtp_password')
            # get the from address, use a default if not specified
            self.FROM = alarm_config.get('from', 'alarm@example.com')
            self.TO = weeutil.weeutil.option_as_list(alarm_config['mailto'])

            self.last_msg_ts = {}
            self.expression = {}
            self.subject = {}
            # construct/populate a number of dicts to support the multiple alarm
            # expressions
            for scalar in alarm_config.scalars:
                if 'expression.' in scalar.lower():
                    _i = scalar.split('.')[1]
                    i = int(_i)
                    self.last_msg_ts[i] = 0
                    self.expression[i] = alarm_config[scalar]
                    # get the subject, use a default if not specified
                    self.subject[i] = alarm_config.get('.'.join(['subject', _i]),
                                                       "Alarm message from WeeWX")
                    # log the expression to be used
                    loginf("Alarm set for expression %s: \"%s\"" % (_i,
                                                                    self.expression[i]))
            # do we include the full archive record in the email body or an
            # abbreviated version based on the alarm expression
            self.full_rec = weeutil.weeutil.to_bool(alarm_config.get('include_full_record',
                                                                     True))
            # if we got this far, it's ok to start intercepting events
            self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)
        except KeyError as e:
            # we had a missing parameter for which we do not have a suitable
            # default so log it and abort our loading
            loginf("No alarm set.  Missing parameter: %s" % e)

    def new_archive_record(self, event):
        """Called on a new archive record event."""

        # Tto avoid a flood of nearly identical emails, this will do the check
        # only if we have never sent an email, or if we haven't sent one in the
        # last self.time_wait seconds
        for key in self.expression.keys():
            if not self.last_msg_ts[key] or abs(time.time() - self.last_msg_ts[key]) >= self.time_wait:
                # get the new archive record
                record = event.record
                # be prepared to catch an exception in the case that the
                # expression contains a variable that is not in the record
                try:
                    # Evaluate the expression in the context of the event
                    # archive record. Sound the alarm if it evaluates true.
                    if eval(self.expression[key], None, record):
                        # sound the alarm!
                        # launch in a separate thread so it doesn't block the
                        # main LOOP thread
                        t = threading.Thread(target=MyAlarm.sound_the_alarm,
                                             args=(self, record, self.expression[key], self.subject[key]))
                        t.start()
                        # record when the message went out
                        self.last_msg_ts[key] = time.time()
                except NameError as e:
                    # The record was missing a named variable. Write a debug
                    # message, then keep going
                    logdbg("%s" % e)

    def sound_the_alarm(self, rec, expr, subj):
        """Sound the alarm."""

        # wrap in a 'try' block so we can catch and log any failure
        try:
            self.do_alarm(rec, expr, subj)
        except socket.gaierror:
            # a gaierror exception is usually caused by an unknown host
            logcrit("unknown host %s" % self.smtp_host)
            # Reraise the exception. This will cause the thread to exit.
            raise
        except Exception as e:
            # some other exception occurred, log it and reraise
            logcrit("unable to sound alarm. Reason: %s" % e)
            # Reraise the exception. This will cause the thread to exit.
            raise

    def do_alarm(self, rec, expr, subj):
        """Send an alarm email."""

        # get the time and convert to a string
        t_str = weeutil.weeutil.timestamp_to_string(rec['dateTime'])
        # log the alarm
        loginf("Alarm expression \"%s\" evaluated True at %s" % (expr, t_str))
        # include the full archive record in the email body or just the fields
        # of interest
        if self.full_rec:
            # full record
            msg_rec = rec
            # create an appropriate message body text
            msg_str = "Alarm expression '%s' evaluated True at %s\n\nRecord: %s"
        else:
            # Just the fields of interest. Perform a simple search of the alarm
            # expression for any archive record keys, this is a fairly basic
            # search and may be prone to false triggers on similarly named
            # fields but it will do the job.
            # Start with the default list of fields
            manifest = list(MyAlarm.default_manifest)
            # iterate over the archive record keys looking for the key in the
            # alarm expression.
            for key in rec.keys():
                if key in expr:
                    # found an occurrence, add the key to our manifest
                    manifest.append(key)
            # now construct an abbreviated record to use in the email body
            msg_rec = {}
            # add archive record fields to our message record for any keys in
            # our manifest
            for key in manifest:
                msg_rec[key] = rec[key]
            # create an appropriate message body text
            msg_str = "Alarm expression '%s' evaluated True at %s\n\nAbbreviated record: %s"
        # form the message text
        msg_text = msg_str % (expr,
                              t_str,
                              weeutil.weeutil.to_sorted_string(msg_rec))
        # convert to MIME
        msg = MIMEText(msg_text)
        # fill in the MIME headers
        msg['Subject'] = subj
        msg['From'] = self.FROM
        msg['To'] = ','.join(self.TO)
        try:
            # first try end-to-end encryption
            s = smtplib.SMTP_SSL(self.smtp_host, timeout=self.timeout)
            logdbg("using SMTP_SSL")
        except (AttributeError, socket.timeout, socket.error, ConnectionRefusedError):
            logdbg("unable to use SMTP_SSL connection.")
            # if that doesn't work, try creating an insecure host, then upgrading
            try:
                s = smtplib.SMTP(self.smtp_host, timeout=self.timeout)
                # be prepared to catch an exception if the server does not
                # support encrypted transport
                s.ehlo()
                s.starttls()
                s.ehlo()
                logdbg("using SMTP encrypted transport")
            except smtplib.SMTPException:
                # we can't use an encrypted transport try an unencrypted
                # transport
                logdbg("using SMTP unencrypted transport")
            except ConnectionRefusedError as e:
                # connection was refused, log it and reraise
                logdbg("Connection was refused: %s" % (e,))
                raise
        try:
            # if a username has been given, assume that login is required for this host
            if self.smtp_user:
                s.login(self.smtp_user, self.smtp_password)
                logdbg("logged in with user name %s" % self.smtp_user)
            # send the email
            s.sendmail(msg['From'], self.TO, msg.as_string())
            # log out of the server
            s.quit()
        except Exception as e:
            # we encountered an exception, log it and reraise
            logerr("SMTP mailer refused message with error %s" % e)
            raise
        # the email was successfully sent so log it
        loginf("email sent to: %s" % self.TO)


# for backwards compatibility
MyAlarm = AlarmMulti


if __name__ == '__main__':
    """This section is used to test alarm_multi.py. It uses a record and alarm
    expression that are guaranteed to trigger an alert.

    You will need a valid weewx.conf configuration file with an [Alarm]
    section that has been set up as illustrated at the top of this file.
    """

    from optparse import OptionParser
    import weecfg
    import weeutil

    usage = """Usage: python -m user.alarm-multi --help    
       python -m user.alarm-multi [CONFIG_FILE|--config=CONFIG_FILE]"""

    epilog = """You must be sure the WeeWX modules are in your PYTHONPATH.
For example:
PYTHONPATH=/home/weewx/bin python -m user.alarm-multi --help\n       

Depending on your system configuration your may also need to
replace 'python' in the above command with 'python2' or 'python3'
to ensure the correct python version is used."""

    weewx.debug = 1

    # Now we can set up the user customized logging but we need to handle both
    # v3 and v4 logging. V4 logging is very easy but v3 logging requires us to
    # set up syslog and raise our log level based on weewx.debug
    try:
        # assume v 4 logging
        weeutil.logger.setup('weewx', dict())
    except AttributeError:
        # must be v3 logging, so first set the defaults for the system logger
        syslog.openlog('alarm_multi.py', syslog.LOG_PID | syslog.LOG_CONS)
        # now raise the log level if required
        if weewx.debug > 0:
            syslog.setlogmask(syslog.LOG_UPTO(syslog.LOG_DEBUG))

    # create a command line parser
    parser = OptionParser(usage=usage,
                          epilog=epilog)
    parser.add_option("--config", dest="config_path", metavar="CONFIG_FILE",
                      help="Use configuration file CONFIG_FILE.")
    # parse the arguments and options
    (options, args) = parser.parse_args()

    try:
        config_path, config_dict = weecfg.read_config(options.config_path, args)
    except IOError as e:
        exit("Unable to open configuration file: %s" % e)

    print("Using configurdation file %s" % config_path)

    if 'Alarm' not in config_dict:
        exit("No [Alarm] section in the configuration file %s" % config_path)

    # this is a fake record that we'll use
    rec = {'extraTemp1': 1.0,
           'outTemp': 38.2,
           'dateTime': int(time.time())}

    # use an expression that will evaluate to True by our fake record
    config_dict['Alarm']['expression.1'] = "outTemp < 40.0"
    config_dict['Alarm']['subject.1'] = "outTemp is too low"
    config_dict['Alarm']['expression.3'] = "extraTemp1 > 0.0"
    config_dict['Alarm']['subject.3'] = "extraTemp1 is too high"

    # we need the main WeeWX engine in order to bind to the event, but we don't
    # need for it to completely start up. So get rid of all services
    config_dict['Engine']['Services'] = {}
    # now we can instantiate our slim engine...
    engine = weewx.engine.StdEngine(config_dict)
    # ... and set the alarm using it
    alarm = AlarmMulti(engine, config_dict)

    # create a NEW_ARCHIVE_RECORD event
    event = weewx.Event(weewx.NEW_ARCHIVE_RECORD, record=rec)

    # use it to trigger the alarm
    alarm.new_archive_record(event)
    print("Alarms triggered, check log and email for successful operation.")
