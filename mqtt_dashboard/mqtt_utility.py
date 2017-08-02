# mqtt_utility.py
#
# A collection of classes and utilities to support MQTT_dashboard services.
#
# Copyright (C) 2017 Gary Roderick                  gjroderick<at>gmail.com
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
# Version: 0.1.0                                    Date: 30 July 2017
#
# Revision History
#  30 July 2017         v0.1.0  - initial release
#
"""A collection of classes and utilities to support MQTT_dashboard services.

MQTT publishing based on MQTT uploader by Matthew Wall.

This file contains the following classes:

1.  MQTTPublish. A class to manage publishing data to a MQTT broker.

2.  WeatherUndergroundAPI. A class for obtaining data via the Weather
    Underground API.

3.  Buffer. A class to buffer loop data and provide loop based aggregates.

4.  VectorBuffer. Supporting class to class Buffer to buffer vector data types.

5.  ScalarBuffer. Supporting class to class Buffer to buffer scalar data types.

6.  ObsTuple. A class to store an observation and the time it occurred.

7.  CachedPacket. A class to provide a cached loop packet.

8.  Various user defined error classes.

This file includes the following utility functions:

1.  calc_trend. Function to calculate the chnage in an observation over a
    period of time.

2.  obfuscate_password. Function to obfuscate a password in a URL string.

3.  Various logging supprot functions

Detailed descriptions of these classes and functions can be found in the
comments at the start of each class/function declaration.

To do:
    - fix raise weewx.restx.FailedPost in mqtt_post_data - maybe already fixed?
    - add TLS defaults as per Paho client
    - finish documenting TLS options
    - do Buffer, ScalarBuffer and VectorBuffer need to keep a MAX_AGE history?
      Should it be just Buffer of just the other? Seems to be duplication.
"""


# python imports
import math
import socket
import ssl
import syslog
import time
import urllib2
import urlparse

import paho.mqtt.client as mqtt


# weeWX imports
import weewx
import weewx.units

from weewx.units import ValueTuple, convert


def logmsg(level, msg):
    syslog.syslog(level, msg)


def logcrit(id, msg):
    logmsg(syslog.LOG_CRIT, '%s: %s' % (id, msg))


def logdbg(id, msg):
    logmsg(syslog.LOG_DEBUG, '%s: %s' % (id, msg))


def logdbg2(id, msg):
    if weewx.debug >= 2:
        logmsg(syslog.LOG_DEBUG, '%s: %s' % (id, msg))


def logdbg3(id, msg):
    if weewx.debug >= 3:
        logmsg(syslog.LOG_DEBUG, '%s: %s' % (id, msg))


def loginf(id, msg):
    logmsg(syslog.LOG_INFO, '%s: %s' % (id, msg))


def logerr(id, msg):
    logmsg(syslog.LOG_ERR, '%s: %s' % (id, msg))


# ============================================================================
#                     Exceptions that could get thrown
# ============================================================================


class MissingApiKey(IOError):
    """Raised when a WU API key cannot be found"""

class UnknownServer(IOError):
    """Raised when an invalid or missing server URL isprovided"""

class FailedPost(IOError):
    """Raised when a post fails after trying the max number of allowed times"""


# ============================================================================
#                            class MQTTPublish
# ============================================================================


class MQTTPublish(object):
    """A wrapper class to publish data to a MQTT broker using the Paho client.

    Based upon the weeWX MQTT uploader by Matthew Wall.

    This class supports publishing free form data to a MQTT broker using the
    Paho client with optional TLS support. An MQTTPublish object has a single
    method, publish(), that will publish the data concerned to the MQTT broker
    defined by the object.

    MQTTPublish constructor parameters:

        server:      Server URL to be used. String in the format:

                     mqtt://user:password@address:port/

                     where:
                        user:     The MQTT user name to be used.
                        password: The password for the MQTT user.
                        address:  The address or resolvable name of the MQTT
                                  server. Note that if using TLS the this
                                  setting may need to match the server name on
                                  the certificate used by the server.
                        port:     The port number on which the mQTT server is
                                  listening.

        tls:         TLS config options. Dictionary with one or more of the
                     following keys:

                     ca_certs:    Path to the trusted Certificate Authority
                                  certificate files. String.
                     certfile:    Path to PEM encoded client certificate.
                                  String.
                     keyfile:     Path to private keys. String.
                     cert_reqs:   Certificate requirements imposed on the
                                  broker. String, available options are 'none',
                                  'optional' or 'required'. Default is
                                  'required'.
                     tls_version: SSL/TLS protocol version to be used. String,
                                  supported options depend on the local OpenSSL
                                  install. Available (dependent on OpenSSL)
                                  options are:
                                      SSLV1
                                      SSLV2
                                      SSLV3
                                      SSLV23
                                      TLS
                                      TLSV1
                     ciphers:     Which encryption ciphers are allowable for
                                  this connection. String, default is to use
                                  the default encryption ciphers.

        retain:      Whether the published data will be set as the
                     "last known good"/retained message for the topic. Boolean,
                     default is False.
        max_tries:   Maximum number of attempts to publish data before giving
                     up. Integer, default is 3.
        retry_wait:  Time in seconds to wait between retries. Float, default
                     is 0.5.
        log_success: Whether to log successful publication or not. Boolean,
                     default is False.

    MQTTPublish methods:

        publish. Publish data to a MQTT broker.
    """

    # Define available options used in setting the Paho client to use TLS.
    # These options may need to change as/if Paho evolves.

    # TLS options accepted by Paho
    TLS_OPTIONS = [
        'ca_certs', 'certfile', 'keyfile',
        'cert_reqs', 'tls_version', 'ciphers'
        ]
    # map for TLS cert request options accepted by Paho
    CERT_REQ_OPTIONS = {
        'NONE': ssl.CERT_NONE,
        'OPTIONAL': ssl.CERT_OPTIONAL,
        'REQUIRED': ssl.CERT_REQUIRED
        }
    # Map for TLS version options accepted by Paho. Some options are dependent
    # on the local OpenSSL install so use try..except for some options.
    TLS_VER_OPTIONS = dict()
    try:
        TLS_VER_OPTIONS['sslv2'] = ssl.PROTOCOL_SSLv2
    except AttributeError:
        pass
    try:
        TLS_VER_OPTIONS['sslv3'] = ssl.PROTOCOL_SSLv3
    except AttributeError:
        pass
    TLS_VER_OPTIONS['sslv23'] = ssl.PROTOCOL_SSLv23
    TLS_VER_OPTIONS['tlsv1'] = ssl.PROTOCOL_TLSv1
    try:
        TLS_VER_OPTIONS['tls'] = ssl.PROTOCOL_TLS
    except AttributeError:
        pass

    def __init__(self, server, tls=None, retain=False,
                 max_tries=3, retry_wait=0.5, log_success=False):
        # initialise the MQTTPublish object

        # do we have a server specified?
        if server:
            self.server = server
        else:
            # no server specified, we cannot continue so raise the error
            raise UnknownServer("MQTT server URL not specified.")
        # gather any TLS options
        self.tls_dict = {}
        if tls is not None:
            # we have TLS options so construct a dict to configure Paho TLS
            for opt in tls:
                if opt == 'cert_reqs':
                    if tls[opt].upper() in self.CERT_REQ_OPTIONS:
                        self.tls_dict[opt] = self.CERT_REQ_OPTIONS.get(tls[opt].upper())
                    else:
                        logdbg("mqttpublish",
                               "Unknown option, ignoring cert_reqs option '%s'" % tls[opt])
                elif opt == 'tls_version':
                    if tls[opt].upper() in self.TLS_VER_OPTIONS:
                        self.tls_dict[opt] = self.TLS_VER_OPTIONS.get(tls[opt].upper())
                    else:
                        logdbg("mqttpublish",
                               "Unknown option, ignoring tls_version option '%s'" % tls[opt])
                elif opt in self.TLS_OPTIONS:
                    self.tls_dict[opt] = tls[opt]
                else:
                    logdbg("mqttpublish", "Unknown TLS option '%s'" % opt)

            logdbg("mqttpublish", "TLS parameters: %s" % self.tls_dict)
        # whether the published data is to be retained by the broker
        self.retain = retain
        # how many tries we will make to publish
        self.max_tries = max_tries
        # wait time between retries
        self.retry_wait = retry_wait
        # log successful posts?
        self.log_success = log_success

    def publish(self, topic, data, identifier):
        """Publish data to a MQTT broker.

        Publishes data to topic. Publish failures (socket.error, socket.timeout
        and socket.herror) that trigger a retry are logged. If publication
        fails after self.max_tries attempts a FailedPost exception is raised.

        Parameters:
            topic:      the topic to which the data is to be published
            data:       the data to be published
            identifier: an identifier (eg timestamp) used to identify a
                        particular publication in the system logs
                        (eg successful publishing)
        """

        # parse the MQTT server URL
        url = urlparse.urlparse(self.server)
        for _count in range(self.max_tries):
            try:
                # get a Paho client object
                mc = mqtt.Client()
                # if we have a user name and password supplied use them
                if url.username is not None and url.password is not None:
                    mc.username_pw_set(url.username, url.password)
                # if we have TLS options configure TLS on our broker connection
                if len(self.tls_dict) > 0:
                    mc.tls_set(**self.tls_dict)
                # connect to the MQTT broker
                mc.connect(url.hostname, url.port)
                # start the background loop() thread
                mc.loop_start()
                # publish the message
                (res, mid) = mc.publish(topic, data, retain=self.retain)
                # do ay error reporting/logging
                if res != mqtt.MQTT_ERR_SUCCESS:
                    # we encountered an MQTT broker error, log it
                    logerr("mqttpublish",
                           "MQTT publish failed for '%s': %s" % (topic, res))
                elif self.log_success:
                    # we are logging success so log it
                    loginf("mqttpublish",
                           "Published data(%s) to MQTT topic '%s'" % (identifier,
                                                                      topic))
                else:
                    # debug=2 so log it
                    logdbg2("mqttpublish",
                            "Published data(%s) to MQTT topic '%s'" % (identifier,
                                                                       topic))
                # we are done, stop the background loop() thread
                mc.loop_stop()
                # disconnect from the broker
                mc.disconnect()
                return
            except (socket.error, socket.timeout, socket.herror), e:
                logdbg("mqttpublish",
                       "MQTT publish attempt %d failed for %s: %s" % (_count+1,
                                                                      topic,
                                                                      e))
            # if we got here we had a failed post due to a non-MQTT broker
            # error (likely a network error) so sleep and try again
            time.sleep(self.retry_wait)
        else:
            # we couldn't post after self.max_tries so raise an error
            raise FailedPost("Failed upload after %d tries" %
                             (self.max_tries,))

# ============================================================================
#                        class WeatherUndergroundAPI
# ============================================================================


class WeatherUndergroundAPI(object):
    """Query the Weather Underground API and return the API response.

    The WU API is accessed by calling one or more features. These features can
    be grouped into two groups, WunderMap layers and data features. This class
    supports access to the API data features only.

    WeatherUndergroundAPI constructor parameters:

        api_key: WeatherUnderground API key to be used.

    WeatherUndergroundAPI methods:

        data_request. Submit a data feature request to the WeatherUnderground
                      API and return the response.
    """

    BASE_URL = 'http://api.wunderground.com/api'

    def __init__(self, api_key):
        # initialise a WeatherUndergroundAPI object

        # save the API key to be used
        self.api_key = api_key

    def data_request(self, features, query, settings=None, format='json', max_tries=3):
        """Make a data feature request via the API and return the results.

        Construct an API call URL, make the call and return the response.

        Parameters:
            features:  One or more WU API data features. String or list/tuple
                       of strings.
            query:     The location for which the information is sought. Refer
                       usage comments at start of this file. String.
            settings:  Optional settings to be included in te API call
                       eg lang:FR for French, pws:1 to use PWS for conditions.
                       String or list/tuple of strings. Default is 'pws:1'
            format:    The output format of the data returned by the WU API.
                       String, either 'json' or 'xml' for JSON or XML
                       respectively. Default is JSON.
            max_tries: The maximum number of attempts to be made to obtain a
                       response from the WU API. Default is 3.

        Returns:
            The WU API response in JSON or XML format.
        """

        # there may be multiple features so if features is a list create a
        # string delimiting the features with a solidus
        if features is not None and hasattr(features, '__iter__'):
            features_str = '/'.join(features)
        else:
            features_str = features

        # Are there any settings parameters? If so construct a query string
        if hasattr(settings, '__iter__'):
            # we have more than one setting
            settings_str = '/'.join(settings)
        elif settings is not None:
            # we have a single setting
            settings_str = settings
        else:
            # we have no setting, use the default pws:1 to make life easier
            # when assembling the URL to be used
            settings_str = 'pws:1'

        # construct the API call URL to be used
        partial_url = '/'.join([self.BASE_URL,
                                self.api_key,
                                features_str,
                                settings_str,
                                'q',
                                query])
        url = '.'.join([partial_url, format])
        # if debug >=1 log the URL used but obfuscate the API key
        if weewx.debug >= 1:
            _obf_api_key = '*'*(len(self.api_key) - 4) + self.api_key[-4:]
            _obf = '/'.join([self.BASE_URL,
                             _obf_api_key,
                             features_str,
                             settings_str,
                             'q',
                             query])
            _obf_url = '.'.join([_obf, format])
            logdbg("weatherundergroundapi",
                   "Submitting API call using URL: %s" % (_obf_url, ))
        # we will attempt the call max_tries times
        for count in range(max_tries):
            # attempt the call
            try:
                w = urllib2.urlopen(url)
                _response = w.read()
                w.close()
                return _response
            except (urllib2.URLError, socket.timeout), e:
                logerr("weatherundergroundapi",
                       "Failed to get '%s' on attempt %d" % (query, count+1))
                logerr("weatherundergroundapi", "   **** %s" % e)
        else:
            logerr("weatherundergroundapi",
                   "Failed to get Weather Underground '%s'" % (query, ))
        return None


# ============================================================================
#                             class Buffer
# ============================================================================


class Buffer(dict):
    """Buffer loop packet obs to facilitate limited loop aggregates.

    Archive based stats are an efficient means of obtaining stats for today.
    However, their use ignores any max/min etc (eg today's max outTemp) that
    'occurs' after the most recent archive record but before the next archive
    record is written to archive. For this reason selected loop data is
    buffered to enable 'loop' stats to be calculated. Accurate daily stats can
    then be determined at any time using a combination of archive based and
    loop based stats.

    The loop based stats are maintained over the period since the last archive
    record was generated. The loop based stats are reset each time an archive
    record is generated.

    Selected observations also have a history of loop value, timestamp pairs
    maintained to enable calculation of short term ma/min stats eg 'max
    windSpeed in last minute'. These histories are based on a moving window of
    a given period eg 10 minutes and are updated each time a looppacket is
    received.

    Buffer constructor parameters:

        day_stats:            An Accumulator (accum.Accumulator) object from
                              the main weeWX database initialised with todays
                              stats
        additional_day_stats: An Accumulator (accum.Accumulator) object from an
                              additional weeWX database initialised with todays
                              stats

    Buffer methods:

        seed_scalar.        Seed a ScalarBuffer object with today's stats.
        seed_vector.        Seed a VectorBuffer object with today's stats.
        seed_windrun.       Seed the windrun property with the day's windrun so
                            far.
        add_packet.         Add a packet to the buffer.
        add_value.          Add a scalar observation to the buffer.
        add_wind_value.     Add wind data to the buffer.
        clean.              Remove any outdated obs from the buffer history
                            list.
        start_of_day_reset. Reset the buffer stats.
        nineam_reset.       Reset any '9am based' stats in the buffer.
    """

    # the obs that we will buffer
    MANIFEST = ['outTemp', 'barometer', 'outHumidity', 'rain', 'rainRate',
                'humidex', 'windchill', 'heatindex', 'windSpeed', 'inTemp',
                'appTemp', 'dewpoint', 'windDir', 'UV', 'radiation', 'wind',
                'windGust', 'windGustDir']
    # obs for which we need hi/lo data
    HILO_MANIFEST = ['outTemp', 'barometer', 'outHumidity',
                     'humidex', 'windchill', 'heatindex', 'windSpeed', 'inTemp',
                     'appTemp', 'dewpoint', 'UV', 'radiation', 'windGust',
                     'windGustDir']
    # obs for which we need a history
    HIST_MANIFEST = ['windSpeed', 'windDir', 'wind']
    # obs for which we need a running sum
    SUM_MANIFEST = ['rain', 'wind']
    # maximum time (seonds) to keep an obs value
    MAX_AGE = 600

    def __init__(self, day_stats, additional_day_stats=None):
        """Initialise an instance of our class."""

        # seed our buffer objects from day_stats
        for obs in [f for f in day_stats if f in self.MANIFEST]:
            seed_func = seed_functions.get(obs, Buffer.seed_scalar)
            seed_func(self, day_stats, obs, obs in self.HIST_MANIFEST,
                      obs in self.SUM_MANIFEST)
        # seed our buffer objects from additional_day_stats
        if additional_day_stats:
            for obs in [f for f in additional_day_stats if f in self.MANIFEST]:
                if obs not in self:
                    seed_func = seed_functions.get(obs, Buffer.seed_scalar)
                    seed_func(self, additional_day_stats, obs,
                              obs in self.HIST_MANIFEST, obs in self.SUM_MANIFEST)
        self.primary_unit_system = day_stats.unit_system
        self.last_windSpeed_ts = None
        self.windrun = self.seed_windrun(day_stats)

    def seed_scalar(self, stats, obs_type, hist, sum):
        """Seed a scalar buffer."""

        self[obs_type] = init_dict.get(obs_type, ScalarBuffer)(stats=stats[obs_type],
                                                               units=stats.unit_system,
                                                               history=hist,
                                                               sum=sum)

    def seed_vector(self, stats, obs_type, hist, sum):
        """Seed a vector buffer."""

        self[obs_type] = init_dict.get(obs_type, VectorBuffer)(stats=stats[obs_type],
                                                               units=stats.unit_system,
                                                               history=hist,
                                                               sum=sum)

    def seed_windrun(self, day_stats):
        """Seed day windrun."""

        if 'windSpeed' in day_stats:
            # The wsum field hold the sum of (windSpeed * interval in seconds)
            # for today so we can calculate windrun from wsum - just need to
            # do a little unit conversion and scaling

            # The day_stats units may be different to our buffer unit system so
            # first convert the wsum value to a km_per_hour based value (the
            # wsum 'units' are a distance but we can use the group_speed
            # conversion to convert to a km_per_hour based value)
            # first get the day_stats windSpeed unit and unit group
            (unit, group) = weewx.units.getStandardUnitType(day_stats.unit_system,
                                                            'windSpeed')
            # now express wsum as a 'group_speed' ValueTuple
            _wr_vt = ValueTuple(day_stats['windSpeed'].wsum, unit, group)
            # convert it to a 'km_per_hour' based value
            _wr_km = convert(_wr_vt, 'km_per_hour').value
            # but _wr_km was based on wsum which was based on seconds not hours
            # so we need to divide by 3600 to get our real windrun in km
            windrun = _wr_km/3600.0
        else:
            windrun = 0.0
        return windrun

    def add_packet(self, packet):
        """Add a packet to the buffer."""

#        packet = weewx.units.to_std_system(packet, self.primary_unit_system)
        if packet['dateTime'] is not None:
            for obs in [f for f in packet if f in self.MANIFEST]:
                add_func = add_functions.get(obs, Buffer.add_value)
                add_func(self, packet, obs, obs in self.HILO_MANIFEST,
                         obs in self.HIST_MANIFEST, obs in self.SUM_MANIFEST)

    def add_value(self, packet, obs, hilo, hist, sum):
        """Add a value to the buffer."""

        if obs not in self:
            self[obs] = init_dict.get(obs, ScalarBuffer)(stats=None,
                                                         units=packet['usUnits'],
                                                         history=hist,
                                                         sum=sum)
        if self[obs].units == packet['usUnits']:
            _value = packet[obs]
        else:
            (unit, group) = weewx.units.getStandardUnitType(packet['usUnits'],
                                                            obs)
            _vt = ValueTuple(packet[obs], unit, group)
            _value = weewx.units.convertStd(_vt, self[obs].units).value
        self[obs].add_value(_value, packet['dateTime'], hilo, hist, sum)

    def add_wind_value(self, packet, obs, hilo, hist, sum):
        """Add a wind value to the buffer."""

        # first add it as 'windSpeed' the scalar
        self.add_value(packet, obs, hilo, hist, sum)

        # update today's windrun
        if 'windSpeed' in packet:
            try:
                self.windrun += packet['windSpeed'] * (packet['dateTime'] - self.last_windSpeed_ts)/1000.0
            except TypeError:
                pass
            self.last_windSpeed_ts = packet['dateTime']

        # now add it as the special vector 'wind'
        if 'wind' not in self:
            self['wind'] = VectorBuffer(stats=None, units=packet['usUnits'],
                                        history=True, sum=True)
        if self['wind'].units == packet['usUnits']:
            _value = packet['windSpeed']
        else:
            (unit, group) = weewx.units.getStandardUnitType(packet['usUnits'],
                                                            'windSpeed')
            _vt = ValueTuple(packet['windSpeed'], unit, group)
            _value = weewx.units.convertStd(_vt, self['wind'].units).value
        self['wind'].add_value((_value, packet.get('windDir')),
                               packet['dateTime'])

    def clean(self, ts):
        """Clean out any old obs from the buffer history."""

        for obs in self.HIST_MANIFEST:
            self[obs]['history_full'] = min([a.ts for a in self[obs]['history'] if a.ts is not None]) <= old_ts
            # calc ts of oldest sample we want to retain
            oldest_ts = ts - self.MAX_AGE
            # remove any values older than oldest_ts
            self[obs]['history'] = [s for s in self[obs]['history'] if s.ts > oldest_ts]

    def start_of_day_reset(self):
        """Reset our buffer stats at the end of an archive period.

        Reset our hi/lo data but don't touch the history, it might need to be
        kept longer than the end of the archive period.
        """

        for obs in self.MANIFEST:
            self[obs].day_reset()

    def nineam_reset(self):
        """Reset our buffer stats at the end of an archive period.

        Reset our hi/lo data but don't touch the history, it might need to be
        kept longer than the end of the archive period.
        """

        for obs in SUM:
            self[obs].nineam_reset()


# ============================================================================
#                             class VectorBuffer
# ============================================================================


class VectorBuffer(object):
    """Class to buffer vector type loop data.

    Buffer constructor parameters:

        stats:   The Accumulator object fields for today for the obs type
                 concerned.

        units:   WeeWX unit system code for the units used in this VectorBuffer
                 object. Default is None.

        history: Whether to keep a history for this obs. Boolean, default is
                 False.

        sum:     Whether to record 'sum' stats for the obs concerned. Boolean,
                 default is False.

    Buffer methods:

        add_value.       Add a vector value to the buffer.
        day_reset.       Reset the day stats for the obs concerned.
        nineam_reset.    Reset the 9am sum.
###        interval_reset.  What does this really do ?
        trim_history.    Trim any too old obs from the obs history.
        history_max.     Calculate the maximum value in the history data.
        history_avg.     Calculate the average of the value in the history
                         data.
        history_vec_avg. Calculate the vector average of the history data for
                         the obs concerned.
        day_vec_avg.     Calculate the day vector average value for the obs
                         concerend.
        day_vec_dir.     Calculate the day vector avereage direction for the
                         obs concerend.
    """

    default_init = (None, None, None, None, None)

    def __init__(self, stats, units=None, history=False, sum=False):
        self.units = units
        self.last = None
        self.lasttime = None
        if stats:
            self.day_min = stats.min
            self.day_mintime = stats.mintime
            self.day_max = stats.max
            self.day_max_dir = stats.max_dir
            self.day_maxtime = stats.maxtime
        else:
            (self.day_min, self.day_mintime,
             self.day_max, self.day_max_dir,
             self.day_maxtime) = VectorBuffer.default_init
        if history:
            self.history = []
            self.history_full = False
        if sum:
            if stats:
                self.day_sum = stats.sum
                self.day_xsum = stats.xsum
                self.day_ysum = stats.ysum
                self.sumtime = stats.sumtime
            else:
                self.day_sum = 0.0
                self.day_xsum = 0.0
                self.day_ysum = 0.0
                self.sumtime = 0.0
            self.nineam_sum = 0.0
            self.interval_sum = 0.0

    def add_value(self, val, ts, hilo=True, history=True, sum=True):
        """Add a value to my hilo and history stats as required."""

        (w_speed, w_dir) = val
        if w_speed is not None:
            if hilo:
                if self.day_min is None or w_speed < self.day_min:
                    self.day_min = w_speed
                    self.day_mintime = ts
                if self.day_max is None or w_speed > self.day_max:
                    self.day_max = w_speed
                    self.day_max_dir = w_dir
                    self.day_maxtime = ts
            if history and w_dir is not None:
                self.history.append(ObsTuple((w_speed,
                                              math.cos(math.radians(90.0 - w_dir)),
                                              math.sin(math.radians(90.0 - w_dir))), ts))
                self.trim_history(ts)
            if sum:
                self.day_sum += w_speed
                if self.lasttime:
                    self.sumtime += ts - self.lasttime
                if w_dir is not None:
                    self.day_xsum += w_speed * math.cos(math.radians(90.0 - w_dir))
                    self.day_ysum += w_speed * math.sin(math.radians(90.0 - w_dir))
            if self.lasttime is None or ts >= self.lasttime:
                self.last = (w_speed, w_dir)
                self.lasttime = ts

    def day_reset(self):
        """Reset the vector obs buffer."""

        (self.day_min, self.day_mintime,
         self.day_max, self.day_max_dir, self.day_maxtime) = VectorBuffer.default_init
        try:
            self.day_sum = 0.0
        except AttributeError:
            pass

    def nineam_reset(self):
        """Reset the vector obs buffer."""

        self.nineam_sum = 0.0

    def interval_reset(self):
        """Reset the vector obs buffer."""

        self.interval_sum = 0.0

    def trim_history(self, ts):
        """Trim an old data from the history list."""

        # calc ts of oldest sample we want to retain
        oldest_ts = ts - Buffer.MAX_AGE
        # set history_full
        self.history_full = min([a.ts for a in self.history if a.ts is not None]) <= oldest_ts
        # remove any values older than oldest_ts
        self.history = [s for s in self.history if s.ts > oldest_ts]

    def history_max(self, ts, age=Buffer.MAX_AGE):
        """Return the max value in my history.

        Search the last age seconds of my history for the max value and the
        corresponding timestamp.

        Inputs:
            ts:  the timestamp to start searching back from
            age: the max age of the records being searched

        Returns:
            An object of type ObsTuple where value is a 3 way tuple of
            (value, x component, y component) and ts is the timestamp when
            it occurred.
        """

        born = ts - age
        snapshot = [a for a in self.history if a.ts >= born]
        if len(snapshot) > 0:
            _max = max(snapshot, key=itemgetter(1)[0])
            return ObsTuple(_max[0], _max[1])
        else:
            return None

    def history_avg(self, ts, age=Buffer.MAX_AGE):
        """Return the average value in my history.

        Search the last age seconds of my history for the max value and the
        corresponding timestamp.

        Inputs:
            ts:  the timestamp to start searching back from
            age: the max age of the records being searched

        Returns:
            An object of type ObsTuple where value is a 3 way tuple of
            (value, x component, y component) and ts is the timestamp when
            it occurred.
        """

        born = ts - age
        snapshot = [a.value[0] for a in self.history if a.ts >= born]
        if len(snapshot) > 0:
            return sum(snapshot)/len(snapshot)
        else:
            return None

    def history_vec_avg(self, ts, age=Buffer.MAX_AGE):
        """Return the my history vector average."""

        born = ts - age
        rec = [a.value for a in self.history if a.ts >= born]
        if len(rec) > 0:
            x = 0
            y = 0
            for sample in rec:
                x += sample[0] * sample[1] if sample[1] is not None else 0.0
                y += sample[0] * sample[2] if sample[2] is not None else 0.0
            _dir = 90.0 - math.degrees(math.atan2(y, x))
            if _dir < 0.0:
                _dir += 360.0
            _value = math.sqrt(pow(x, 2) + pow(y, 2))
            return _value, _dir
        else:
            return None

    @property
    def day_vec_avg(self):
        """The day vector average value."""

        return math.sqrt((self.day_xsum**2 + self.day_ysum**2) / self.sumtime**2)

    @property
    def day_vec_dir(self):
        """The day vector average direction."""

        _dir = 90.0 - math.degrees(math.atan2(self.day_ysum, self.day_xsum))
        if _dir < 0.0:
            _dir += 360.0
        return _dir


# ============================================================================
#                             class ScalarBuffer
# ============================================================================


class ScalarBuffer(object):
    """Class to buffer scalar type loop data.

    Buffer constructor parameters:

        stats:   The Accumulator object fields for today for the obs type
                 concerned.

        units:   WeeWX unit system code for the units used in this ScalarBuffer
                 object. Default is None.

        history: Whether to keep a history for this obs. Boolean, default is
                 False.

        sum:     Whether to record 'sum' stats for the obs concerned. Boolean,
                 default is False.

    Buffer methods:

        add_value.       Add a scalar value to the buffer.
        day_reset.       Reset the day stats for the obs concerned.
        nineam_reset.    Reset the 9am sum.
###        interval_reset.  What does this really do ?
        trim_history.    Trim any too old obs from the obs history.
        history_max.     Calculate the maximum value in the history data.
        history_avg.     Calculate the average of the value in the history
                         data.
    """

    default_init = (None, None, None, None)

    def __init__(self, stats, units=None, history=False, sum=False):
        self.units = units
        self.last = None
        self.lasttime = None
        if stats:
            self.day_min = stats.min
            self.day_mintime = stats.mintime
            self.day_max = stats.max
            self.day_maxtime = stats.maxtime
        else:
            (self.day_min, self.day_mintime,
             self.day_max, self.day_maxtime) = self.default_init
        if history:
            self.history = []
            self.history_full = False
        if sum:
            if stats:
                self.day_sum = stats.sum
            else:
                self.day_sum = 0.0
            self.nineam_sum = 0.0
            self.interval_sum = 0.0

    def add_value(self, val, ts, hilo, history, sum):
        """Add a value to my hilo and history stats as required."""

        if val is not None:
            if self.lasttime is None or ts >= self.lasttime:
                self.last = val
                self.lasttime = ts
            if hilo:
                if self.day_min is None or val < self.day_min:
                    self.day_min = val
                    self.day_mintime = ts
                if self.day_max is None or val > self.day_max:
                    self.day_max = val
                    self.day_maxtime = ts
            if history:
                self.history.append(ObsTuple(val, ts))
                self.trim_history(ts)
            if sum:
                self.day_sum += val
                self.nineam_sum += val
                self.interval_sum += val

    def day_reset(self):
        """Reset the scalar obs buffer."""

        (self.day_min, self.day_mintime,
         self.day_max, self.day_maxtime) = self.default_init
        try:
            self.day_sum = 0.0
        except AttributeError:
            pass

    def nineam_reset(self):
        """Reset the scalar obs buffer."""

        self.nineam_sum = 0.0

    def interval_reset(self):
        """Reset the scalar obs buffer."""

        self.interval_sum = 0.0

    def trim_history(self, ts):
        """Trim an old data from the history list."""

        # calc ts of oldest sample we want to retain
        oldest_ts = ts - Buffer.MAX_AGE
        # set history_full
        self.history_full = min([a.ts for a in self.history if a.ts is not None]) <= oldest_ts
        # remove any values older than oldest_ts
        self.history = [s for s in self.history if s.ts > oldest_ts]

    def history_max(self, ts, age=Buffer.MAX_AGE):
        """Return the max value in my history.

        Search the last age seconds of my history for the max value and the
        corresponding timestamp.

        Inputs:
            ts:  the timestamp to start searching back from
            age: the max age of the records being searched

        Returns:
            An object of type ObsTuple where value is the max value found and
            ts is the timestamp when it ocurred.
        """

        born = ts - age
        snapshot = [a for a in self.history if a.ts >= born]
        if len(snapshot) > 0:
            _max = max(snapshot, key=itemgetter(1))
            return ObsTuple(_max[0], _max[1])
        else:
            return None

    def history_avg(self, ts, age=Buffer.MAX_AGE):
        """Return my average."""

        if len(self.history) > 0:
            born = ts - age
            rec = [a.value for a in self.history if a.ts >= born]
            return float(sum(rec))/len(rec)
        else:
            return None


# ============================================================================
#                            Configuration dictionaries
# ============================================================================

# various config dictionaries used by the Buffer classes
init_dict = weewx.units.ListOfDicts({'wind': VectorBuffer})
add_functions = weewx.units.ListOfDicts({'windSpeed': Buffer.add_wind_value})
seed_functions = weewx.units.ListOfDicts({'wind': Buffer.seed_vector})


# ============================================================================
#                              class ObsTuple
# ============================================================================


class ObsTuple(tuple):
    """Class to represent an observation in time.

    A observation during some period can be represented by the value of the
    observation and the time at which it was observed. This can be represented
    in a 2 way tuple called an obs tuple. An obs tuple is useful because its
    contents can be accessed using named attributes.

    Item    attribute   Meaning
      0       value     The observed value eg 19.5
      1       ts        The epoch timestamp that the value was observed
                        eg 1488245400

    It is valid to have an observed value of None.

    It is also valid to have a ts of None (meaning there is no information
    about the time the was was observed.

    ObsTuple constructor parameters:

        *args: A (minimum) two-way tuple where the first element is the obs
               value (may be a tuple for a vector obs) and the second element
               is the epoch timesdtamp of the observation. Any other elements
               will be stored in the Obstuple object but not used in the class.

    ObsTuple properties:

        value. The observation value.
        ts.    The epoch timestamp the observation was made.
    """

    def __new__(cls, *args):
        return tuple.__new__(cls, args)

    @property
    def value(self):
        return self[0]

    @property
    def ts(self):
        return self[1]


# ============================================================================
#                            Class CachedPacket
# ============================================================================


class CachedPacket():
    """Class to cache loop packet data.

    The purpose of the cache is to ensure that necessary fields for the
    generation of the JSON data are continuously available on systems whose
    station emits partial packets. The key requirement is that the field
    exists, the value (numerical or None) is handled by method calculate().
    Method calculate() could be refactored to deal with missing fields, but
    this would either result in the gauges dials oscillating when a loop packet
    is missing an essential field, or overly complex code in method calculate()
    if field caching was to occur.

    The cache consists of a dictionary of value, timestamp pairs where
    timestamp is the timestamp of the packet when obs was last seen and value
    is the value of the obs at that time. None values may be cached.

    A cached loop packet may be obtained by calling the get_packet() method.

    CachedPacket constructor parameters:

        rec: A dictionary of observation data to initialise the cache.

    CachedPacket methods:

        update.     Update the cache from a loop packet.
        get_value.  Get an individual obs value from the cache.
        get_packet. Get a loop packet from the cache.

    """

    # These fields must be available in every loop packet read from the
    # cache.
    OBS = ["cloudbase", "windDir", "windrun", "inHumidity", "outHumidity",
           "barometer", "radiation", "rain", "rainRate", "windSpeed",
           "appTemp", "dewpoint", "heatindex", "humidex", "inTemp",
           "outTemp", "windchill", "UV"]

    def __init__(self, rec):
        """Initialise our cache object.

        The cache needs to be initialised to include all of the fields required
        by method calculate(). We could initialise all field values to None
        (method calculate() will interpret the None values to be '0' in most
        cases). The result on the gauge display may be misleading. We can get
        ballpark values for all fields by priming them with values from the
        last archive record. As the archive may have many more fields than rtd
        requires, only prime those fields that rtd requires.

        This approach does have the drawback that in situations where the
        archive unit system is different to the loop packet unit system the
        entire loop packet will be converted each time the cache is updated.
        This is inefficient.
        """

        self.cache = dict()
        # if we have a dateTime field in our record source use that otherwise
        # use the current system time
        _ts = rec['dateTime'] if 'dateTime' in rec else int(time.time() + 0.5)
        # only prime those fields in CachedPacket.OBS
        for _obs in self.OBS:
            if _obs in rec and 'usUnits' in rec:
                # only add a value if it exists and we know what units its in
                self.cache[_obs] = {'value': rec[_obs], 'ts': _ts}
            else:
                # otherwise set it to None
                self.cache[_obs] = {'value': None, 'ts': _ts}
        # set the cache unit system if known
        self.unit_system = rec['usUnits'] if 'usUnits' in rec else None

    def update(self, packet, ts):
        """Update the cache from a loop packet.

        If the loop packet uses a different unit system to that of the cache
        then convert the loop packet before adding it to the cache. Update any
        previously seen cache fields and add any loop fields that have not been
        seen before.
        """

        if self.unit_system is None:
            self.unit_system = packet['usUnits']
        elif self.unit_system != packet['usUnits']:
            packet = weewx.units.to_std_system(packet, self.unit_system)
        for obs in [x for x in packet if x not in ['dateTime', 'usUnits']]:
            if packet[obs] is not None:
                self.cache[obs] = {'value': packet[obs], 'ts': ts}

    def get_value(self, obs, ts, max_age):
        """Get an obs value from the cache.

        Return a value for a given obs from the cache. If the value is older
        than max_age then None is returned.
        """

        if obs in self.cache and ts - self.cache[obs]['ts'] <= max_age:
            return self.cache[obs]['value']
        return None

    def get_packet(self, ts=None, max_age=600):
        """Get a loop packet from the cache.

        Resulting packet may contain None values.
        """

        if ts is None:
            ts = int(time.time() + 0.5)
        packet = {'dateTime': ts, 'usUnits': self.unit_system}
        for obs in self.cache:
            packet[obs] = self.get_value(obs, ts, max_age)
        return packet


# ============================================================================
#                            Utility Functions
# ============================================================================

def calc_trend(obs_type, now_vt, units, db_manager, then_ts, grace=0):
    """ Calculate change in an observation over a specified period.

    Parameters:
        obs_type:   database field name of observation concerned
        now_vt:     value of observation now (ie the finishing value)
        units:      units our returned value must be in
        db_manager: manager to be used
        then_ts:    timestamp of start of trend period
        grace:      the largest difference in time when finding the then_ts
                    record that is acceptable

    Returns:
        Change in value over trend period. Can be positive, 0, negative or
        None. Result will be in 'units' units.
    """

    if now_vt.value is None:
        return None
    then_record = db_manager.getRecord(then_ts, grace)
    if then_record is None:
        return None
    else:
        if obs_type not in then_record:
            return None
        else:
            then_vt = weewx.units.as_value_tuple(then_record, obs_type)
            now = convert(now_vt, units).value
            then = convert(then_vt, units).value
            return now - then

def obfuscate_password(url):
    """Obfuscate the password in a URL.

    Obfuscates the password in a URL of the format:

        scheme://user:password@hostname:port/

    returning:

        scheme://user:xxx@hostname:port/

    Parameters
        url: the URL in which the password is to be obfuscated

    Returns:
        A string containing a copy of the original URL with the password
        obfuscated.
    """

    if url is None:
        return None
    # parse the URL
    parts = urlparse.urlparse(url)
    # do we have a password
    if parts.password is not None:
        # split out the host portion manually. We could use
        # parts.hostname and parts.port, but then you'd have to check
        # if either part is None. The hostname would also be lowercased.
        host_info = parts.netloc.rpartition('@')[-1]
        parts = parts._replace(netloc='{}:xxx@{}'.format(
            parts.username, host_info))
        # re-assemble the URL
        url = parts.geturl()
    return url