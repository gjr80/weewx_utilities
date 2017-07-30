# mqtt_dasboard.py
#
# A collection of weeWX services to support MQTT publishing of dashboard data.
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
# Version: 0.1.0                                    Date: 24 July 2017
#
# Revision History
#  24 July 2017         v0.1.0  - initial release
#
"""A collection of weeWX services to support MQTT publishing of dashboard data.

MQTT publishing based on MQTT uploader by Matthew Wall.

This file contains the following weeWX services:

1.  MQTTArchive. WeeWX service to publish JSON format data to a MQTT broker
    each archive period. This service is predominantly used for publishing data
    that does not chnage between successive archive records eg yesterday
    aggregates, today sun/moon rise/set etc. The publication to a MQTT broker
    is triggered by the arrival of a new archive record.

3.  MQTTWU. A weeWX service to publish JSON format current conditions and
    forecast data obtained from the WeatherUnderground (WU). The service
    obtains WU data via calls to the WU API at a frequency defined by the user.
    Selected current conditions and forecast data in JSON format is then
    published to a MQTT broker. Upon arrival of a new archive record the
    service checks if it is time to update the WU data, if it is time to update
    the WU API is called and updated data published to the MQTT broker. If it
    is not time to update no further processing or publishing is undertaken
    until the arrival of the next archive record.

Abbreviated instructions for use:

1.  The python bindings for MQTT are required:

    $ pip install paho-mqtt

2.  Put this file in $BIN_ROOT/user.

3.  If using MQTTArchive add the following stanza to weewx.conf:

[MQTTArchive]
    # Any database data (eg aggregates) are extracted using the [StdArchive]
    # data_binding. Data from another database (eg appTemp) can be accessed
    # using the binding specified by additional_binding. Optional, default
    # 'wx_binding'.
    additional_binding = wx_binding

    [[Formats]]
        degree_C               = %.2f
        degree_F               = %.2f
        degree_compass         = %.1f
        foot                   = %.2f
        hPa                    = %.2f
        inHg                   = %.4f
        inch                   = %.3f
        inch_per_hour          = %.3f
        km_per_hour            = %.1f
        mile_per_hour          = %.1f
        mbar                   = %.2f
        meter                  = %.1f
        meter_per_second       = %.2f
        mm                     = %.2f
        mm_per_hour            = %.2f
        percent                = %.1f
        uv_index               = %.2f
        volt                   = %.2f
        watt_per_meter_squared = %.1f
        NONE                   = 'None'

    [[Groups]]
        # Groups. Optional. Note not all available weeWX units are supported
        # for each group.
        group_altitude = foot        # Options are 'meter' or 'foot'
        group_pressure = hPa         # Options are 'inHg', 'mbar', or 'hPa'
        group_rain = mm              # Options are 'inch' or 'mm'
        group_speed = km_per_hour    # Options are 'mile_per_hour',
                                     #  'km_per_hour' or 'meter_per_second'
        group_temperature = degree_C # Options are 'degree_F' or 'degree_C'

    # Config options to control MQTT uploader
    [[MQTT]]
        # MQTT server URL to be used. Must be int he format:
        #
        # server_url = mqtt://user:password@address:port/
        #
        # where:
        #   user:     The MQTT user name to be used
        #   password: The password for the MQTT user
        #   address:  The address or resolvable name of the MQTT server. Note
        #             that if using TLS the this setting may need to match the
        #             server name on the certificate used by the server.
        #   port:     The port number on which the mQTT server is listening
        #
        #   eg:
        #   server_url = mqtt://bill:bills_password@mqtt.domain_name.com:8883/
        #
        server_url = mqtt://user_name:password@mqtt.domain_name.com:port/
        # MQTT server topic to post to eg:
        #
        # topic = weather/slow
        #
        # will result in the topic weather/slow being used.
        topic = weather/slow
        [[[tls]]]
            Options to be passed to Paho client tls_set method. Refer to Paho
            client documentation: https://eclipse.org/paho/clients/python/docs/

            # Path and name of CA certificates file. String, mandatory.
            ca_certs = /etc/ssl/certs/ca-certificates.crt
            # Path and name of PEM encoded client certificate. String, optional.
            certfile =
            # Path and name of private key. String, optional.
            keyfile =
            # Certificate requirements imposed on the broker. String, optional.
            # Available options are none, optional or required (default).
            cert_reqs =
            # SSL/TLS protocol to be used. String, optional. Available options
            # are sslv1, sslv2, sslv23, tls, tlsv1 (default). Not all options
            # are supported by all systems.
            tls_version =
            # Allowable encryption ciphers. If using comma separators enclose
            # option in quotes. String, optional.
            ciphers =

4.  If using MQTTWU add the following stanza to weewx.conf:

[MQTTWU]
    # Config options to control MQTT uploader
    [[MQTT]]
        # MQTT server URL to be used. Must be int he format:
        #
        # server_url = mqtt://user:password@address:port/
        #
        # where:
        #   user:     The MQTT user name to be used
        #   password: The password for the MQTT user
        #   address:  The address or resolvable name of the MQTT server. Note
        #             that if using TLS the this setting may need to match the
        #             server name on the certificate used by the server.
        #   port:     The port number on which the mQTT server is listening
        #
        #   eg:
        #   server_url = mqtt://bill:bills_password@mqtt.domain_name.com:8883/
        #
        server_url = mqtt://user_name:password@mqtt.domain_name.com:port/
        # MQTT server topic to post to eg:
        #
        # topic = weather/slow
        #
        # will result in the topic weather/slow being used.
        topic = weather/slow
        [[[tls]]]
            Options to be passed to Paho client tls_set method. Refer to Paho
            client documentation: https://eclipse.org/paho/clients/python/docs/

            # Path and name of CA certificates file. String, mandatory.
            ca_certs = /etc/ssl/certs/ca-certificates.crt
            # Path and name of PEM encoded client certificate. String, optional.
            certfile =
            # Path and name of private key. String, optional.
            keyfile =
            # Certificate requirements imposed on the broker. String, optional.
            # Available options are none, optional or required (default).
            cert_reqs =
            # SSL/TLS protocol to be used. String, optional. Available options
            # are sslv1, sslv2, sslv23, tls, tlsv1 (default). Not all options
            # are supported by all systems.
            tls_version =
            # Allowable encryption ciphers. If using comma separators enclose
            # option in quotes. String, optional.
            ciphers =

    [[WU]]
        # WU API key to be used when calling the WU API
        api_key = xxxxxxxxxxxxxxxx

        # Interval (in seconds) between forecast downloads. Default is 1800.
        forecast_interval = 1800

        # Interval (in seconds) between current condition downloads. Default
        # is 1800.
        conditions_interval = 1800

        # Minimum period (in seconds) between like (eg forecast) API calls.
        # This prevents conditions where a misbehaving program could call the
        # WU API repeatedly thus violating the API usage conditions. Default
        # is 60.
        api_lockout_period = 60

        # Maximum number attempts to obtain an API response. Default is 3.
        max_WU_tries = 3

        # The location for the forecast and current conditions can be one of
        # the following:
        #   CA/San_Francisco     - US state/city
        #   60290                - US zip code
        #   Australia/Sydney     - Country/City
        #   37.8,-122.4          - latitude,longitude
        #   KJFK                 - airport code
        #   pws:KCASANFR70       - PWS id
        #   autoip               - AutoIP address location
        #   autoip.json?geo_ip=38.102.136.138 - specific IP address location
        # If no location is specified, station latitude and longitude are used
        location = enter location here

5.  Set the options in the stanzas added to weewx.conf as required.

6.  Add the MQTT required services to the list of report services under
[Engine] [[Services]] in weewx.conf:

[Engine]
    [[Services]]
        report_services = ..., user.mqtt_dashboard.MQTTArchive, user.mqtt_dashboard.MQTTWU

7.  Stop/start weeWX.

9.  If using the MQTTArchive service, confirm that the JSON format data is
being generated and posted to the MQTT server as per the [MQTTArchive] config
options in weewx.conf.

10. If using the MQTTWU service confirm that the relevant current conditions
and forecast data is being downloaded from WU and published to the MQTT server
as per the [MQTTWU] config options in weewx.conf.

To do:
    - almanac temperature and pressure in process_record() need to better
      handle absence of temperature and pressure in the current archive record,
      maybe cache these before reverting to 15C/1010hPa (more than 1 occurrence)
    - fix raise weewx.restx.FailedPost in mqtt_post_data
    - get rid of manager_dict in StdService based classes?
"""


# python imports
import Queue
import json
import operator
import syslog
import threading
import time


# weeWX imports
import user.mqtt_utility
import weewx
import weeutil.weeutil
import weewx.almanac
import weewx.manager
import weewx.tags
import weewx.units

from weeutil.weeutil import to_bool, to_float, to_int, timestamp_to_string
from weewx.engine import StdService
from weewx.units import ValueTuple, convert, getStandardUnitType

# version number of this script
MD_VERSION = '0.1.0'


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
#                             class MQTTArchive
# ============================================================================


class MQTTArchive(StdService):
    """Service that posts slow changing JSON data to an MQTT server.

    The MQTTArchive class creates and controls a threaded object of class
    MQTTArchiveThread that generates slow changing JSON data once each archive
    period and posts this data to a MQTT server.
    """

    def __init__(self, engine, config_dict):
        # initialize my superclass
        super(MQTTArchive, self).__init__(engine, config_dict)

        # create a Queue object to pass data to our thread
        self.queue = Queue.Queue()

        # get the MQTTDashboard config dictionary
        md_config_dict = config_dict.get('MQTTDashboard')
        # get the MQTTArchive config dict
        ma_config_dict = md_config_dict.get('MQTTArchive')
        if ma_config_dict is None:
            return
        # get a MQTT config dict from [MQTTDashboard] to use as a starting
        # point
        mqtt_config_dict = md_config_dict.get('MQTT', {})
        # merge any MQTT overrides that may be specified under
        # [MQTTWU][[MQTT]]
        mqtt_config_dict.merge(ma_config_dict.get('MQTT'))

        # get a manager dict
        manager_dict = weewx.manager.get_manager_dict_from_config(config_dict,
                                                                  'wx_binding')
        self.db_manager = weewx.manager.open_manager(manager_dict)

        # get an instance of class MQTTArchiveThread and start the thread
        # running
        self.thread = MQTTArchiveThread(self.queue,
                                        config_dict,
                                        manager_dict,
                                        ma_config_dict,
                                        mqtt_config_dict,
                                        lat=engine.stn_info.latitude_f,
                                        long=engine.stn_info.longitude_f,
                                        alt=convert(engine.stn_info.altitude_vt,
                                                    'meter').value)
        self.thread.start()

        # bind ourself to the weeWX NEW_ARCHIVE_RECORD event
        self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)

        _server = user.mqtt_utility.obfuscate_password(mqtt_config_dict.get('server_url'))
        _topic = mqtt_config_dict.get('topic',
                                      'weather/slow')
        # log what will be done
        loginf("mqttarchive",
               "Data will be published to MQTT broker '%s' under topic '%s'" % (_server,
                                                                                _topic))

    def new_archive_record(self, event):
        """Puts archive records in the queue."""

        self.queue.put(event.record)
        logdbg2("mqttarchive",
                "Queued archive record dateTime=%s" % timestamp_to_string(event.record['dateTime']))

    def shutDown(self):
        """Shut down any threads."""

        if hasattr(self, 'queue') and hasattr(self, 'thread'):
            if self.queue and self.thread.isAlive():
                # put a None in the queue to signal the thread to shutdown
                self.queue.put(None)
                # wait up to 20 seconds for the thread to exit:
                self.thread.join(20.0)
                if self.thread.isAlive():
                    logerr("mqttarchive",
                           "Unable to shut down %s thread" % self.thread.name)
                else:
                    logdbg("mqttarchive",
                           "Shut down %s thread" % self.thread.name)


# ============================================================================
#                        class MQTTArchiveThread
# ============================================================================


class MQTTArchiveThread(threading.Thread):
    """Thread that generates JSON data and publishes to a MQTT broker."""

    def __init__(self, queue, config_dict, manager_dict, ma_config_dict, mqtt_config_dict, lat, long, alt):
        # Initialize my superclass
        threading.Thread.__init__(self)

        self.setDaemon(True)
        self.queue = queue
        self.config_dict = config_dict
        self.manager_dict = manager_dict

        # various dicts used later with converters and formatters
        self.group_dict = ma_config_dict.get('Groups', weewx.units.MetricUnits)
        self.format_dict = ma_config_dict.get('Formats',
                                              weewx.units.default_unit_format_dict)
        self.moonphases = ma_config_dict.get('Almanac', {}).get('moon_phases',
                                                                weeutil.Moon.moon_phases)

        # get MQTT config options
        server_url = mqtt_config_dict.get('server_url', None)
        self.topic = mqtt_config_dict.get('topic', 'weather/slow')
        tls_opt = mqtt_config_dict.get('tls', None)
        retain = to_bool(mqtt_config_dict.get('retain', True))
        max_tries = to_int(mqtt_config_dict.get('max_tries', 3))
        retry_wait = to_float(mqtt_config_dict.get('retry_wait', 0.5))
        log_success = to_bool(mqtt_config_dict.get('log_success', False))

        # get a MQTTPublish object to do the publishing for us
        self.publisher = user.mqtt_utility.MQTTPublish(server=server_url,
                                                       tls=tls_opt,
                                                       retain=retain,
                                                       max_tries=max_tries,
                                                       retry_wait=retry_wait,
                                                       log_success=log_success)

        # Set the binding to be used for data from an additonal (ie not the
        # [StdArchive]) binding. Default to 'wx_binding'.
        self.additional_binding = ma_config_dict.get('additional_binding',
                                                      'wx_binding')

        # get some station info
        self.latitude = lat
        self.longitude = long
        self.altitude_m = alt

        # initialise some properties that will pickup real values later
        self.db_manager = None
        self.additional_manager = None
        self.stats = None

    def run(self):
        """Collect records from the queue and manage their processing.

        Now that we are in a thread get a manager for our dbs (bindings). Once
        this is done we wait for something in the queue.
        """

        # Would normally do this in our class' __init__ but since we are are
        # running in a thread we need to wait until the thread is actually
        # running before we can get db managers and do any associated setup.

        # get a db manager
        self.db_manager = weewx.manager.open_manager(self.manager_dict)
        # get a db manager for any additional obs (eg appTemp)
        if self.additional_binding:
            self.additional_manager = weewx.manager.open_manager_with_config(self.config_dict,
                                                                             self.additional_binding)
        self.db_binder = weewx.manager.DBBinder(self.config_dict)

        # get Converter and Formatter objects to convert/format our data
        self.converter = weewx.units.Converter(self.group_dict)
        self.formatter = weewx.units.Formatter(unit_format_dict=self.format_dict)

        # Run a continuous loop, processing data received in the queue. Only
        # break out if we receive the shutdown singal (None) from our parent.
        while True:
            # Run an inner loop checking for the shutdown signal and keeping
            # the queue length from getting too long. If an archive record is
            # received break out of the loop and process it
            while True:
                _package = self.queue.get()
                if _package is None:
                    # None is our signal to exit
                    return
                # if packets have backed up in the queue, trim it until it's no
                # bigger than the max allowed backlog
                if self.queue.qsize() <= 5:
                    break

            # we now have a record to process
            # first, log receipt
            if weewx.debug == 2:
                logdbg("mqttarchivethread", "Received archive record")
            elif weewx.debug > 2:
                logdbg("mqttarchivethread",
                       "Received archive record: %s" % (_package, ))
            # process the record
            self.process_record(_package)

    def process_record(self, record):
        """Process incoming record, generate and post the JSON data.

        Input:
            packet: dict containing the just received archive record
        """

        # get time for debug timing
        t1 = time.time()

        # since we are in a thread include some robust error catching and
        # reporting so we don't just silently die
        try:
            db_lookup = self.db_binder.bind_default('wx_binding')
            self.stats = weewx.tags.TimeBinder(db_lookup,
                                               record['dateTime'],
                                               converter=self.converter,
                                               formatter=self.formatter)
            temperature_C = record.get('outTemp', 15.0)
            pressure_mbar = record.get('barometer', 1010.0)
            self.almanac = weewx.almanac.Almanac(record['dateTime'],
                                                 self.latitude,
                                                 self.longitude,
                                                 altitude=self.altitude_m,
                                                 temperature=temperature_C,
                                                 pressure=pressure_mbar,
                                                 moon_phases=self.moonphases,
                                                 formatter=self.formatter)
            # get a data dict from which to construct our JSON data
            data = self.calculate(record)
            # publish the data
            self.publisher.publish(self.topic,
                                   json.dumps(data),
                                   data['dateTime']['now'])
            # log the time taken to process this record
            logdbg("mqttarchivethread",
                   "Record (%s) processed in %.5f seconds" % (record['dateTime'],
                                                              (time.time()-t1)))
        except FailedPost, e:
            # data could not be published, log and continue
            logerr("mqttarchivethread",
                   "Data was not published: %s" % (e, ))
        except Exception, e:
            # Some unknown exception occurred. This is probably a serious
            # problem. Exit.
            logcrit("mqttarchivethread",
                    "Unexpected exception of type %s" % (type(e), ))
            weeutil.weeutil.log_traceback('mqttarchivethread: **** ')
            logcrit("mqttarchivethread", "Thread exiting. Reason: %s" % (e, ))

    def calculate(self, record):
        """Construct a data dict to be used as the JSON source.

        Input:
            record: loop data record

        Returns:
            Dictionary containing the data to be posted.
        """

        packet_d = dict(record)
        ts = int(packet_d['dateTime'])

        # initialise our result containing dict
        data = {}

        # Add dateTime fields
        dateTime = {}
        # now
        dateTime['now'] = ts
        # add dateTime fields to our data
        data['dateTime'] = dateTime

        # Add outTemp fields
        outTemp = {}
        # yesterday
        outTemp['yest'] = {}
        outTemp['yest']['min'] = to_float(self.stats.yesterday().outTemp.min.formatted)
        outTemp['yest']['min_t'] = self.stats.yesterday().outTemp.mintime.raw
        outTemp['yest']['max'] = to_float(self.stats.yesterday().outTemp.max.formatted)
        outTemp['yest']['max_t'] = self.stats.yesterday().outTemp.maxtime.raw
        # add outTemp fields to our data
        data['outTemp'] = outTemp

        # Add wind fields
        wind = {}
        # windGust
        wind['windGust'] = {}
        # yesterday
        wind['windGust']['yest'] = {}
        wind['windGust']['yest']['max'] = to_float(self.stats.yesterday().windGust.max.formatted)
        wind['windGust']['yest']['max_t'] = self.stats.yesterday().windGust.maxtime.raw
        # windrun
        wind['windrun'] = {}
        if self.stats.yesterday().windrun.exists:
            _run = to_float(self.stats.yesterday().windrun.sum.formatted)
        else:
            try:
                _run_km = self.stats.yesterday().windSpeed.avg.km_per_hour.raw * 24
                _run_vt = ValueTuple(_run_km, 'km', 'group_distance')
                _run_c = self.converter.convert(_run_vt)
                _run = to_float(self.formatter.toString(_run_c, addLabel=False))
            except TypeError:
                _run = None
            wind['windrun']['yest'] = _run
        # add wind fields to our data
        data['wind'] = wind

        # Add rain fields
        rain = {}
        # yesterday
        rain['yest'] = to_float(self.stats.yesterday().rain.sum.formatted)
        # month to date
        rain['mtd'] = to_float(self.stats.month().rain.sum.formatted)
        # year to date
        rain['ytd'] = to_float(self.stats.year().rain.sum.formatted)
        # add rain fields to our data
        data['rain'] = rain

        # Add radiation fields
        radiation = {}
        # yesterday
        radiation['yest'] = {}
        radiation['yest']['max'] = to_float(self.stats.yesterday().radiation.max.formatted)
        radiation['yest']['max_t'] = self.stats.yesterday().radiation.maxtime.raw
        # add radiation fields to our data
        data['radiation'] = radiation

        # Add UV fields
        UV = {}
        # yesterday
        UV['yest'] = {}
        UV['yest']['max'] = to_float(self.stats.yesterday().UV.max.formatted)
        UV['yest']['max_t'] = self.stats.yesterday().UV.maxtime.raw
        # add UV fields to our data
        data['UV'] = UV

        # Add sun fields
        sun = {}
        # rise, set and day length
        sun['rise'] = self.almanac.sun.rise.raw
        sun['set'] = self.almanac.sun.set.raw
        if self.almanac.hasExtras:
            sun['dayLength'] = (self.almanac(pressure=0, horizon=-34.0/60).sun.set.raw -
                                    self.almanac(pressure=0, horizon=-34.0/60).sun.rise.raw)
        else:
            sun['dayLength'] = sun['set'] - sun['rise']
        # add sun fields to our data
        data['sun'] = sun

        # Add moon fields
        moon = {}
        # rise and set
        if self.almanac.hasExtras:
            moon['rise'] = self.almanac.moon.rise.raw
            moon['set'] = self.almanac.moon.set.raw
            prev_phases = []
            next_phases = []
            prev_phases.append(["Full Moon",
                                self.almanac.previous_full_moon.raw])
            prev_phases.append(["Last Quarter",
                                self.almanac.previous_last_quarter_moon.raw])
            prev_phases.append(["New Moon",
                                self.almanac.previous_new_moon.raw])
            prev_phases.append(["First Quarter",
                                self.almanac.previous_first_quarter_moon.raw])
            prev_ph = max(prev_phases, key=operator.itemgetter(1))
            next_phases.append(["Full Moon",
                                self.almanac.next_full_moon.raw])
            next_phases.append(["Last Quarter",
                                self.almanac.next_last_quarter_moon.raw])
            next_phases.append(["New Moon",
                                self.almanac.next_new_moon.raw])
            next_phases.append(["First Quarter",
                                self.almanac.next_first_quarter_moon.raw])
            next_ph = min(next_phases, key=operator.itemgetter(1))
            moon['lastPhase'] = {}
            moon['lastPhase']['name'] = prev_ph[0]
            moon['lastPhase']['date'] = prev_ph[1]
            moon['nextPhase'] = {}
            moon['nextPhase']['name'] = next_ph[0]
            moon['nextPhase']['date'] = next_ph[1]
        else:
            moon['rise'] = None
            moon['set'] = None
            moon['lastPhase'] = {}
            moon['lastPhase']['name'] = None
            moon['lastPhase']['date'] = None
            moon['nextPhase'] = {}
            moon['nextPhase']['name'] = None
            moon['nextPhase']['date'] = None
        # add moon fields to our data
        data['moon'] = moon

        return data


# ============================================================================
#                               class MQTTWU
# ============================================================================


class MQTTWU(StdService):
    """Service that publishes WU conditions and forecast data to a MQTT broker.

    The MQTTWU class creates and controls a threaded object of class
    MQTTWUThread that obtains current conditions and forecast data for a
    location from the WU API and publishes selected elements to a MQTT broker.
    """

    def __init__(self, engine, config_dict):
        # initialize my superclass
        super(MQTTWU, self).__init__(engine, config_dict)

        # create a Queue object to pass data to our thread
        self.queue = Queue.Queue()

        # get the MQTTDashboard config dictionary
        md_config_dict = config_dict.get('MQTTDashboard')
        # get the MQTTWU config dict
        mw_config_dict = md_config_dict.get('MQTTWU')
        if mw_config_dict is None:
            return
        # get a MQTT config dict from [MQTTDashboard] to use as a starting
        # point
        mqtt_config_dict = md_config_dict.get('MQTT', {})
        # merge any MQTT overrides that may be specified under
        # [MQTTWU][[MQTT]]
        mqtt_config_dict.merge(mw_config_dict.get('MQTT'))

        # get an instance of class MQTTWUThread and start the thread running
        self.thread = MQTTWUThread(self.queue,
                                   config_dict,
                                   mw_config_dict,
                                   mqtt_config_dict,
                                   lat=engine.stn_info.latitude_f,
                                   long=engine.stn_info.longitude_f,
                                   alt_m=convert(engine.stn_info.altitude_vt,
                                                 'meter').value)
        self.thread.start()

        # bind ourself to the weeWX NEW_ARCHIVE_RECORD event
        self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)

        _server = user.mqtt_utility.obfuscate_password(mqtt_config_dict.get('server_url'))
        _forecast_topic = mqtt_config_dict.get('forecast_topic',
                                               'weather/forecast')
        _conditions_topic = mqtt_config_dict.get('conditions_topic',
                                                 'weather/conditions')
        # log what will be done
        loginf("mqttwu",
               "Forecast data will be published to MQTT broker '%s' under topic '%s'" % (_server,
                                                                                         _forecast_topic))
        loginf("mqttwu",
               "Conditions data will be published to MQTT broker '%s' under topic '%s'" % (_server,
                                                                                           _conditions_topic))

    def new_archive_record(self, event):
        """Puts archive records in the queue."""

        self.queue.put(event.record)
        logdbg2("mqttwu",
                "Queued archive record dateTime=%s" % timestamp_to_string(event.record['dateTime']))

    def shutDown(self):
        """Shut down any threads."""

        if hasattr(self, 'queue') and hasattr(self, 'thread'):
            if self.queue and self.thread.isAlive():
                # put a None in the queue to signal the thread to shutdown
                self.queue.put(None)
                # wait up to 20 seconds for the thread to exit:
                self.thread.join(20.0)
                if self.thread.isAlive():
                    logerr("mqttwu",
                           "Unable to shut down %s thread" % self.thread.name)
                else:
                    logdbg("mqttwu",
                           "Shut down %s thread" % self.thread.name)


# ============================================================================
#                            class MQTTWUThread
# ============================================================================


class MQTTWUThread(threading.Thread):
    """Thread to gather WU API data and publish selected data to a MQTT server."""

    # Define a dictionary to look up WU icon names and return corresponding
    # Saratoga icon code
    icon_dict = {
        'clear'             : 0,
        'cloudy'            : 18,
        'flurries'          : 25,
        'fog'               : 11,
        'hazy'              : 7,
        'mostlycloudy'      : 18,
        'mostlysunny'       : 9,
        'partlycloudy'      : 19,
        'partlysunny'       : 9,
        'sleet'             : 23,
        'rain'              : 20,
        'snow'              : 25,
        'sunny'             : 28,
        'tstorms'           : 29,
        'nt_clear'          : 1,
        'nt_cloudy'         : 13,
        'nt_flurries'       : 16,
        'nt_fog'            : 11,
        'nt_hazy'           : 13,
        'nt_mostlycloudy'   : 13,
        'nt_mostlysunny'    : 1,
        'nt_partlycloudy'   : 4,
        'nt_partlysunny'    : 1,
        'nt_sleet'          : 12,
        'nt_rain'           : 14,
        'nt_snow'           : 16,
        'nt_tstorms'        : 17,
        'chancerain'        : 20,
        'chancesleet'       : 23,
        'chancesnow'        : 25,
        'chancetstorms'     : 29
        }

    def __init__(self, queue, config_dict, mw_config_dict, mqtt_config_dict, lat, long, alt_m):
        # Initialize my superclass
        threading.Thread.__init__(self)

        self.setName('MQTTWUThread')
        self.setDaemon(True)
        self.queue = queue

        # Get station info required for Sun related calcs
        self.latitude = lat
        self.longitude = long
        self.altitude_m = alt_m

        # MQTT broker URL
        server_url = mqtt_config_dict.get('server_url', None)
        # topics to publish to
        self.topic = {}
        self.topic['conditions'] = mqtt_config_dict.get('conditions_topic', 'weather/conditions')
        self.topic['forecast'] = mqtt_config_dict.get('forecast_topic', 'weather/forecast')
        # TLS options
        tls_opt = mqtt_config_dict.get('tls', None)
        # will MQTT broker retain messages
        retain = to_bool(mqtt_config_dict.get('retain', True))
        # maximum number of attempts to publish data
        max_tries = to_int(mqtt_config_dict.get('max_tries', 3))
        # time (in seconds) to wait between retries
        retry_wait = to_float(mqtt_config_dict.get('retry_wait', 0.5))
        # log successful publishing of data
        log_success = to_bool(mqtt_config_dict.get('log_success', False))

        # get a MQTTPublish object to do the publishing for us
        self.publisher = user.mqtt_utility.MQTTPublish(server=server_url,
                                                      tls=tls_opt,
                                                      retain=retain,
                                                      max_tries=max_tries,
                                                      retry_wait=retry_wait,
                                                      log_success=log_success)

        # get WU config dictionary
        wu_config_dict = mw_config_dict.get('WU', {})

        # list of the WU API 'features' to be used
        self.features = ['conditions', 'forecast']
        # interval between API calls for each 'feature'
        self.interval = {}
        self.interval['conditions'] = to_int(wu_config_dict.get('current_interval', 1800))
        self.interval['forecast'] = to_int(wu_config_dict.get('forecast_interval', 1800))
        # max no of tries we will make in any one attempt to contact WU via API
        self.max_WU_tries = to_int(wu_config_dict.get('max_WU_tries', 3))
        # Get API call lockout period. This is the minimum period between API
        # calls for the same feature. This prevents an error condition making
        # multiple rapid API calls and thus breac the API usage conditions.
        self.lockout_period = to_int(wu_config_dict.get('api_lockout_period', 60))
        # initialise containers for timestamp of last API call made for each
        # feature
        self.last = {}
        self.last['conditions'] = None
        self.last['forecast'] = None
        # initialise container for timestamp of last WU api call
        self.last_call_ts = None
        # Get our API key from weewx.conf, first look in [Weewx-WD] and if no luck
        # try [Forecast] if it exists. Wrap in a try..except loop to catch exceptions (ie one or
        # both don't exist.
        try:
            if wu_config_dict.get('api_key') != None:
                api_key = wu_config_dict.get('api_key')
            elif config_dict['Forecast']['WU'].get('api_key', None) != None:
                api_key = config_dict['Forecast']['WU'].get('api_key')
            else:
                raise MissingApiKey("Cannot find valid Weather Underground API key")
        except:
            raise MissingApiKey("Cannot find Weather Underground API key")
        # Get 'query' (ie the location) to be used for use in WU API calls.
        # Refer weewx.conf for details.
        self.query = wu_config_dict.get('location', (lat, long))
        # get a WeatherUndergroundAPI object to handle the API calls
        self.api = user.mqtt_utility.WeatherUndergroundAPI(api_key)

        # initialise night flag
        self.night = None

    def run(self):
        """Collect records from the queue and manage their processing."""

        try:
            # Run a continuous loop, processing data received in the queue. Only
            # break out if we receive the shutdown signal (None) from our parent.
            while True:
                # Run an inner loop checking for the shutdown signal and keeping
                # the queue length from getting too long. If an archive record is
                # received break out of the loop and process it
                while True:
                    _package = self.queue.get()
                    if _package is None:
                        # None is our signal to exit
                        return
                    # if packets have backed up in the queue, trim it until it's no
                    # bigger than the max allowed backlog
                    if self.queue.qsize() <= 5:
                        break

                # we now have a record to process
                # First, log receipt. The amount of logging depends on the debug
                # level.
                if weewx.debug == 2:
                    logdbg("mqttwuthread", "Received archive record")
                elif weewx.debug > 2:
                    logdbg("mqttwuthread",
                           "Received archive record: %s" % (_package, ))
                # process the record
                self.process_wu(_package)
        except Exception, e:
            # Some unknown exception occurred. This is probably a serious
            # problem. Exit.
            logcrit("mqttwuthread",
                    "Unexpected exception of type %s" % (type(e), ))
            weeutil.weeutil.log_traceback('mqttwuthread: **** ')
            logcrit("mqttwuthread", "Thread exiting. Reason: %s" % (e, ))

    def process_wu(self, record):
        """Gather and parse the WU API data then publish to a MQTT server."""

        # determine if it is night or day so we can set day or night icons
        # when translating WU icons to Saratoga icons
        self.night = self.is_night(record)
        # loop through our list of API calls to be made
        for feature in self.features:
            # get the current archive record time
            now = int(record['dateTime'])
            logdbg2("mqttwuthread",
                   "Last Weather Underground %s API call at %s" % (feature,
                                                                   self.last[feature]))
            # has the lockout period passed since the last call of this
            # feature?
            if self.last_call_ts is None or ((now + 1 - self.lockout_period) >= self.last_call_ts):
                # If we haven't made this API call previously or if its been
                # too long since the last call then make the call
                if (self.last[feature] is None) or ((now + 1 - self.interval[feature]) >= self.last[feature]):
                    # Make the call, wrap in a try..except just in case
                    try:
                        response = self.api.data_request(features=feature,
                                                         query=self.query,
                                                         format='json',
                                                         max_tries=self.max_WU_tries)
                        logdbg("mqttwuthread",
                               "Downloaded updated Weather Underground %s information" % (feature))
                        # if we got something back then reset our timestamp for
                        # this feature
                        if response is not None:
                            self.last[feature] = now
                        # parse the WU response and create a dict to publish
                        _data = self.parse_WU_response(feature, response)
                        # timestamp the data to be posted
                        _data['last_updated'] = now
                        # publish to MQTT broker
                        self.publisher.publish(self.topic[feature],
                                               json.dumps(_data),
                                               now)
                    except:
                        loginf("mqttwuthread",
                               "Weather Underground '%s' API query failure" % (feature))
            else:
                # API call limiter kicked in so say so
                loginf("mqttwuthread",
                       "Tried to make an API call within %d sec of the previous call." % (self.lockout_period, ))
                loginf("            ",
                       "API call limit reached. API call skipped.")
                break
        # get the last API call timestamp
        self.last_call_ts = max(self.last[q] for q in self.last)

    def is_night(self, record):
        """Given a weeWX archive record determine if it is night.

        Calculates sun rise and sun set and determines whether the dateTime
        field in the record concerned falls outside of the period sun rise to
        sun set.

        Input:
            record: a weeWX archive record or loop packet.

        Returns:
            False if the dateTime field is during the daytime otherwise True.
        """

        # Almanac object gives more accurate results if current temp and
        # pressure are provided. Initialise some defaults.
        temperature_C = 15.0
        pressure_mbar = 1010.0
        # get current outTemp and barometer if they exist
        if 'outTemp' in record:
            temperature_C = convert(weewx.units.as_value_tuple(record, 'outTemp'),
                                    "degree_C").value
        if 'barometer' in record:
            pressure_mbar = convert(weewx.units.as_value_tuple(record, 'barometer'),
                                    "mbar").value
        # get our almanac object
        almanac = weewx.almanac.Almanac(record['dateTime'],
                                        self.latitude,
                                        self.longitude,
                                        self.altitude_m,
                                        temperature_C,
                                        pressure_mbar)
        # work out sunrise and sunset timestamp so we can determine if it is
        # night or day
        sunrise_ts = almanac.sun.rise.raw
        sunset_ts = almanac.sun.set.raw
        # if we are not between sunrise and sunset it must be night
        return not (sunrise_ts < record['dateTime'] < sunset_ts)

    def parse_WU_response(self, feature, response):
        """ Parse a WU response and return a data packet of required fields.

        Take a WU API response for a given 'feature', check for (WU defined)
        errors then extract and return the fields of interest.

        Inputs:
            feature: The WU data set or 'feature' to be parsed. String.
                     (refer https://www.wunderground.com/weather/api/d/docs?d=index)
            response: A WU API response in JSON format.

        Returns:
            A dictionary containing the fields of interest from the WU API
            response.
        """

        # create a holder for the data we gather
        data = {}
        # deserialise the response
        _response_json = json.loads(response)
        # check for recognised format
        if not 'response' in _response_json:
            loginf("mqttwuthread",
                   "Unknown format in Weather Underground '%s'" % (feature, ))
            return data
        _response = _response_json['response']
        # check for WU provided error else start pulling in the data we want
        if 'error' in _response:
            loginf("mqttwuthread",
                   "Error in Weather Underground '%s' response" % (feature, ))
            return data
        # no WU error so start pulling in the data we want
        if feature == 'forecast':
            # we have forecast data
            _fcast = _response_json['forecast']['txt_forecast']['forecastday']
            for fcst_period in _fcast:
                data[fcst_period['period']] = {}
                data[fcst_period['period']]['title'] = fcst_period['title']
                data[fcst_period['period']]['forecastIcon'] = fcst_period['icon']
                data[fcst_period['period']]['forecastText'] = fcst_period['fcttext']
                data[fcst_period['period']]['forecastTextMetric'] = fcst_period['fcttext_metric']
        elif feature == 'conditions':
            # we have conditions data
            _current = _response_json['current_observation']
            # WU does not seem to provide day/night icon name in their
            # 'conditions' response so we need to do. Just need to add 'nt_'
            # to front of name before looking up in out Saratoga icons
            # dictionary
            if self.night:
                data['currentIcon'] = 'nt_' + _current['icon']
            else:
                data['currentIcon'] = _current['icon']
            data['currentText'] = _current['weather']
        return data


# ============================================================================
#                             class MQTTRealtime
# ============================================================================


class MQTTRealtime(StdService):
    """Service that publishes loop based data to a MQTT broker.

    The MQTTRealtime class creates and controls a threaded object of class
    MQTTRealtimeThread that generates bootstrap.json. Class MQTTRealtime feeds
    the MQTTRealtimeThread object with data via a Queue.Queue instance.
    """

    def __init__(self, engine, config_dict):
        # initialize my superclass
        super(MQTTRealtime, self).__init__(engine, config_dict)

        # create a Queue object to pass data to our thread
        self.queue = Queue.Queue()

        # get the MQTTDashboard config dictionary
        md_config_dict = config_dict.get('MQTTDashboard')
        # get the MQTTRealtime config dict
        mr_config_dict = md_config_dict.get('MQTTRealtime')
        if mr_config_dict is None:
            return
        # get a MQTT config dict from [MQTTDashboard] to use as a starting
        # point
        mqtt_config_dict = md_config_dict.get('MQTT', {})
        # merge any MQTT overrides that may be specified under
        # [MQTTWU][[MQTT]]
        mqtt_config_dict.merge(mr_config_dict.get('MQTT'))

        # get an instance of class MQTTRealtimeThread and start the thread
        # running
        self.thread = MQTTRealtimeThread(self.queue,
                                         config_dict,
                                         mr_config_dict,
                                         mqtt_config_dict,
                                         lat=engine.stn_info.latitude_f,
                                         long=engine.stn_info.longitude_f,
                                         alt=convert(engine.stn_info.altitude_vt,
                                                     'meter').value)
        self.thread.start()

        # bind ourself to the relevant weeWX events
        self.bind(weewx.NEW_LOOP_PACKET, self.new_loop_packet)
        self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)
        self.bind(weewx.END_ARCHIVE_PERIOD, self.end_archive_period)

        _server = user.mqtt_utility.obfuscate_password(mqtt_config_dict.get('server_url'))
        _topic = mqtt_config_dict.get('topic',
                                      'weather/realtime')
        # log what will be done
        loginf("mqttrealtime",
               "Data will be published to MQTT broker '%s' under topic '%s'" % (_server,
                                                                                _topic))
        _min_interval = to_int(mr_config_dict.get('min_interval', None))
        # log when it will be done
        if _min_interval is None:
            _interval_str = 'None'
        elif _min_interval == 1:
            _interval_str = '1 second'
        else:
            _interval_str = '%s seconds' % _min_interval
        loginf("mqttrealtime",
               "Minimum MQTT publish interval is %s" % _interval_str)
        # log JSON map
        _json_config_map = mr_config_dict.get('JSONMap', {})
        loginf("mqttrealtime",
               "JSON config map=%s" % json.dumps(_json_config_map))

    def new_loop_packet(self, event):
        """Puts new loop packets in the queue."""

        # package the loop packet in a dict since this is not the only data
        # we send via the queue
        _package = {'type': 'loop',
                    'payload': event.packet}
        self.queue.put(_package)
        if weewx.debug == 2:
            logdbg("mqttrealtime", "Queued loop packet")
        elif weewx.debug > 2:
            logdbg("mqttrealtime", "Queued loop packet: %s" % _package['payload'])

    def new_archive_record(self, event):
        """Puts archive records in the queue."""

        # new archive record timestamp
        _package = {'type': 'archive',
                    'payload': event.record['dateTime']}
        self.queue.put(_package)
        logdbg2("mqttrealtime",
                "Queued archive record (%s)" % timestamp_to_string(event.record['dateTime']))

    def end_archive_period(self, event):
        """Puts END_ARCHIVE_PERIOD event in the queue."""

        # package the event in a dict since this is not the only data we send
        # via the queue
        _package = {'type': 'event',
                    'payload': weewx.END_ARCHIVE_PERIOD}
        self.queue.put(_package)
        logdbg2("mqttrealtime", "queued weewx.END_ARCHIVE_PERIOD event")

    def shutDown(self):
        """Shut down any threads."""

        if hasattr(self, 'queue') and hasattr(self, 'thread'):
            if self.queue and self.thread.isAlive():
                # put a None in the queue to signal the thread to shutdown
                self.queue.put(None)
                # wait up to 20 seconds for the thread to exit:
                self.thread.join(20.0)
                if self.thread.isAlive():
                    logerr("mqttrealtime",
                           "Unable to shut down %s thread" % self.thread.name)
                else:
                    logdbg("mqttrealtime",
                           "Shut down %s thread" % self.thread.name)


# ============================================================================
#                         class MQTTRealtimeThread
# ============================================================================


class MQTTRealtimeThread(threading.Thread):
    """Thread that generates JSON data and publishes to a MQTT broker."""

    def __init__(self, queue, config_dict, mr_config_dict, mqtt_config_dict, lat, long, alt):
        # Initialize my superclass:
        threading.Thread.__init__(self)

        self.setDaemon(True)
        self.queue = queue
        self.config_dict = config_dict
        self.manager_dict = manager_dict = weewx.manager.get_manager_dict_from_config(config_dict,
                                                                                      'wx_binding')

        # get MQTT config options
        server_url = mqtt_config_dict.get('server_url', None)
        self.topic = mqtt_config_dict.get('topic', 'weather/realtime')
        tls_opt = mqtt_config_dict.get('tls', None)
        retain = to_bool(mqtt_config_dict.get('retain', True))
        max_tries = to_int(mqtt_config_dict.get('max_tries', 3))
        retry_wait = to_float(mqtt_config_dict.get('retry_wait', 0.5))
        log_success = to_bool(mqtt_config_dict.get('log_success', False))

        # get a MQTTPublish object to do the publishing for us
        self.publisher = user.mqtt_utility.MQTTPublish(server=server_url,
                                                       tls=tls_opt,
                                                       retain=retain,
                                                       max_tries=max_tries,
                                                       retry_wait=retry_wait,
                                                       log_success=log_success)

        # setup file generation timing
        self.min_interval = to_int(mr_config_dict.get('min_interval', None))
        self.last_ts = 0  # ts (actual) of last generation


        # setup calculation options
        # do we have any?
        calc_dict = config_dict.get('Calculate', {})
        # algorithm
        algo_dict = calc_dict.get('Algorithm', {})
        self.solar_algorithm = algo_dict.get('maxSolarRad', 'RS')
        # atmospheric transmission coefficient [0.7-0.91]
        self.atc = to_float(calc_dict.get('atc', 0.8))
        # fail hard if out of range:
        if not 0.7 <= self.atc <= 0.91:
            raise weewx.ViolatedPrecondition("Atmospheric transmission "
                                             "coefficient (%f) out of "
                                             "range [.7-.91]" % self.atc)
        # atmospheric turbidity (2=clear, 4-5=smoggy)
        self.nfac = to_float(calc_dict.get('nfac', 2))
        # fail hard if out of range:
        if not 2 <= self.nfac <= 5:
            raise weewx.ViolatedPrecondition("Atmospheric turbidity (%d) "
                                             "out of range (2-5)" % self.nfac)

        # Get output units and decimal places
        self.temp_units = mr_config_dict['Groups'].get('group_temperature',
                                                        'degree_C')
        self.temp_dp = to_int(mr_config_dict['DecimalPlaces'].get(self.temp_units, 2))
        self.hum_units = 'percent'
        self.hum_dp = to_int(mr_config_dict['DecimalPlaces'].get(self.hum_units, 1))
        self.pres_units = mr_config_dict['Groups'].get('group_pressure',
                                                        'hPa')
        self.pres_dp = to_int(mr_config_dict['DecimalPlaces'].get(self.pres_units, 2))
        self.wind_units = mr_config_dict['Groups'].get('group_speed',
                                                        'km_per_hour')
        self.wind_dp = to_int(mr_config_dict['DecimalPlaces'].get(self.wind_units, 2))
        self.rain_units = mr_config_dict['Groups'].get('group_rain',
                                                        'mm')
        self.rain_dp = to_int(mr_config_dict['DecimalPlaces'].get(self.rain_units, 2))
        self.rainrate_units = mr_config_dict['Groups'].get('group_rainrate',
                                                            'mm_per_hour')

        self.rainrate_dp = to_int(mr_config_dict['DecimalPlaces'].get(self.rainrate_units, 2))
        self.dir_group = 'degree_compass'
        self.dir_dp = to_int(mr_config_dict['DecimalPlaces'].get(self.dir_group, 1))
        self.rad_units = 'watt_per_meter_squared'
        self.rad_dp = to_int(mr_config_dict['DecimalPlaces'].get(self.rad_units, 1))
        self.uv_units = 'uv_index'
        self.uv_dp = to_int(mr_config_dict['DecimalPlaces'].get(self.uv_units, 2))
        self.dist_units = mr_config_dict['Groups'].get('group_distance', 'km')
        self.dist_dp = to_int(mr_config_dict['DecimalPlaces'].get(self.dist_units, 2))
        self.alt_units = mr_config_dict['Groups'].get('group_altitude',
                                                       'meter')
        self.alt_dp = to_int(mr_config_dict['DecimalPlaces'].get(self.alt_units, 2))

        # what units are incoming packets using, initialise to None
        self.packet_units = None

        # get max cache age, defaul to 600
        self.max_cache_age = to_int(mr_config_dict.get('max_cache_age', 600))

        # Initialise last wind directions for use when respective direction is
        # None. We need latest and average.
        self.last_latest_dir = 0
        self.last_average_dir = 0

        # get windrun update method, loop=True, archive=False
        self.windrun_loop = to_bool(mr_config_dict.get('windrun_loop',
                                                        False))

        # Set the binding to be used for data from an additonal (ie not the
        # [StdArchive]) binding. Default to 'wx_binding'.
        self.additional_binding = mr_config_dict.get('additional_binding',
                                                      'wx_binding')

        # initialise day of the week property so when know when it's a new day
        self.dow = None

        # flag for a new day, used to reset 9am stats
        self.new_day = False

        # get some station info
        self.latitude = lat
        self.longitude = long
        self.altitude_m = alt
        self.station_type = config_dict['Station']['station_type']

        # initialise some properties that will pickup real values later
        self.db_manager = None
        self.packet_cache = None
        self.additional_manager = None
        self.day_stats = None
        self.additional_day_stats = None
        self.buffer = None

    def run(self):
        """Collect packets from the queue and manage their processing.

        Now that we are in a thread get a manager for our dbs (bindings) so we
        can initialise our forecast and day stats. Once this is done we wait
        for something in the queue.
        """

        # Would normally do this in our objects __init__ but since we are are
        # running in a thread we need to wait until the thread is actually
        # running before we can get db managers and do any associated setup.

        # get a db manager
        self.db_manager = weewx.manager.open_manager(self.manager_dict)
        # get a db manager for any additional obs (eg appTemp)
        if self.additional_binding:
            self.additional_manager = weewx.manager.open_manager_with_config(self.config_dict,
                                                                             self.additional_binding)
        # initialise our day stats
        self.day_stats = self.db_manager._get_day_summary(time.time())
        # set the unit system for our day stats
        self.day_stats.unit_system = self.db_manager.std_unit_system
        # initialise our day stats from our 'additional' source
        if self.additional_manager:
            self.additional_day_stats = self.additional_manager._get_day_summary(time.time())
            # set the unit system for our additional day stats
            self.additional_day_stats.unit_system = self.additional_manager.std_unit_system

        # get a Buffer object
        self.buffer = user.mqtt_utility.Buffer(day_stats=self.day_stats,
                                               additional_day_stats=self.additional_day_stats)

        # setup our loop cache and set some starting wind values
        _ts = self.db_manager.lastGoodStamp()
        if _ts is not None:
            _rec = self.db_manager.getRecord(_ts)
        else:
            _rec = {'usUnits': None}
        # get a CachedPacket object as our loop packet cache and prime it with
        # values from the last good archive record if available
        logdbg2("mqttrealtimethread", "Initialising loop packet cache ...")
        self.packet_cache = user.mqtt_utility.CachedPacket(_rec)
        logdbg2("mqttrealtimethread", "Loop packet cache initialised")

        # now run a continuous loop, processing data received in the queue
        while True:
            # Run an loop determine what type of data has arrived in the queue
            # and the process it as appropriate. Only jump out of the loop when
            # we receive a loop packet and provided the queue has not backed up
            # too much.
            while True:
                _package = self.queue.get()
                # a None record is our signal to exit, otherwise process the
                # package depending on what it contains
                if _package is None:
                    return
                elif _package['type'] == 'archive':
                    # received the timestamp of a new archive record
                    logdbg2("mqttrealtimethread",
                            "Received archive record timestamp")
                    self.new_archive_record(_package['payload'])
                    logdbg2("mqttrealtimethread",
                            "Processed archive record timestamp")
                    continue
                elif _package['type'] == 'event':
                    # received notification of a weeWX event
                    if _package['payload'] == weewx.END_ARCHIVE_PERIOD:
                        # end of archive period
                        logdbg2("mqttrealtimethread",
                                "Received END_ARCHIVE_PERIOD event")
                        self.end_archive_period()
                        logdbg2("mqttrealtimethread",
                                "Processed END_ARCHIVE_PERIOD event")
                    continue
                elif _package['type'] == 'stats':
                    # received a stats package
                    if weewx.debug == 2:
                        logdbg("mqttrealtimethread", "Received stats package")
                    elif weewx.debug > 2:
                        logdbg("mqttrealtimethread",
                               "Received stats package payload=%s" % (_package['payload'], ))
                    self.process_stats(_package['payload'])
                    logdbg2("mqttrealtimethread", "Processed stats package")
                    continue
                # if packets have backed up in the rtd queue, trim it until
                # it's no bigger than the max allowed backlog
                if self.queue.qsize() <= 5:
                    break

            # if necessary log receipt of the packet
            if weewx.debug == 2:
                logdbg("mqttrealtimethread", "Received loop packet")
            elif weewx.debug > 2:
                logdbg("mqttrealtimethread",
                       "Received loop packet: %s" % _package['payload'])
            # process the packet, wrap in a try..except to catch any errors
            try:
                self.process_packet(_package['payload'])
            except Exception, e:
                # Some unknown exception occurred. This is probably a serious
                # problem. Exit.
                logcrit("mqttrealtimethread",
                        "Unexpected exception of type %s" % (type(e), ))
                weeutil.weeutil.log_traceback('*** ', syslog.LOG_DEBUG)
                logcrit("mqttrealtimethread",
                        "Thread exiting. Reason: %s" % (e, ))
                return

    def process_packet(self, packet):
        """Process incoming loop packets, generate and process JSON data.

        Input:
            packet: dict containing the loop packet to be processed
        """

        # get time for debug timing
        t1 = time.time()

        # we are working MetricWX so convert to MetricWX
        packet_wx = weewx.units.to_METRICWX(packet)

        # update the packet cache with this packet
        self.packet_cache.update(packet_wx, packet_wx['dateTime'])

        # is this the first packet of the day, if so we need to reset our
        # buffer day stats
        dow = time.strftime('%w', time.localtime(packet_wx['dateTime']))
        if self.dow is not None and self.dow != dow:
            self.new_day = True
            self.buffer.start_of_day_reset()
            logdbg2("mqttrealtimethread", "Buffer day max/min reset")
        self.dow = dow

        # if this is the first packet after 9am we need to reset any 9am sums
        # first get the current hour as an int
        _hour = int(time.strftime('%w', time.localtime(packet_wx['dateTime'])))
        # if its a new day and hour>=9 we need to reset any 9am sums
        if self.new_day and _hour >= 9:
            self.new_day = False
            self.buffer.nineam_reset()
            logdbg2("mqttrealtimethread", "Buffer 9am reset")

        # now add the packet to our buffer
        self.buffer.add_packet(packet_wx)

        # generate if we have no minimum interval setting or if minimum
        # interval seconds have elapsed since our last generation
        if self.min_interval is None or (self.last_ts + float(self.min_interval)) < time.time():
            try:
                # get a cached packet
                cached_packet = self.packet_cache.get_packet(packet_wx['dateTime'],
                                                             self.max_cache_age)
                logdbg3("mqttrealtimethread",
                        "Cached loop packet: %s" % (cached_packet,))
                # get a data dict from which to construct our JSON data
                data = self.calculate(cached_packet)
                # set our generation time
                self.last_ts = cached_packet['dateTime']
                # publish the data
                self.publisher.publish(self.topic,
                                       json.dumps(data),
                                       data['dateTime']['now'])
                # log the generation
                logdbg("mqttrealtimethread",
                       "Packet (%s) processed in %.5f seconds" % (cached_packet['dateTime'],
                                                                  (self.last_ts-t1)))
            except FailedPost, e:
                # data could not be published, log and continue
                logerr("mqttrealtimethread",
                       "Data was not published: %s" % (e, ))
            except Exception, e:
                # Some unknown exception occurred. This is probably a serious
                # problem. Exit.
                logcrit("mqttrealtimethread",
                        "Unexpected exception of type %s" % (type(e), ))
                weeutil.weeutil.log_traceback('mqttrealtimethread: **** ')
                logcrit("mqttrealtimethread", "Thread exiting. Reason: %s" % (e, ))
        else:
            # we skipped this packet so log it
            logdbg("mqttrealtimethread",
                   "Packet (%s) skipped" % packet['dateTime'])

    def process_stats(self, package):
        """Process a stats package.

        Input:
            package: dict containing the stats data to process
        """

        if package is not None:
            for key, value in package.iteritems():
                setattr(self, key, value)

    def new_archive_record(self, ts):
        """Control processing when new a archive record is presented."""

        # refresh our day (archive record based) stats to date in case we have
        # jumped to the next day
        self.day_stats = self.db_manager._get_day_summary(ts)
        if self.additional_manager:
            self.additional_day_stats = self.additional_manager._get_day_summary(ts)

    def end_archive_period(self):
        """Control processing at the end of each archive period."""

        for obs in user.mqtt_utility.SUM_MANIFEST:
            self.buffer[obs].interval_reset()

    def calculate(self, packet):
        """Construct a data dict for bootstrap.json.

        Input:
            packet: loop data packet

        Returns:
            Dictionary of bootstrap.json data elements.
        """

        packet_d = dict(packet)
        ts = packet_d['dateTime']

        # get units and groups used in the buffer for each of groups of obs
        # that have multiple possible units
        (b_temp_unit, b_temp_group) = getStandardUnitType(self.buffer.primary_unit_system,
                                                          'outTemp')
        (b_pres_unit, b_pres_group) = getStandardUnitType(self.buffer.primary_unit_system,
                                                          'barometer')
        (b_wind_unit, b_wind_group) = getStandardUnitType(self.buffer.primary_unit_system,
                                                          'windSpeed')
        (b_rain_unit, b_rain_group) = getStandardUnitType(self.buffer.primary_unit_system,
                                                          'rain')
        # initialise our result containing dict
        data = {}

        ### dateTime fields
        dateTime = {}
        # now
        dateTime['now'] = ts
        # add dateTime fields to our data
        data['dateTime'] = dateTime

        ### outTemp fields
        outTemp = {}
        # now
        _outTemp_now_vt = weewx.units.as_value_tuple(packet_d, 'outTemp')
        _outTemp_now = weewx.units.convert(_outTemp_now_vt, self.temp_units).value
        outTemp['now'] = self.format(_outTemp_now, self.temp_dp)
        # trend
        _outTemp_trend = user.mqtt_utility.calc_trend('outTemp', _outTemp_now_vt,
                                                      self.temp_units, self.db_manager,
                                                      ts - 3600, 300)
        outTemp['trend'] = self.format(_outTemp_trend, self.temp_dp)
        _outTemp_24h_trend = user.mqtt_utility.calc_trend('outTemp', _outTemp_now_vt,
                                                          self.temp_units, self.db_manager,
                                                          ts - 86400, 300)
        outTemp['24h_trend'] = self.format(_outTemp_24h_trend, self.temp_dp)
        # today
        outTemp['today'] = {}
        _outTemp_min_vt = ValueTuple(self.buffer['outTemp'].day_min,
                                     b_temp_unit, b_temp_group)
        _outTemp_min = weewx.units.convert(_outTemp_min_vt, self.temp_units).value
        outTemp['today']['min'] = self.format(_outTemp_min, self.temp_dp)
        outTemp['today']['min_t'] = self.buffer['outTemp'].day_mintime
        _outTemp_max_vt = ValueTuple(self.buffer['outTemp'].day_max,
                                     b_temp_unit, b_temp_group)
        _outTemp_max = weewx.units.convert(_outTemp_max_vt, self.temp_units).value
        outTemp['today']['max'] = self.format(_outTemp_max, self.temp_dp)
        outTemp['today']['max_t'] = self.buffer['outTemp'].day_maxtime
        # add outTemp fields to our data
        data['outTemp'] = outTemp

        ### outHumidity fields
        outHumidity = {}
        # now
        outHumidity['now'] = self.format(packet_d['outHumidity'], self.hum_dp)
        # # trend
        _outHumidity_now_vt = ValueTuple(packet_d['outHumidity'], 'percent',
                                         'group_percent')
        _outHumidity_trend = user.mqtt_utility.calc_trend('outHumidity', _outHumidity_now_vt,
                                                          'percent', self.db_manager,
                                                          ts - 3600, 300)
        outHumidity['trend'] = self.format(_outHumidity_trend, self.hum_dp)
        # today
        outHumidity['today'] = {}
        outHumidity['today']['min'] = self.format(self.buffer['outHumidity'].day_min,
                                                  self.hum_dp)
        outHumidity['today']['min_t'] = self.buffer['outHumidity'].day_mintime
        outHumidity['today']['max'] = self.format(self.buffer['outHumidity'].day_max,
                                                  self.hum_dp)
        outHumidity['today']['max_t'] = self.buffer['outHumidity'].day_maxtime
        # add outHumidity fields to our data
        data['outHumidity'] = outHumidity

        ### UV fields
        UV = {}
        # now
        UV['now'] = self.format(packet_d['UV'], self.uv_dp)
        # today
        UV['today'] = {}
        UV['today']['max'] = self.format(self.buffer['UV'].day_max, self.uv_dp)
        UV['today']['max_t'] = self.buffer['UV'].day_maxtime
        # add UV fields to our data
        data['UV'] = UV

        ### radiation fields
        radiation = {}
        # now
        radiation['now'] = self.format(packet_d['radiation'], self.rad_dp)
        # today
        radiation['today'] = {}
        radiation['today']['max'] = self.format(self.buffer['radiation'].day_max,
                                                self.rad_dp)
        radiation['today']['max_t'] = self.buffer['radiation'].day_maxtime
        # add radiation fields to our data
        data['radiation'] = radiation

        ### barometer fields
        barometer = {}
        # now
        _barometer_now_vt = weewx.units.as_value_tuple(packet_d, 'barometer')
        _barometer_now = weewx.units.convert(_barometer_now_vt,
                                             self.pres_units).value
        barometer['now'] = self.format(_barometer_now, self.pres_dp)
        # trend
        _barometer_trend = user.mqtt_utility.calc_trend('barometer', _barometer_now_vt,
                                                        self.pres_units, self.db_manager,
                                                        ts - 10800, 300)
        barometer['trend'] = self.format(_barometer_trend, self.pres_dp)
        # today
        barometer['today'] = {}
        _barometer_min_vt = ValueTuple(self.buffer['barometer'].day_min,
                                       b_pres_unit, b_pres_group)
        _barometer_min = weewx.units.convert(_barometer_min_vt,
                                             self.pres_units).value
        barometer['today']['min'] = self.format(_barometer_min, self.pres_dp)
        barometer['today']['min_t'] = self.buffer['barometer'].day_mintime
        _barometer_max_vt = ValueTuple(self.buffer['barometer'].day_max,
                                       b_pres_unit, b_pres_group)
        _barometer_max = weewx.units.convert(_barometer_max_vt, self.pres_units).value
        barometer['today']['max'] = self.format(_barometer_max, self.pres_dp)
        barometer['today']['max_t'] = self.buffer['barometer'].day_maxtime
        # add barometer fields to our data
        data['barometer'] = barometer

        ### windchill fields
        windchill = {}
        # now
        _windchill_now_vt = weewx.units.as_value_tuple(packet_d, 'windchill')
        _windchill_now = weewx.units.convert(_windchill_now_vt, self.temp_units).value
        windchill['now'] = self.format(_windchill_now, self.temp_dp)
        # today
        windchill['today'] = {}
        _windchill_min_vt = ValueTuple(self.buffer['windchill'].day_min,
                                       b_temp_unit, b_temp_group)
        _windchill_min = weewx.units.convert(_windchill_min_vt, self.temp_units).value
        windchill['today']['min'] = self.format(_windchill_min, self.temp_dp)
        windchill['today']['min_t'] = self.buffer['windchill'].day_mintime
        # add windchill fields to our data
        data['windchill'] = windchill

        ### heatindex fields
        heatindex = {}
        # now
        _heatindex_now_vt = weewx.units.as_value_tuple(packet_d, 'heatindex')
        _heatindex_now = weewx.units.convert(_heatindex_now_vt, self.temp_units).value
        heatindex['now'] = self.format(_heatindex_now, self.temp_dp)
        # today
        heatindex['today'] = {}
        _heatindex_max_vt = ValueTuple(self.buffer['heatindex'].day_max,
                                       b_temp_unit, b_temp_group)
        _heatindex_max = weewx.units.convert(_heatindex_max_vt, self.temp_units).value
        heatindex['today']['max'] = self.format(_heatindex_max, self.temp_dp)
        heatindex['today']['max_t'] = self.buffer['heatindex'].day_maxtime
        # add heatindex fields to our data
        data['heatindex'] = heatindex

        ### dewpoint fields
        dewpoint = {}
        # now
        _dewpoint_now_vt = weewx.units.as_value_tuple(packet_d, 'dewpoint')
        _dewpoint_now = weewx.units.convert(_dewpoint_now_vt, self.temp_units).value
        dewpoint['now'] = self.format(_dewpoint_now, self.temp_dp)
        # trend
        _dewpoint_trend = user.mqtt_utility.calc_trend('dewpoint', _dewpoint_now_vt,
                                                       self.temp_units, self.db_manager,
                                                       ts - 3600, 300)
        dewpoint['trend'] = self.format(_dewpoint_trend, self.temp_dp)
        # today
        dewpoint['today'] = {}
        _dewpoint_min_vt = ValueTuple(self.buffer['dewpoint'].day_min,
                                      b_temp_unit, b_temp_group)
        _dewpoint_min = weewx.units.convert(_dewpoint_min_vt, self.temp_units).value
        dewpoint['today']['min'] = self.format(_dewpoint_min, self.temp_dp)
        dewpoint['today']['min_t'] = self.buffer['dewpoint'].day_mintime
        _dewpoint_max_vt = ValueTuple(self.buffer['dewpoint'].day_max,
                                      b_temp_unit, b_temp_group)
        _dewpoint_max = weewx.units.convert(_dewpoint_max_vt, self.temp_units).value
        dewpoint['today']['max'] = self.format(_dewpoint_max, self.temp_dp)
        dewpoint['today']['max_t'] = self.buffer['dewpoint'].day_maxtime
        # add dewpoint fields to our data
        data['dewpoint'] = dewpoint

        ### appTemp fields
        appTemp = {}
        # now
        _appTemp_now_vt = weewx.units.as_value_tuple(packet_d, 'appTemp')
        _appTemp_now = weewx.units.convert(_appTemp_now_vt, self.temp_units).value
        appTemp['now'] = self.format(_appTemp_now, self.temp_dp)
        # today
        appTemp['today'] = {}
        _appTemp_min_vt = ValueTuple(self.buffer['appTemp'].day_min,
                                     b_temp_unit, b_temp_group)
        _appTemp_min = weewx.units.convert(_appTemp_min_vt, self.temp_units).value
        appTemp['today']['min'] = self.format(_appTemp_min, self.temp_dp)
        appTemp['today']['min_t'] = self.buffer['appTemp'].day_mintime
        _appTemp_max_vt = ValueTuple(self.buffer['appTemp'].day_max,
                                     b_temp_unit, b_temp_group)
        _appTemp_max = weewx.units.convert(_appTemp_max_vt, self.temp_units).value
        appTemp['today']['max'] = self.format(_appTemp_max, self.temp_dp)
        appTemp['today']['max_t'] = self.buffer['appTemp'].day_maxtime
        # add appTemp fields to our data
        data['appTemp'] = appTemp

        ### humidex fields
        humidex = {}
        # now
        if 'humidex' in packet_d:
            _humidex_now_vt = weewx.units.as_value_tuple(packet_d, 'humidex')
        elif 'outTemp' in packet_d and 'outHumidity' in packet_d:
            if packet_d['usUnits'] == weewx.US:
                _humidex_F = wxformulas.humidexF(packet_d['outTemp'],
                                                 packet_d['outHumidity'])
                _humidex_now_vt = ValueTuple(_humidex_F, 'degree_F',
                                             'group_temperature')
            else:
                _humidex_C = wxformulas.humidexC(packet_d['outTemp'],
                                                 packet_d['outHumidity'])
                _humidex_now_vt = ValueTuple(_humidex_C, 'degree_C',
                                             'group_temperature')
        else:
            _humidex_now_vt = ValueTuple(None, 'degree_C', 'group_temperature')
        _humidex_now = weewx.units.convert(_humidex_now_vt, self.temp_units).value
        humidex['now'] = self.format(_humidex_now, self.temp_dp)
        # add humidex fields to our data
        data['humidex'] = humidex

        ### wind fields
        wind = {}
        # windSpeed
        wind['windSpeed'] = {}
        # now
        _windSpeed_now_vt = weewx.units.as_value_tuple(packet_d, 'windSpeed')
        _windSpeed_now = weewx.units.convert(_windSpeed_now_vt, self.wind_units).value
        wind['windSpeed']['now'] = self.format(_windSpeed_now, self.wind_dp)
        # today
        wind['windSpeed']['today'] = {}
        wind['windSpeed']['today']['avg'] = self.format(self.buffer['wind'].day_vec_avg,
                                                        self.wind_dp)
        # windDir
        wind['windDir'] = {}
        # now
        wind['windDir']['now'] = self.format(packet_d['windDir'], self.dir_dp)
        # today
        wind['windDir']['today'] = {}
        wind['windDir']['today']['avg'] = self.format(self.buffer['wind'].day_vec_dir,
                                                      self.dir_dp)
        # windGust
        wind['windGust'] = {}
        # now
        _windGust_now_vt = weewx.units.as_value_tuple(packet_d, 'windGust')
        _windGust_now = weewx.units.convert(_windGust_now_vt, self.wind_units).value
        wind['windGust']['now'] = self.format(_windGust_now, self.wind_dp)
        # today
        wind['windGust']['today'] = {}
        _windGust_max_vt = ValueTuple(self.buffer['wind'].day_max,
                                      b_wind_unit, b_wind_group)
        _windGust_max = weewx.units.convert(_windGust_max_vt, self.wind_units).value
        wind['windGust']['today']['max'] = self.format(_windGust_max,
                                                       self.wind_dp)
        wind['windGust']['today']['max_dir'] = self.format(self.buffer['wind'].day_max_dir,
                                                           self.dir_dp)
        wind['windGust']['today']['max_t'] = self.buffer['wind'].day_maxtime
        # add wind fields to our data
        data['wind'] = wind

        ### rain fields
        rain = {}
        # today
        _rain_vt = ValueTuple(self.buffer['rain'].day_sum, b_rain_unit,
                              b_rain_group)
        _rain = weewx.units.convert(_rain_vt, self.rain_units).value
        rain['today'] = self.format(_rain, self.rain_dp)
        # since 9am
        _rain_9am_vt = ValueTuple(self.buffer['rain'].nineam_sum, b_rain_unit,
                                  b_rain_group)
        _rain_9am = weewx.units.convert(_rain_9am_vt, self.rain_units).value
        rain['9am'] = self.format(_rain_9am, self.rain_dp)
        # rainRate
        _rainRate_vt = weewx.units.as_value_tuple(packet_d, 'rainRate')
        _rainRate = weewx.units.convert(_rainRate_vt, self.rainrate_units).value
        rain['rainRate'] = self.format(_rainRate, self.rainrate_dp)
        # add rain fields to our data
        data['rain'] = rain

        return data

    @staticmethod
    def format(value, places=None):
        """Format a number to a given number of decimal places.

        Format a number, that could be None, to a given number of decimal
        places. If value is None then None is returned. If places is None then
        value is returned unchanged. Otherwise value is rounded to 'places'.

        Inputs:
            value: the value to be formatted, may be numeric or None
            places: the number of places to round to

        Returns:
            None if value is None, otherwise a float
        """

        if value is None:
            return None
        elif places is None:
            return value
        else:
            return round(value, places)


