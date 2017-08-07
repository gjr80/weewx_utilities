#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation; either version 2 of the License, or (at your option) any later
# version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
# details.
#
#                        Installer for MQTT Dashboard
#
# Version: 0.1.0                                        Date: 24 July 2017
#
# Revision History
#   24 July 2017        v0.1
#       - initial implementation
#

import weewx

from distutils.version import StrictVersion
from setup import ExtensionInstaller

REQUIRED_VERSION = "3.4.0"
MD_VERSION = "0.1.0"


def loader():
    return MdInstaller()


class MdInstaller(ExtensionInstaller):
    def __init__(self):
        if StrictVersion(weewx.__version__) < StrictVersion(REQUIRED_VERSION):
            msg = "%s requires weeWX %s or greater, found %s" % ('MQTT Dashboard ' + MD_VERSION,
                                                                 REQUIRED_VERSION,
                                                                 weewx.__version__)
            raise weewx.UnsupportedFeature(msg)
        super(MdInstaller, self).__init__(
            version=MD_VERSION,
            name='MQTT_Dashboard',
            description='weeWX support for a MQTT based dashboard.',
            author="Gary Roderick",
            author_email="gjroderick<@>gmail.com",
            report_services=['user.mqtt_dashboard.MQTTRealtime',
                             'user.mqtt_dashboard.MQTTArchive',
                             'user.mqtt_dashboard.MQTTWU'],
            config={
                'MQTTDashboard': {
                    'MQTT': {
                        'server_url': 'replace_me'
                    },
                    'MQTTRealtime': {
                        'min_interval': '0',
                        'additional_binding': 'wx_binding',
                        'windrun_loop': 'True',
                        'max_cache_age': '600',
                        'Calculate': {
                            'atc': '0.8',
                            'nfac': '2',
                            'Algorithm': {
                                'maxSolarRad': 'RS'
                            }
                        },
                        'DecimalPlaces': {
                            'degree_compass': '1',
                            'degree_C': '2',
                            'degree_F': '2',
                            'foot': '2',
                            'hPa': '2',
                            'inch': '3',
                            'inch_per_hour': '3',
                            'inHg': '4',
                            'km': '2',
                            'km_per_hour': '1',
                            'mbar': '2',
                            'meter': '1',
                            'meter_per_second': '2',
                            'mile': '2',
                            'mile_per_hour': '1',
                            'mm': '2',
                            'mm_per_hour': '2',
                            'percent': '1',
                            'uv_index': '2',
                            'watt_per_meter_squared': '1',
                        },
                        'Groups': {
                            'group_altitude': 'foot',
                            'group_pressure': 'hPa',
                            'group_rain': 'mm',
                            'group_speed': 'km_per_hour',
                            'group_temperature': 'degree_C'
                        },
                        'MQTT': {
                            'topic': 'replace_me/realtime'
                        }
                    },
                    'MQTTArchive': {
                        'Formats': {
                            'degree_C': '%.1f',
                            'degree_F': '%.1f',
                            'degree_compass': '%.0f',
                            'foot': '%.0f',
                            'hPa': '%.1f',
                            'inHg': '%.2f',
                            'inch': '%.2f',
                            'inch_per_hour': '%.2f',
                            'km_per_hour': '%.1f',
                            'km': '%.1f',
                            'mbar': '%.1f',
                            'meter': '%.0f',
                            'meter_per_second': '%.1f',
                            'mile': '%.1f',
                            'mile_per_hour': '%.1f',
                            'mm': '%.1f',
                            'mm_per_hour': '%.1f',
                            'percent': '%.0f',
                            'uv_index': '%.1f',
                            'watt_per_meter_squared': '%.0f',
                            'NONE': 'None'
                        },
                        'Groups': {
                            'group_altitude': 'foot',
                            'group_pressure': 'hPa',
                            'group_rain': 'mm',
                            'group_speed': 'km_per_hour',
                            'group_temperature': 'degree_C'
                        },
                        'MQTT': {
                            'topic': 'replace_me/slow'
                        }
                    },
                    'MQTTWU': {
                        'WU': {
                            'api_key': '4306891b564ca391',
                            'forecast_interval': '1800',
                            'api_lockout_period': '60',
                            'max_WU_tries': '3',
                            'location': 'pws:IQUEENSL336',
                            'conditions_interval': '1800'
                        },
                        'MQTT': {
                            'conditions_topic': 'replace_me/conditions',
                            'forecast_topic': 'replace_me/forecast'
                        }
                    }
                }
            },
            files=[('bin/user', ['bin/user/mqtt_dashboard.py',
                                 'bin/user/mqtt_utility.py'])]
        )
