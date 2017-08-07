The MQTT Dashboard extension publishes various weather observations to a MQTT
broker to support a MQTT based weather dashboard. The extension consists of
three weeWX report services that publish data to a number of topics on a MQTT
broker.

Pre-Requisites

The MQTT Dashboard extension requires:

-   weeWX v3.4.0 or greater

-   installation of the Paho MQTT Python client

Installation Instructions

Installation using the wee_extension utility

Note:   Symbolic names are used below to refer to some file location on the
weeWX system. These symbolic names allow a common name to be used to refer to
a directory that may be different from system to system. The following symbolic
names are used below:

-   $DOWNLOAD_ROOT. The path to the directory containing the downloaded
    MQTT Dashboard extension.

-   $BIN_ROOT. The path to the directory where weeWX executables are located.
    This directory varies depending on weeWX installation method. Refer to
    'where to find things' in the weeWX User's Guide:
    http://weewx.com/docs/usersguide.htm#Where_to_find_things for further
    information.

1.  Download the latest MQTT Dashboard extension from the MQTT Dashboard
releases page (https://github.com/gjr80/weewx_utilities/releases) into a
directory accessible from the weeWX machine.

    wget -P $DOWNLOAD_ROOT https://github.com/gjr80/weewx_utilities/releases/download/v0.2.14/mqttdashboard-0.1.0.tar.gz

	where $DOWNLOAD_ROOT is the path to the directory where the MQTT Dashboard
    extension is to be downloaded.

2.  Stop weeWX:

    sudo /etc/init.d/weewx stop

	or

    sudo service weewx stop

3.  Install the MQTT Dashboard extension downloaded at step 1 using the
wee_extension utility:

    wee_extension --install=$DOWNLOAD_ROOT/mqttdashboard-0.1.0.tar.gz

    This will result in output similar to the following:

        Request to install '/var/tmp/mqttdashboard-0.1.0.tar.gz'
        Extracting from tar archive /var/tmp/mqttdashboard-0.1.0.tar.gz
        Saving installer file to /home/weewx/bin/user/installer/MQTT_Dashboard
        Saved configuration dictionary. Backup copy at /home/weewx/weewx.conf.20161123124410
        Finished installing extension '/var/tmp/mqttdashboard-0.1.0.tar.gz'

4.  Edit weewx.conf and locate the [MQTTDashboard] stanza. Edit the existing
[MQTTDashboard] configuration options to suit. In particular, make sure any
configuration options that include 'replace_me' are changed. Save weewx.conf.

5.  Start weeWX:

    sudo /etc/init.d/weewx start

	or

    sudo service weewx start

This will result in various JSON format data packages being published to the
topics under [MQTTDashboard]. MQTTRealtime will publish data on arrival of
every loop packet. MQTTArchive will publish data on arrival of every archive
record. MQTTWU will publish data based on the [[MQTTWU]] [[[WU]]]
forecast_interval and conditions_interval configuration options.

Further customization of the MQTTDashboard services can be undertaken by
referring to the MQTT Dashboard extension wiki (to be written).

Manual installation

1.  Download the latest MQTT Dashboard extension from the MQTT Dashboard
releases page (https://github.com/gjr80/weewx_utilities/releases) into a
directory accessible from the weeWX machine.

    wget -P $DOWNLOAD_ROOT https://github.com/gjr80/weewx_utilities/releases/download/v0.2.14/mqttdashboard-0.1.0.tar.gz

	where $DOWNLOAD_ROOT is the path to the directory where the MQTT Dashboard
    extension is to be downloaded.

2.  Unpack the extension as follows:

    tar xvfz mqttdashboard-0.1.0.tar.gz

3.  Copy files from within the resulting folder as follows:

    cp md/bin/user/mqtt*.py $BIN_ROOT/user

	replacing the symbolic name $BIN_ROOT with the nominal locations for your
    installation.

4.  Edit weewx.conf:

    vi weewx.conf

5.  Locate the [MQTTDashboard] stanza. Edit the existing [MQTTDashboard]
configuration options to suit. In particular, make sure any configuration
options that include 'replace_me' are changed.

6.  Modify the [Engine] [[Services]] section by adding the MQTTDashboard
services to the list of report services to be run:

    [Engine]
        [[Services]]

            report_services = ..., user.mqtt_dashboard.MQTTRealtime, user.mqtt_dashboard.MQTTArchive, user.mqtt_dashboard.MQTTWU

7.  Save weewx.conf.

8.  Start weeWX:

    sudo /etc/init.d/weewx start

	or

    sudo service weewx start

This will result in various JSON format data packages being published to the
topics under [MQTTDashboard]. MQTTRealtime will publish data on arrival of
every loop packet. MQTTArchive will publish data on arrival of every archive
record. MQTTWU will publish data based on the [[MQTTWU]] [[[WU]]]
forecast_interval and conditions_interval configuration options.

Further customization of the MQTTDashboard services can be undertaken by
referring to the MQTT Dashboard extension wiki (to be written).
