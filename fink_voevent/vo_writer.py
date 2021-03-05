#!/usr/bin/env python
# Copyright 2020 AstroLab Software
# Author: Julien Peloton
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import sys
import pandas as pd

import voeventparse

import fink_voevent.vo_writer_lib as vo

EVENTDIR: str = "vodb"
USEHDFS: bool = False
HOST: str = None
PORT: int = None
USER: str = None

def handler():
    """ Convert VOEvent from stdin to parquet file, and store it.

    The user will have to define the following constants:
    EVENTDIR: str
    USEHDFS: bool
    HOST: str
    PORT: int
    USER: str

    They are stored in the fink_voevent/vo_writer.py file.

    Usage as a Comet handler:
        twistd -n comet --verbose --local-ivo=ivo://fink-broker/$(hostname)\
            --remote=voevent.4pisky.org --cmd=fink_voevent/vo_writer.py

    Usage as a standard script:
        cat a_voevent_from_disk | fink_voevent/vo_writer.py
    """
    # Check if the outdir exists
    if not vo.check_dir_exist(EVENTDIR, USEHDFS):
        print("EVENTDIR={} does not exist".format(EVENTDIR))
        print("Create it or edit fink_broker/vo_writer.py")
        sys.exit()

    # Read the data from the stdin (string)
    packet_data = sys.stdin.buffer.read()

    # Load as XML
    xml_packet_data = voeventparse.loads(packet_data)

    # Extract the ivorn
    ivorn = xml_packet_data.attrib['ivorn']

    # Skip if the event is a test
    if not vo.is_observation(xml_packet_data):
        print('test/utility received - {}'.format(ivorn))
        return 0

    # Extract information about position and time
    coords = voeventparse.get_event_position(xml_packet_data)
    time_utc = str(voeventparse.get_event_time_as_utc(xml_packet_data))

    # Store useful information for coincidence in a DataFrame
    df = pd.DataFrame.from_dict(
        {
            'ivorn': [ivorn],
            'ra': [coords.ra],
            'dec': [coords.dec],
            'err': [coords.err],
            'units': [coords.units],
            'timeUTC': [time_utc],
            'raw_event': packet_data}
    )

    # Filename for the event is based on the ivorn.
    fn = '{}/{}.parquet'.format(EVENTDIR, vo.string_to_filename(ivorn))

    # Get the connector
    if USEHDFS:
        fs = vo.get_hdfs_connector(HOST, PORT, USER)
    else:
        fs = None
    vo.write_dataframe(df, outpath=fn, fs=fs)

    return 0


if __name__ == '__main__':
    sys.exit(handler())
