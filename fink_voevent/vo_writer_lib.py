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
import os
import sys
import string
import doctest

import pyarrow
import pyarrow.parquet as pq

def get_hdfs_connector(host: str, port: int, user: str):
    """ Initialise a connector to HDFS

    Parameters
    ----------
    host: str
        IP address for the host machine
    port: int
        Port to access HDFS data.
    user: str
        Username on Hadoop.

    Returns
    ----------
    fs: pyarrow.hdfs.HadoopFileSystem
    """
    return pyarrow.hdfs.connect(host=host, port=port, user=user)

def write_dataframe(df, outpath: str, fs=None):
    """ Write a Pandas DataFrame to HDFS

    Parameters
    ----------
    df: Pandas DataFrame
        Input Pandas DataFrame containing alert data
    outpath: str
        Full path (folder+filename) where to store the data.
    fs: filesystem, optional
        If None (default), assume file-like object. For
        HDFS see `pyarrow.hdfs`.
    """
    table = pyarrow.Table.from_pandas(df)

    pq.write_table(table, outpath, filesystem=fs)

def check_dir_exist(directory: str, usehdfs: bool) -> bool:
    """ Check if `directory` exists.

    Note that it also supports hdfs.

    Parameters
    ----------
    directory: str
        Path to a directory. Can be hdfs:///path/to/somewhere
    usehdfs: bool
        Set it to True for HDFS file system.

    Returns
    ----------
    ok: bool
        True if the directory exists, False otherwise
    """
    if usehdfs:
        cmd = 'hdfs dfs -stat {}'.format(directory)
        ok = os.system(cmd) == 0
    else:
        ok = os.path.exists(directory)

    return ok

def string_to_filename(input_string: str, replaceby: str = '_') -> str:
    """ Strip weird, confusing or special characters from input_string

    Parameters
    ----------
    input_string: str
        Input string with any character
    replaceby: str
        character to use to replace "/" and "\"

    Returns
    ----------
    out: str
        Sanitized input_string that can safely be used as filename.

    Examples
    ----------
    >>> bad_string = "i_am/a_string"
    >>> good_string = string_to_filename(bad_string)
    >>> print(good_string)
    i_am_a_string
    """
    # Allow ".", but not as the first character.
    if input_string[0] == ".":
        input_string = input_string[1:]

    # Replace "/" and "\" with `replaceby` for readability.
    striped = input_string.replace("/", replaceby).replace("\\", replaceby)

    # characters allowed
    allowed = string.digits + string.ascii_letters + "_."

    return "".join(x for x in striped if x in allowed)

def is_observation(data) -> bool:
    """ check if the event is a real observation based on the event role.

    Parameters
    ----------
    data: xml
        decoded voevent

    Returns
    ----------
    out: bool
        True if the event role is an observation.
        False otherwise (test/utility).
    """
    return data.attrib['role'] == 'observation'


if __name__ == "__main__":
    sys.exit(doctest.testmod()[0])
