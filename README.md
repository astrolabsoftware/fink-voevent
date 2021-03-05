# Fink VOEvent integration

the goal of this repo is to have a handler for COMET to process and store in Parquet received VOEvent.

Handler:
- extract content of the alert based on its provider information (GRB, GW, neutrino, ...)
- Skip calibration, tests, or fake injections
- Save alert content in a Parquet file in HDFS


export HADOOP_HOME=$HADOOP_HDFS_HOME

Comet usage

```bash
# listen to voevent.4pisky.org
twistd -n comet \
  --verbose \
  --local-ivo=ivo://finkbroker/$(hostname) \
  --remote=voevent.4pisky.org \
  --cmd=fink_voevent/vo_writer.py
```

Debug
```bash

```
