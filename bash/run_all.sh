#!/bin/bash

gnome-terminal -- bash -c "./run_producer.sh; exec bash"
gnome-terminal -- bash -c "./run_spark.sh; exec bash"
gnome-terminal -- bash -c "./run_influx_writer.sh; exec bash"
gnome-terminal -- bash -c "./run_dashboard.sh; exec bash"