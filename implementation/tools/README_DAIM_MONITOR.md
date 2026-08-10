# daim-monitor

`daim_monitor.py` is an independent fault-monitoring daemon for the Linux/OVS
experiments. It polls an OVS interface, records detection, policy-decision,
and actuation timestamps, and invokes only a configured repair executable.
The experiment process must not invoke the repair executable itself. In the
full integrated run the repair executable is the DAIM OVS adapter path and its
stdout/stderr are retained as evidence.
