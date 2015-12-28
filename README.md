iDynamic
====================================
A quick and dirty script for dynamically queueing iPython parallel engines in SLURM

Author: Alexey Gritsenko

Overview
========

```
usage: idynamic.py [-h] [--profile PROFILE] [--profile-dir PROFILE_DIR]
                   [--n N_WANTED] [--q MAX_N_QUEUED]

Dynamic iPython cluster engine queueing.

optional arguments:
  -h, --help            show this help message and exit
  --profile PROFILE     iPython profile name
  --profile-dir PROFILE_DIR
                        iPython profile directory
  --n N_WANTED          Desired number of running engines (default: n = 10)
  --q MAX_N_QUEUED      Maximum number of queued jobs (default: q = 20)
```

Prerequistes
=============

* [PySLURM](https://github.com/gingergeeks/pyslurm) for job state querying.
