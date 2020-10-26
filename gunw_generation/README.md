This script will determine the number of gunw products generated for the specified AOI's over the specified time range. The number of gunws generated for each tag is printed. If the verbose flag is set, the gunw id's will be printed as well. 

Here are some sample run commands:

Searches for all gunw products generated in the last two months:
```
gunws_generated.py --time 2m
```

Searches for all gunw products generated over AOI_monitoring_Tibet_D121_B and AOI_monitoring_Tibet_A114_C in the last 3 months:
```
gunws_generated.py --aoi AOI_monitoring_Tibet_D121_B,AOI_monitoring_Tibet_A114_C --time 3m
```

Searches for all gunw products generated over AOI_monitoring_Tibet_D121_B in the past day and list all the gunw id's:
```
gunws_generated.py --aoi AOI_monitoring_Tibet_D121_B --time 1d --verbose
```
