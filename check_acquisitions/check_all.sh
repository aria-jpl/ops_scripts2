#!/bin/bash

cat "aoi_list.txt" | while read line
do
    aoi=$line
    #echo $aoi
    track=$(echo $aoi | sed -n -e 's/.*[Tt][nN]\([0-9][0-9][0-9]\).*/\1/p')
    echo "RUNNING OVER AOI: $aoi & TRACK: $track"
    echo "------------------------------------------------------------------------"
    echo "CHECKING $aoi ACQUISITIONS..."
    ./check_acquisition_completeness.py --aoi $aoi --track $track
    echo "CHECKING $aoi IPFS..."
    ./check_ipf_completeness.py --aoi $aoi --track $track
    echo ""
done
