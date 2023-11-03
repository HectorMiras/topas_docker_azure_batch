#!/bin/bash

INFILE1="I125_seed_in_water.txt" 

CURRENTPATH=`pwd`
SEED=`bash -c 'echo $RANDOM'`
sed -i "s/i:Ts\/Seed = .*/i:Ts\/Seed = $SEED/" $CURRENTPATH/$INFILE1
# Topas path in docker container:
time /topas/bin/topas $CURRENTPATH/$INFILE1
# Local topas path
# time ~/topas/bin/topas $CURRENTPATH/$INFILE1
	
