# observation-planner
Python code for the observation planner

This repository contains the code for generating coordinated observation plans for a constellation of earth observing satellites. It also contains the preprocssing code for generating planner inputs from raw data produced by the code in the other repositories.

Requires Python 3.8 or higher.

# Input data required:
* payload access files: Specifies "access times" and viewing angles for each payload, and which Ground Positions (GP) are visible by the payload at that time and viewing angle
* eclipse files: specifies when each satellite is in eclipes
* slewTable: specifies the time and energy required for slewing from one viewing angle to another
* GP grid: specifies 1.67 million ground position ID's along with the latitude, longitude and biome type for each GP
* soil moisture prediction error: specifies the prediction error associated with each ground position
* measurement error table: specifies the measurement error associate with each payload independently and in combination, at all possible viewing angle combinations
