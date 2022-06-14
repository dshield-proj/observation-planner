# observation-planner
Python code for the observation planner

This repository contains the code for generating coordinated observation plans for a constellation of earth observing satellites. It also contains the preprocssing code for generating planner inputs from raw data produced by the code in the other repositories.

Requires Python 3.8 or higher.

# Input data required:
* _payload access files_: Specifies "access times" and viewing angles for each payload, and which Ground Positions (GP) are visible by the payload at that time and viewing angle
* _eclipse files_: specifies when each satellite is in eclipes
* _slewTable_: specifies the time and energy required for slewing from one viewing angle to another
* _GP grid_: specifies 1.67 million ground position ID's along with the latitude, longitude and biome type for each GP
* _soil moisture prediction error_: specifies the prediction error associated with each ground position
* _measurement error table_: specifies the measurement error associate with each payload independently and in combination, at all possible viewing angle combinations
