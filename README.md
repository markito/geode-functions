[![Build Status](https://travis-ci.org/markito/geode-functions.svg?branch=master)](https://travis-ci.org/markito/geode-functions)
[![Test Coverage](https://codeclimate.com/github/markito/geode-functions/badges/coverage.svg)](https://codeclimate.com/github/markito/geode-functions/coverage)

# geode-functions
Collection of Apache Geode util functions.

| Function name | Function ID | Description |
|---------------|-------------|-------------|
|  CopyRegion   |  CopyRegion | Executes OnRegion and receives destination region name as `String` |
|  ClearRegionFunction   |  ClearRegionFunction | Executes OnRegion and clears the local primary entries one at a time |
|  ClearRegionRemoveAllFunction   |  ClearRegionRemoveAllFunction | Executes OnRegion and clears the local primary entries all at once using removeAll |

Compatible with Apache Geode 1.0
