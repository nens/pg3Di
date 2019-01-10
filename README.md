## What is pg3Di?
pg3Di is an extension for postgres databases to store, analyze and manipulate 3Di models. It depends on the PostGIS extension.

It is meant to reduce the amount of SQL needed to achieve the same thing. It does not include any functionality for things that are already easily done without the extension.

## Naming system
Functions, arguments and outputs are systematically named, based on a number of objects. The naming is meant to be intuitive and stay as close as possible to the naming systems of existing 3Di tooling, such as the threedigrid python library and the names in the 3Di spatialite. The following object groups are distinguished:

## Node object - Object that refers to only one connection node
ConnectionNode
Manhole
Surface
ImperviousSurface
Lateral
OneDBoundaryCondition

## Line - Object that connects two connection nodes
Channel
Culvert
Weir
Orifice
Pumpstation
Pipe

## Settings
global settings
numerical settings
Aggregation settings
groundwater settings
Simple Infiltration Settings
infiltration settings
Interflow settings

## 2D Objects
2D Boundary Condition
2D Lateral
Grid Refinement Line
Grid Refinement Area
Obstacle
Levee

## Other objects
Cross section location
Cross section definition
Breach
Control
Surface Parameters

## Network
A group of connected node objects and lines

## Defaults


## Constraints


## Backups


## Stylings
QGIS stylings saved in PostGIS database

## Extents
1D Extent
2D Extent
