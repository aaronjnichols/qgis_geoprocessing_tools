# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This repository contains custom QGIS Processing algorithms for civil engineering and hydrology workflows. All tools are Python scripts that extend `QgsProcessingAlgorithm` and integrate with QGIS Processing Toolbox.

## Architecture

### Tool Structure
Each `.py` file is a standalone QGIS Processing algorithm following this pattern:
- Class inherits from `QgsProcessingAlgorithm`
- Required methods: `name()`, `displayName()`, `group()`, `groupId()`, `createInstance()`, `initAlgorithm()`, `processAlgorithm()`
- Parameters defined via `QgsProcessingParameter*` classes in `initAlgorithm()`
- Main logic in `processAlgorithm(parameters, context, feedback)`

### Common Patterns
- **AOI-based downloads**: Many tools take an AOI polygon layer and download/process data from web APIs (USGS, FEMA, NOAA, SSURGO)
- **Coordinate transforms**: Tools frequently reproject between CRS using `QgsCoordinateTransform`
- **GDAL integration**: Raster processing uses `osgeo.gdal` directly for performance
- **Auto-loading results**: Use `context.addLayerToLoadOnCompletion()` to load outputs into QGIS

### Key Dependencies
- QGIS core: `qgis.core`, `qgis.PyQt.QtCore`
- GDAL/OGR: `osgeo.gdal`, `osgeo.ogr`, `osgeo.osr`
- Standard libs: `requests`, `json`, `csv`
- Optional: `xlsxwriter`, `openpyxl`, `reportlab` for Excel/PDF exports

## Tools Summary

| Tool | Purpose |
|------|---------|
| `usgs_lidar_download.py` | Download USGS 3DEP LiDAR DEMs, create mosaic, generate contours (DXF/SHP/GPKG) |
| `fema_nfhl_download.py` | Download FEMA flood hazard layers for an AOI |
| `soils_hsg_download.py` | Download SSURGO soils data with hydrologic soil groups |
| `noaa14_download.py` | Extract NOAA Atlas 14 precipitation frequency data |
| `subbasin_curve_numbers.py` | Calculate area-weighted curve numbers for subbasins |
| `cut_fill_volumes.py` | Compare existing vs proposed DEMs for earthwork volumes |
| `hecras_ga_infiltration_layer.py` | Generate HEC-RAS Green-Ampt infiltration layers |

## Development Notes

- Tools use `feedback.pushInfo()` for progress messages and `feedback.reportError()` for errors
- For web API calls, handle pagination and timeouts appropriately
- When generating DXF files, use `ogr2ogr -zfield` for proper 3D polylines (OGR Python API doesn't preserve Z reliably)
- Clean up temp files with retry logic on Windows (file locking issues)
- Output CRS should match the input AOI's CRS for proper alignment
