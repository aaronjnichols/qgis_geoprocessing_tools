"""
QGIS Processing Tool: Download USGS 3DEP OPR DEMs
Author: Claude
Version: 1.3 - Added auto-load to map canvas
QGIS Version: 3.44+

This tool downloads USGS 3DEP OPR DEM tiles that intersect an AOI polygon
and creates a seamless mosaic with highest-resolution priority.

No external dependencies required - uses only built-in QGIS/GDAL libraries.
"""

from qgis.PyQt.QtCore import QCoreApplication, QVariant
from qgis.core import (
    QgsProcessing,
    QgsProcessingAlgorithm,
    QgsProcessingParameterVectorLayer,
    QgsProcessingParameterFolderDestination,
    QgsProcessingParameterString,
    QgsProcessingParameterBoolean,
    QgsProcessingParameterRasterDestination,
    QgsProcessingException,
    QgsVectorLayer,
    QgsGeometry,
    QgsCoordinateReferenceSystem,
    QgsCoordinateTransform,
    QgsProject,
    QgsRectangle,
    QgsRasterLayer,
    QgsMessageLog,
    Qgis
)
from qgis import processing

import os
import json
import csv
import time
import re
from urllib import request, error, parse
from osgeo import gdal, osr

class DownloadOprDemsAlgorithm(QgsProcessingAlgorithm):
    """
    QGIS Processing Algorithm for downloading USGS 3DEP OPR DEMs
    """
    
    # Define parameter names as constants
    INPUT_AOI = 'INPUT_AOI'
    OUTPUT_FOLDER = 'OUTPUT_FOLDER'
    MOSAIC_NAME = 'MOSAIC_NAME'
    CLIP_TO_AOI = 'CLIP_TO_AOI'
    OUTPUT_MOSAIC = 'OUTPUT_MOSAIC'
    
    # USGS 3DEP API constants
    INDEX_LAYER = "https://index.nationalmap.gov/arcgis/rest/services/3DEPElevationIndex/MapServer/11/query"
    OUT_FIELDS = ["workunit", "project", "ql", "dem_gsd_meters", "sourcedem_link", "metadata_link"]
    PAGE_SIZE = 2000
    
    def tr(self, string):
        """Returns a translatable string"""
        return QCoreApplication.translate('Processing', string)
    
    def createInstance(self):
        """Returns a new instance of the algorithm"""
        return DownloadOprDemsAlgorithm()
    
    def name(self):
        """Algorithm name (internal identifier)"""
        return 'usgs_lidar_download'
    
    def displayName(self):
        """Algorithm display name"""
        return self.tr('USGS LiDAR Download')
    
    def group(self):
        """Algorithm group"""
        return self.tr('Data Download Toolbox')
    
    def groupId(self):
        """Algorithm group ID"""
        return 'data_download_toolbox'
    
    def shortHelpString(self):
        """Algorithm help text"""
        return self.tr("""
        Downloads USGS 3D Elevation Program (3DEP) Original Project Raster (OPR) DEM tiles 
        that intersect the input Area of Interest (AOI) and creates a seamless mosaic.
        
        <b>Features:</b>
        • Automatic spatial filtering - downloads only tiles intersecting your AOI
        • Intelligent highest-resolution priority when multiple resolutions available
        • Creates seamless mosaic with optional clipping to AOI bounds
        • Optional conversion from meters to feet (USGS DEMs are natively in meters)
        • Saves individual tiles and manifest CSV for reference
        • Automatically loads output DEM into the map canvas
        
        <b>Inputs:</b>
        • <b>AOI Layer</b>: Polygon layer defining area of interest (any CRS)
        • <b>Output Folder</b>: Directory for outputs
        • <b>Mosaic Name</b>: Name for output mosaic file (e.g., "site_dem.tif")
        • <b>Clip to AOI</b>: Whether to clip mosaic to AOI bounds
        • <b>Convert to Feet</b>: Convert elevations from meters to feet (US standard)
        
        <b>Outputs:</b>
        • Individual DEM tiles saved to: [Output Folder]/tiles/
        • Manifest CSV saved to: [Output Folder]/tiles/manifest.csv
        • Mosaic saved to: [Output Folder]/[Mosaic Name]
        • Mosaic automatically loaded into map canvas
        
        <b>Data Source:</b>
        USGS 3D Elevation Program (3DEP) via National Map
        Native elevation units: meters
        Conversion factor: 1 meter = 3.28084 feet
        
        <b>Note:</b> Processing time depends on number of tiles (typically 2-5 minutes for 10-20 tiles)
        """)
    
    def initAlgorithm(self, config=None):
        """Define inputs and outputs"""
        
        # Input AOI layer
        self.addParameter(
            QgsProcessingParameterVectorLayer(
                self.INPUT_AOI,
                self.tr('AOI Layer (Polygon)'),
                [QgsProcessing.TypeVectorPolygon]
            )
        )
        
        # Output folder
        self.addParameter(
            QgsProcessingParameterFolderDestination(
                self.OUTPUT_FOLDER,
                self.tr('Output Folder')
            )
        )
        
        # Mosaic filename
        self.addParameter(
            QgsProcessingParameterString(
                self.MOSAIC_NAME,
                self.tr('Mosaic Filename'),
                defaultValue='opr_dem_mosaic.tif'
            )
        )
        
        # Clip to AOI option
        self.addParameter(
            QgsProcessingParameterBoolean(
                self.CLIP_TO_AOI,
                self.tr('Clip mosaic to AOI bounds'),
                defaultValue=True
            )
        )
        
        # Convert to feet option
        self.addParameter(
            QgsProcessingParameterBoolean(
                'CONVERT_TO_FEET',
                self.tr('Convert elevations from meters to feet'),
                defaultValue=True
            )
        )
        
        # Output mosaic (for auto-loading into map)
        self.addParameter(
            QgsProcessingParameterRasterDestination(
                self.OUTPUT_MOSAIC,
                self.tr('Output Mosaic DEM'),
                optional=False,
                createByDefault=True
            )
        )
    
    def processAlgorithm(self, parameters, context, feedback):
        """Main processing logic"""
        
        # Get parameters
        aoi_layer = self.parameterAsVectorLayer(parameters, self.INPUT_AOI, context)
        output_folder = self.parameterAsString(parameters, self.OUTPUT_FOLDER, context)
        mosaic_name = self.parameterAsString(parameters, self.MOSAIC_NAME, context)
        clip_to_aoi = self.parameterAsBoolean(parameters, self.CLIP_TO_AOI, context)
        convert_to_feet = self.parameterAsBoolean(parameters, 'CONVERT_TO_FEET', context)
        
        # Get output mosaic path - use the destination parameter if provided, otherwise construct from folder + name
        mosaic_path = self.parameterAsOutputLayer(parameters, self.OUTPUT_MOSAIC, context)
        if not mosaic_path or mosaic_path.strip() == '':
            mosaic_path = os.path.join(output_folder, mosaic_name)
        
        # Validate inputs
        if not aoi_layer:
            raise QgsProcessingException('Invalid AOI layer')
        
        if not aoi_layer.featureCount():
            raise QgsProcessingException('AOI layer has no features')
        
        # Create output directories
        tiles_folder = os.path.join(output_folder, 'tiles')
        os.makedirs(tiles_folder, exist_ok=True)
        
        # Ensure the output directory for mosaic exists
        mosaic_dir = os.path.dirname(mosaic_path)
        if mosaic_dir:
            os.makedirs(mosaic_dir, exist_ok=True)
        
        manifest_path = os.path.join(tiles_folder, 'manifest.csv')
        
        feedback.pushInfo(f'Output folder: {output_folder}')
        feedback.pushInfo(f'Tiles folder: {tiles_folder}')
        feedback.pushInfo(f'Mosaic output: {mosaic_path}')
        
        # Step 1: Convert AOI to WGS84 and get Esri JSON geometry
        feedback.pushInfo('\n=== Step 1: Processing AOI ===')
        esri_geom = self.aoi_to_esri_polygon(aoi_layer, feedback)
        
        # Step 2: Query USGS 3DEP Index for intersecting workunits
        feedback.pushInfo('\n=== Step 2: Querying USGS 3DEP Index ===')
        workunits = self.query_3dep_index(esri_geom, feedback)
        
        if not workunits:
            raise QgsProcessingException('No OPR DEM workunits found for this AOI')
        
        feedback.pushInfo(f'Found {len(workunits)} workunit(s)')
        
        # Step 3: Get tile URLs and filter by AOI
        feedback.pushInfo('\n=== Step 3: Filtering tiles by AOI ===')
        all_tiles = []
        
        for wu in workunits:
            feedback.pushInfo(f"\nProcessing workunit: {wu['workunit']}")
            feedback.pushInfo(f"  Project: {wu['project']}")
            feedback.pushInfo(f"  GSD: {wu['dem_gsd_meters']}m")
            
            tile_urls = self.get_tile_urls_from_workunit(wu, feedback)
            feedback.pushInfo(f"  Total tiles in workunit: {len(tile_urls)}")
            
            # Filter tiles
            filtered_urls = self.filter_tiles_by_aoi(tile_urls, aoi_layer, feedback)
            feedback.pushInfo(f"  Tiles intersecting AOI: {len(filtered_urls)}")
            
            for url in filtered_urls:
                all_tiles.append({
                    'workunit': wu['workunit'],
                    'project': wu['project'],
                    'ql': wu['ql'],
                    'dem_gsd_meters': wu['dem_gsd_meters'],
                    'url': url,
                    'metadata_link': wu['metadata_link']
                })
        
        if not all_tiles:
            raise QgsProcessingException('No tiles intersect the AOI')
        
        feedback.pushInfo(f'\n=== Total tiles to download: {len(all_tiles)} ===')
        
        # Step 4: Download tiles
        feedback.pushInfo('\n=== Step 4: Downloading tiles ===')
        results = self.download_tiles(all_tiles, tiles_folder, feedback)
        
        # Step 5: Write manifest
        feedback.pushInfo('\n=== Step 5: Writing manifest ===')
        self.write_manifest(results, manifest_path, feedback)
        
        # Step 6: Create mosaic
        feedback.pushInfo('\n=== Step 6: Creating mosaic ===')
        self.create_mosaic(results, mosaic_path, aoi_layer if clip_to_aoi else None, convert_to_feet, feedback)
        
        feedback.pushInfo('\n' + '='*60)
        feedback.pushInfo('✓ Processing complete!')
        feedback.pushInfo(f'  Tiles: {len(results)} files in {tiles_folder}')
        feedback.pushInfo(f'  Mosaic: {mosaic_path}')
        feedback.pushInfo(f'  Manifest: {manifest_path}')
        feedback.pushInfo('  Mosaic will be loaded into map canvas')
        feedback.pushInfo('='*60)
        
        return {
            self.OUTPUT_MOSAIC: mosaic_path,
            'TILES_FOLDER': tiles_folder,
            'MANIFEST': manifest_path
        }
    
    def aoi_to_esri_polygon(self, layer, feedback):
        """Convert AOI layer to Esri JSON polygon in WGS84"""
        
        # Get union of all features
        features = list(layer.getFeatures())
        if not features:
            raise QgsProcessingException('No features in AOI layer')
        
        # Union all geometries
        geom = QgsGeometry()
        for feature in features:
            if geom.isEmpty():
                geom = feature.geometry()
            else:
                geom = geom.combine(feature.geometry())
        
        # Transform to WGS84
        source_crs = layer.crs()
        wgs84_crs = QgsCoordinateReferenceSystem('EPSG:4326')
        
        if source_crs != wgs84_crs:
            transform = QgsCoordinateTransform(source_crs, wgs84_crs, QgsProject.instance())
            geom.transform(transform)
        
        # Get bounds
        bbox = geom.boundingBox()
        feedback.pushInfo(f'AOI bounds (WGS84): {bbox.xMinimum():.6f}, {bbox.yMinimum():.6f}, {bbox.xMaximum():.6f}, {bbox.yMaximum():.6f}')
        
        # Convert to Esri JSON format
        rings = []
        
        if geom.isMultipart():
            multipolygon = geom.asMultiPolygon()
            for polygon in multipolygon:
                for ring in polygon:
                    rings.append([[pt.x(), pt.y()] for pt in ring])
        else:
            polygon = geom.asPolygon()
            for ring in polygon:
                rings.append([[pt.x(), pt.y()] for pt in ring])
        
        return {
            "rings": rings,
            "spatialReference": {"wkid": 4326}
        }
    
    def query_3dep_index(self, esri_geom, feedback):
        """Query USGS 3DEP Index for intersecting workunits"""
        
        base_params = {
            "f": "json",
            "where": "sourcedem_link IS NOT NULL",
            "geometryType": "esriGeometryPolygon",
            "inSR": "4326",
            "spatialRel": "esriSpatialRelIntersects",
            "outFields": ",".join(self.OUT_FIELDS),
            "returnGeometry": "false",
            "orderByFields": "workunit",
            "resultRecordCount": str(self.PAGE_SIZE),
        }
        
        items = []
        result_offset = 0
        
        while True:
            params = base_params.copy()
            params["resultOffset"] = str(result_offset)
            params["geometry"] = json.dumps(esri_geom)
            
            # Encode parameters as form data for POST body
            data = parse.urlencode(params).encode('utf-8')
            
            try:
                # Make POST request with data in body
                req = request.Request(self.INDEX_LAYER, data=data, method='POST')
                req.add_header('Content-Type', 'application/x-www-form-urlencoded')
                
                with request.urlopen(req, timeout=60) as response:
                    result = json.loads(response.read().decode())
                
                if "error" in result:
                    feedback.reportError(f"API Error: {result['error']}")
                    break
                
                feats = result.get("features", [])
                feedback.pushInfo(f"  Retrieved {len(feats)} features (offset {result_offset})")
                
                for ftr in feats:
                    attrs = ftr.get("attributes", {})
                    if attrs.get("sourcedem_link"):
                        items.append(attrs)
                
                if len(feats) < self.PAGE_SIZE:
                    break
                
                result_offset += self.PAGE_SIZE
                time.sleep(0.2)
                
            except Exception as e:
                feedback.reportError(f"Error querying API: {str(e)}")
                break
        
        return items
    
    def get_tile_urls_from_workunit(self, workunit_item, feedback):
        """Get list of tile URLs from workunit"""
        
        url = workunit_item['sourcedem_link']
        
        if 'prefix=' not in url:
            return []
        
        prefix = url.split('prefix=')[1]
        listing_url = f"http://prd-tnm.s3.amazonaws.com/{prefix}/0_file_download_links.txt"
        
        try:
            with request.urlopen(listing_url, timeout=30) as response:
                content = response.read().decode('utf-8')
            
            lines = content.strip().split('\n')
            tif_urls = [line.strip() for line in lines if line.strip().endswith('.tif')]
            return tif_urls
            
        except Exception as e:
            feedback.reportError(f"Could not fetch tile listing: {str(e)}")
            return []
    
    def parse_tile_coords(self, filename):
        """Extract UTM coordinates from tile filename"""
        match = re.search(r'w(\d{4})n(\d{4})', filename.lower())
        if match:
            easting = int(match.group(1)) * 1000
            northing = int(match.group(2)) * 1000
            return (easting, northing)
        return None
    
    def filter_tiles_by_aoi(self, tile_urls, aoi_layer, feedback):
        """Filter tiles to those intersecting AOI"""
        
        # Get AOI in UTM Zone 12N
        utm_crs = QgsCoordinateReferenceSystem('EPSG:6341')
        transform = QgsCoordinateTransform(aoi_layer.crs(), utm_crs, QgsProject.instance())
        
        # Get AOI geometry in UTM
        features = list(aoi_layer.getFeatures())
        aoi_geom = QgsGeometry()
        for feature in features:
            if aoi_geom.isEmpty():
                aoi_geom = feature.geometry()
            else:
                aoi_geom = aoi_geom.combine(feature.geometry())
        
        aoi_geom.transform(transform)
        aoi_bbox = aoi_geom.boundingBox()
        
        # Filter tiles
        intersecting = []
        for url in tile_urls:
            filename = url.split('/')[-1]
            coords = self.parse_tile_coords(filename)
            
            if coords:
                easting, northing = coords
                # Create tile bbox (tiles are ~1km x 1km)
                tile_bbox = QgsRectangle(easting, northing, easting + 1000, northing + 1000)
                
                # Check intersection
                if tile_bbox.intersects(aoi_bbox):
                    intersecting.append(url)
        
        return intersecting
    
    def download_tiles(self, tiles, output_folder, feedback):
        """Download all tiles"""
        
        results = []
        total = len(tiles)
        
        for i, tile in enumerate(tiles):
            if feedback.isCanceled():
                break
            
            # Update progress
            progress = int((i / total) * 100)
            feedback.setProgress(progress)
            
            url = tile['url']
            filename = url.split('/')[-1]
            output_path = os.path.join(output_folder, filename)
            
            feedback.pushInfo(f'[{i+1}/{total}] {filename}')
            
            # Check if already exists
            if os.path.exists(output_path) and os.path.getsize(output_path) > 1000000:
                feedback.pushInfo(f'  ✓ Already exists')
                status = 'exists'
            else:
                # Download
                try:
                    request.urlretrieve(url, output_path)
                    status = 'downloaded'
                    feedback.pushInfo(f'  ✓ Downloaded ({os.path.getsize(output_path)/1024/1024:.1f} MB)')
                except Exception as e:
                    feedback.reportError(f'  ✗ Error: {str(e)}')
                    status = 'error'
                    output_path = None
            
            results.append({
                **tile,
                'local_path': output_path,
                'status': status
            })
            
            time.sleep(0.1)
        
        return results
    
    def write_manifest(self, results, manifest_path, feedback):
        """Write manifest CSV"""
        
        with open(manifest_path, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['workunit', 'project', 'ql', 'dem_gsd_meters', 
                         'url', 'local_path', 'metadata_link', 'status']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        
        feedback.pushInfo(f'Manifest written: {manifest_path}')
    
    def create_mosaic(self, results, mosaic_path, aoi_layer, convert_to_feet, feedback):
        """Create mosaic from tiles with highest-resolution priority"""
        
        # Group tiles by GSD
        tiles_by_gsd = {}
        for result in results:
            if result['local_path'] and result['status'] != 'error':
                gsd = result['dem_gsd_meters']
                if gsd not in tiles_by_gsd:
                    tiles_by_gsd[gsd] = []
                tiles_by_gsd[gsd].append(result['local_path'])
        
        if not tiles_by_gsd:
            raise QgsProcessingException('No valid tiles to mosaic')
        
        # Sort by GSD (smallest first = highest resolution)
        sorted_gsds = sorted(tiles_by_gsd.keys())
        
        feedback.pushInfo(f'\nResolution priority order:')
        for gsd in sorted_gsds:
            feedback.pushInfo(f'  {gsd}m GSD: {len(tiles_by_gsd[gsd])} tiles')
        
        # Use GDAL to create mosaic
        # Process highest resolution first
        feedback.pushInfo(f'\nProcessing {sorted_gsds[0]}m resolution tiles...')
        tile_paths = tiles_by_gsd[sorted_gsds[0]]
        
        # Build VRT first (virtual mosaic)
        vrt_path = mosaic_path.replace('.tif', '_temp.vrt')
        
        feedback.pushInfo('Building VRT...')
        vrt_options = gdal.BuildVRTOptions(
            resolution='highest',
            resampleAlg='bilinear'
        )
        vrt_ds = gdal.BuildVRT(vrt_path, tile_paths, options=vrt_options)
        vrt_ds = None
        
        # Clip to AOI if requested
        if aoi_layer:
            feedback.pushInfo('Clipping to AOI bounds...')
            
            # Get AOI extent in tile CRS
            # Read CRS from first tile
            ds = gdal.Open(tile_paths[0])
            tile_crs_wkt = ds.GetProjection()
            ds = None
            
            # Transform AOI to tile CRS
            tile_crs = QgsCoordinateReferenceSystem()
            tile_crs.createFromWkt(tile_crs_wkt)
            
            transform = QgsCoordinateTransform(aoi_layer.crs(), tile_crs, QgsProject.instance())
            
            features = list(aoi_layer.getFeatures())
            aoi_geom = QgsGeometry()
            for feature in features:
                if aoi_geom.isEmpty():
                    aoi_geom = feature.geometry()
                else:
                    aoi_geom = aoi_geom.combine(feature.geometry())
            
            aoi_geom.transform(transform)
            bbox = aoi_geom.boundingBox()
            
            # Warp with clipping
            warp_options = gdal.WarpOptions(
                format='GTiff',
                outputBounds=[bbox.xMinimum(), bbox.yMinimum(), bbox.xMaximum(), bbox.yMaximum()],
                dstSRS=tile_crs_wkt,
                creationOptions=['COMPRESS=LZW', 'TILED=YES', 'BIGTIFF=IF_SAFER'],
                resampleAlg='bilinear'
            )
        else:
            # No clipping
            warp_options = gdal.WarpOptions(
                format='GTiff',
                creationOptions=['COMPRESS=LZW', 'TILED=YES', 'BIGTIFF=IF_SAFER'],
                resampleAlg='bilinear'
            )
        
        feedback.pushInfo('Creating final mosaic...')
        gdal.Warp(mosaic_path, vrt_path, options=warp_options)
        
        # Clean up VRT
        if os.path.exists(vrt_path):
            os.remove(vrt_path)
        
        # Convert to feet if requested
        if convert_to_feet:
            feedback.pushInfo('\nConverting elevations from meters to feet...')
            self.convert_dem_to_feet(mosaic_path, feedback)
        
        # Get mosaic info
        ds = gdal.Open(mosaic_path)
        if ds:
            feedback.pushInfo(f'\n✓ Mosaic created successfully!')
            feedback.pushInfo(f'  Dimensions: {ds.RasterXSize} x {ds.RasterYSize} pixels')
            feedback.pushInfo(f'  Resolution: {sorted_gsds[0]}m')
            
            # Get elevation stats
            band = ds.GetRasterBand(1)
            stats = band.GetStatistics(True, True)
            if stats:
                units = 'ft' if convert_to_feet else 'm'
                feedback.pushInfo(f'  Elevation range: {stats[0]:.2f} - {stats[1]:.2f} {units}')
                feedback.pushInfo(f'  Mean elevation: {stats[2]:.2f} {units}')
            
            file_size = os.path.getsize(mosaic_path) / (1024*1024)
            feedback.pushInfo(f'  File size: {file_size:.1f} MB')
            
            ds = None
    
    def convert_dem_to_feet(self, raster_path, feedback):
        """Convert DEM elevations from meters to feet (multiply by 3.28084)"""
        
        METERS_TO_FEET = 3.28084
        
        # Open the raster
        ds = gdal.Open(raster_path, gdal.GA_Update)
        if not ds:
            feedback.reportError(f'Could not open {raster_path} for conversion')
            return
        
        band = ds.GetRasterBand(1)
        
        # Get raster dimensions
        xsize = ds.RasterXSize
        ysize = ds.RasterYSize
        
        # Process in chunks to handle large rasters
        chunk_size = 1024
        
        for y in range(0, ysize, chunk_size):
            # Calculate chunk height
            if y + chunk_size < ysize:
                rows = chunk_size
            else:
                rows = ysize - y
            
            for x in range(0, xsize, chunk_size):
                # Calculate chunk width
                if x + chunk_size < xsize:
                    cols = chunk_size
                else:
                    cols = xsize - x
                
                # Read chunk
                data = band.ReadAsArray(x, y, cols, rows)
                
                if data is not None:
                    # Convert meters to feet
                    data = data * METERS_TO_FEET
                    
                    # Write back
                    band.WriteArray(data, x, y)
        
        # Flush cache and close
        band.FlushCache()
        ds = None
        
        feedback.pushInfo(f'  ✓ Converted to feet (multiplied by {METERS_TO_FEET})')