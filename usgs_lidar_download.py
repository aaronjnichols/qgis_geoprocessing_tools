"""
QGIS Processing Tool: Download USGS 3DEP OPR DEMs
Author: Claude
Version: 1.4 - Simplified UI, fixed contour generation
QGIS Version: 3.44+

This tool downloads USGS 3DEP OPR DEM tiles that intersect an AOI polygon
and creates a seamless mosaic with highest-resolution priority.

No external dependencies required - uses only built-in QGIS/GDAL libraries.
"""

from qgis.PyQt.QtCore import QCoreApplication, QVariant
from qgis.core import (
    QgsProcessing,
    QgsProcessingAlgorithm,
    QgsProcessingContext,
    QgsProcessingParameterVectorLayer,
    QgsProcessingParameterFolderDestination,
    QgsProcessingParameterBoolean,
    QgsProcessingParameterNumber,
    QgsProcessingParameterEnum,
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
import subprocess
from urllib import request, error, parse
from osgeo import gdal, osr, ogr

class DownloadOprDemsAlgorithm(QgsProcessingAlgorithm):
    """
    QGIS Processing Algorithm for downloading USGS 3DEP OPR DEMs
    """

    # Define parameter names as constants
    INPUT_AOI = 'INPUT_AOI'
    OUTPUT_FOLDER = 'OUTPUT_FOLDER'
    CLIP_TO_AOI = 'CLIP_TO_AOI'
    CONVERT_TO_FEET = 'CONVERT_TO_FEET'
    GENERATE_CONTOURS = 'GENERATE_CONTOURS'
    CONTOUR_INTERVAL = 'CONTOUR_INTERVAL'
    CONTOUR_FORMAT = 'CONTOUR_FORMAT'

    # Contour format options
    CONTOUR_FORMATS = ['DXF', 'Shapefile', 'GeoPackage']

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
        - Automatic spatial filtering - downloads only tiles intersecting your AOI
        - Intelligent highest-resolution priority when multiple resolutions available
        - Creates seamless mosaic with optional clipping to AOI bounds
        - Optional conversion from meters to feet (USGS DEMs are natively in meters)
        - Optional contour generation with choice of output format (DXF, Shapefile, GeoPackage)

        <b>Inputs:</b>
        - <b>AOI Layer</b>: Polygon layer defining area of interest (any CRS)
        - <b>Output Folder</b>: Directory for all outputs (DEM.tif, contours, tiles folder)
        - <b>Clip to AOI</b>: Whether to clip mosaic to AOI bounds
        - <b>Convert to Feet</b>: Convert elevations from meters to feet
        - <b>Generate Contours</b>: Create contour lines from DEM
        - <b>Contour Interval</b>: Vertical spacing between contours (default 1 foot)
        - <b>Contour Format</b>: Output format - DXF (3D for CAD), Shapefile, or GeoPackage

        <b>Outputs (all saved to Output Folder):</b>
        - DEM.tif - The mosaic DEM raster
        - tiles/ - Individual downloaded DEM tiles
        - tiles/manifest.csv - Download manifest
        - contours.dxf/shp/gpkg - Contour lines (if enabled)

        <b>Data Source:</b>
        USGS 3D Elevation Program (3DEP) via National Map
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

        # Output folder - all outputs go here
        self.addParameter(
            QgsProcessingParameterFolderDestination(
                self.OUTPUT_FOLDER,
                self.tr('Output Folder')
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
                self.CONVERT_TO_FEET,
                self.tr('Convert elevations from meters to feet'),
                defaultValue=True
            )
        )

        # Contour generation option
        self.addParameter(
            QgsProcessingParameterBoolean(
                self.GENERATE_CONTOURS,
                self.tr('Generate contours from DEM'),
                defaultValue=False
            )
        )

        # Contour interval - default 1 foot
        self.addParameter(
            QgsProcessingParameterNumber(
                self.CONTOUR_INTERVAL,
                self.tr('Contour interval'),
                type=QgsProcessingParameterNumber.Double,
                defaultValue=1.0,
                minValue=0.1
            )
        )

        # Contour format selection
        self.addParameter(
            QgsProcessingParameterEnum(
                self.CONTOUR_FORMAT,
                self.tr('Contour output format'),
                options=self.CONTOUR_FORMATS,
                defaultValue=0  # DXF
            )
        )

    def processAlgorithm(self, parameters, context, feedback):
        """Main processing logic"""

        # Get parameters
        aoi_layer = self.parameterAsVectorLayer(parameters, self.INPUT_AOI, context)
        output_folder = self.parameterAsString(parameters, self.OUTPUT_FOLDER, context)
        clip_to_aoi = self.parameterAsBoolean(parameters, self.CLIP_TO_AOI, context)
        convert_to_feet = self.parameterAsBoolean(parameters, self.CONVERT_TO_FEET, context)
        generate_contours = self.parameterAsBoolean(parameters, self.GENERATE_CONTOURS, context)
        contour_interval = self.parameterAsDouble(parameters, self.CONTOUR_INTERVAL, context)
        contour_format_idx = self.parameterAsEnum(parameters, self.CONTOUR_FORMAT, context)
        contour_format = self.CONTOUR_FORMATS[contour_format_idx]

        # Validate contour parameters
        if generate_contours and contour_interval <= 0:
            raise QgsProcessingException('Contour interval must be greater than zero')

        # Set up output paths - everything goes in output_folder
        mosaic_path = os.path.join(output_folder, 'DEM.tif')
        tiles_folder = os.path.join(output_folder, 'tiles')
        manifest_path = os.path.join(tiles_folder, 'manifest.csv')

        # Determine contour output path based on format
        if contour_format == 'DXF':
            contour_path = os.path.join(output_folder, 'contours.dxf')
        elif contour_format == 'Shapefile':
            contour_path = os.path.join(output_folder, 'contours.shp')
        else:  # GeoPackage
            contour_path = os.path.join(output_folder, 'contours.gpkg')

        # Validate inputs
        if not aoi_layer:
            raise QgsProcessingException('Invalid AOI layer')

        if not aoi_layer.featureCount():
            raise QgsProcessingException('AOI layer has no features')

        # Create output directories
        os.makedirs(output_folder, exist_ok=True)
        os.makedirs(tiles_folder, exist_ok=True)

        feedback.pushInfo(f'Output folder: {output_folder}')
        feedback.pushInfo(f'DEM output: {mosaic_path}')
        if generate_contours:
            feedback.pushInfo(f'Contour output: {contour_path}')

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
        self.create_mosaic(results, mosaic_path, aoi_layer if clip_to_aoi else None, convert_to_feet, aoi_layer.crs(), feedback)

        # Step 7: Generate contours (optional)
        final_contour_path = None

        if generate_contours:
            feedback.pushInfo('\n=== Step 7: Generating contours ===')

            units = 'ft' if convert_to_feet else 'm'
            feedback.pushInfo(f'Contour interval: {contour_interval} {units}')
            feedback.pushInfo(f'Output format: {contour_format}')

            final_contour_path = self.generate_contours(
                mosaic_path,
                contour_path,
                contour_interval,
                contour_format,
                aoi_layer.crs(),  # Pass AOI's CRS for reprojection
                feedback
            )

        feedback.pushInfo('\n' + '='*60)
        feedback.pushInfo('Processing complete!')
        feedback.pushInfo(f'  DEM: {mosaic_path}')
        feedback.pushInfo(f'  Tiles: {tiles_folder}')
        if final_contour_path:
            feedback.pushInfo(f'  Contours: {final_contour_path}')
        feedback.pushInfo('='*60)

        # Auto-load DEM into QGIS
        feedback.pushInfo('\nLoading outputs into QGIS...')
        context.addLayerToLoadOnCompletion(
            mosaic_path,
            QgsProcessingContext.LayerDetails('DEM', QgsProject.instance(), 'DEM')
        )

        # Auto-load contours if they were created
        if final_contour_path and os.path.exists(final_contour_path):
            context.addLayerToLoadOnCompletion(
                final_contour_path,
                QgsProcessingContext.LayerDetails('Contours', QgsProject.instance(), 'Contours')
            )

        return {
            'OUTPUT_DEM': mosaic_path,
            'TILES_FOLDER': tiles_folder,
            'MANIFEST': manifest_path,
            'OUTPUT_CONTOURS': final_contour_path
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
                feedback.pushInfo(f'  Already exists')
                status = 'exists'
            else:
                # Download
                try:
                    request.urlretrieve(url, output_path)
                    status = 'downloaded'
                    feedback.pushInfo(f'  Downloaded ({os.path.getsize(output_path)/1024/1024:.1f} MB)')
                except Exception as e:
                    feedback.reportError(f'  Error: {str(e)}')
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

    def create_mosaic(self, results, mosaic_path, aoi_layer, convert_to_feet, target_crs, feedback):
        """Create mosaic from tiles with highest-resolution priority, reprojected to target CRS"""

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

        # Get target CRS as WKT for GDAL
        target_crs_wkt = target_crs.toWkt()

        # Clip to AOI if requested
        if aoi_layer:
            feedback.pushInfo('Clipping to AOI bounds...')

            # Get AOI bounds in target CRS (which is the AOI's native CRS)
            features = list(aoi_layer.getFeatures())
            aoi_geom = QgsGeometry()
            for feature in features:
                if aoi_geom.isEmpty():
                    aoi_geom = feature.geometry()
                else:
                    aoi_geom = aoi_geom.combine(feature.geometry())

            bbox = aoi_geom.boundingBox()

            feedback.pushInfo(f'Reprojecting to {target_crs.authid()}...')

            # Warp with clipping and reprojection to target CRS
            warp_options = gdal.WarpOptions(
                format='GTiff',
                outputBounds=[bbox.xMinimum(), bbox.yMinimum(), bbox.xMaximum(), bbox.yMaximum()],
                dstSRS=target_crs_wkt,
                creationOptions=['COMPRESS=LZW', 'TILED=YES', 'BIGTIFF=IF_SAFER'],
                resampleAlg='bilinear'
            )
        else:
            feedback.pushInfo(f'Reprojecting to {target_crs.authid()}...')
            # No clipping, but still reproject
            warp_options = gdal.WarpOptions(
                format='GTiff',
                dstSRS=target_crs_wkt,
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
            feedback.pushInfo(f'\nMosaic created successfully!')
            feedback.pushInfo(f'  Dimensions: {ds.RasterXSize} x {ds.RasterYSize} pixels')
            feedback.pushInfo(f'  Resolution: {sorted_gsds[0]}m')
            feedback.pushInfo(f'  CRS: {target_crs.authid()}')

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

    def generate_contours(self, dem_path, output_path, interval, output_format, target_crs, feedback):
        """
        Generate contours from DEM mosaic

        Args:
            dem_path: Path to input DEM raster
            output_path: Path for output contours
            interval: Contour interval in DEM units
            output_format: 'DXF', 'Shapefile', or 'GeoPackage'
            target_crs: Target CRS to reproject contours to (from AOI)
            feedback: Processing feedback object

        Returns:
            Path to generated contours or None on error
        """

        feedback.pushInfo(f'Generating contours with {interval} interval...')

        try:
            is_dxf = output_format == 'DXF'

            # Always generate to temp shapefile first, then reproject and convert
            temp_shp = output_path.replace('.dxf', '_temp.shp').replace('.gpkg', '_temp.shp').replace('.shp', '_temp.shp')

            # Run GDAL contour algorithm
            feedback.pushInfo('  Running GDAL contour...')
            result = processing.run("gdal:contour", {
                'INPUT': dem_path,
                'BAND': 1,
                'INTERVAL': interval,
                'OFFSET': 0.0,
                'FIELD_NAME': 'ELEV',
                'CREATE_3D': True,  # Always create 3D for proper Z values
                'IGNORE_NODATA': True,
                'NODATA': None,
                'OUTPUT': temp_shp
            }, feedback=feedback, is_child_algorithm=True)

            # Check if contours were generated
            if not os.path.exists(temp_shp):
                feedback.reportError('Contour generation failed - no output file created')
                return None

            # Get statistics from the generated contours
            contour_layer = QgsVectorLayer(temp_shp, 'contours', 'ogr')
            if contour_layer.isValid():
                count = contour_layer.featureCount()
                feedback.pushInfo(f'  Generated {count} contour lines')

                if count == 0:
                    feedback.reportError('No contours generated - check if DEM has elevation variation')
                    # Clean up temp files
                    del contour_layer
                    self.cleanup_shapefile(temp_shp)
                    return None

                # Get elevation range
                elevations = []
                for feat in contour_layer.getFeatures():
                    elev = feat['ELEV']
                    if elev is not None:
                        elevations.append(elev)

                if elevations:
                    min_elev = min(elevations)
                    max_elev = max(elevations)
                    feedback.pushInfo(f'  Elevation range: {min_elev:.1f} - {max_elev:.1f}')

            # Release the layer before reprojecting
            del contour_layer

            # Reproject to target CRS (AOI's coordinate system)
            feedback.pushInfo(f'  Reprojecting to {target_crs.authid()}...')
            reprojected_shp = temp_shp.replace('_temp.shp', '_reprojected.shp')

            processing.run("native:reprojectlayer", {
                'INPUT': temp_shp,
                'TARGET_CRS': target_crs,
                'OUTPUT': reprojected_shp
            }, feedback=feedback, is_child_algorithm=True)

            # Clean up original temp shapefile
            self.cleanup_shapefile(temp_shp)

            # Now convert/save to final format
            if is_dxf:
                feedback.pushInfo('  Converting to DXF format with 3D polylines...')
                success = self.convert_to_dxf_ogr2ogr(reprojected_shp, output_path, feedback)

                # Clean up reprojected shapefile
                self.cleanup_shapefile(reprojected_shp)

                if not success:
                    feedback.reportError('DXF conversion failed')
                    return None
            else:
                # For shapefile or geopackage, just rename/move the reprojected file
                if output_format == 'Shapefile':
                    # Move reprojected shapefile files to final location
                    base_src = reprojected_shp.replace('.shp', '')
                    base_dst = output_path.replace('.shp', '')
                    for ext in ['.shp', '.shx', '.dbf', '.prj', '.cpg']:
                        src = base_src + ext
                        dst = base_dst + ext
                        if os.path.exists(src):
                            if os.path.exists(dst):
                                os.remove(dst)
                            os.rename(src, dst)
                else:
                    # GeoPackage - convert from shapefile
                    processing.run("native:reprojectlayer", {
                        'INPUT': reprojected_shp,
                        'TARGET_CRS': target_crs,
                        'OUTPUT': output_path
                    }, feedback=feedback, is_child_algorithm=True)
                    # Clean up reprojected shapefile
                    self.cleanup_shapefile(reprojected_shp)

            # Verify output exists
            if os.path.exists(output_path):
                file_size = os.path.getsize(output_path) / (1024*1024)
                feedback.pushInfo(f'  Output file: {output_path}')
                feedback.pushInfo(f'  File size: {file_size:.2f} MB')
                return output_path
            else:
                feedback.reportError(f'Output file not created: {output_path}')
                return None

        except Exception as e:
            feedback.reportError(f'Error generating contours: {str(e)}')
            import traceback
            feedback.reportError(traceback.format_exc())
            # Try to clean up temp files on error
            try:
                self.cleanup_shapefile(temp_shp)
                self.cleanup_shapefile(temp_shp.replace('_temp.shp', '_reprojected.shp'))
            except:
                pass
            return None

    def convert_to_dxf_ogr2ogr(self, input_shp, output_dxf, feedback):
        """
        Convert shapefile to DXF using ogr2ogr command line for reliable 3D export

        Args:
            input_shp: Path to input shapefile with Z coordinates
            output_dxf: Path for output DXF file
            feedback: Processing feedback object

        Returns:
            True if successful, False otherwise
        """
        try:
            # Remove existing DXF if present
            if os.path.exists(output_dxf):
                os.remove(output_dxf)

            # Find ogr2ogr executable - check QGIS bin directory first
            ogr2ogr_exe = None

            # Try to find via QGIS paths
            qgis_prefix = os.environ.get('QGIS_PREFIX_PATH', '')
            if qgis_prefix:
                potential_path = os.path.join(qgis_prefix, 'bin', 'ogr2ogr.exe')
                if os.path.exists(potential_path):
                    ogr2ogr_exe = potential_path

            # Try common QGIS installation paths
            if not ogr2ogr_exe:
                common_paths = [
                    r'C:\Program Files\QGIS 3.40.1\bin\ogr2ogr.exe',
                    r'C:\Program Files\QGIS 3.34.14\bin\ogr2ogr.exe',
                    r'C:\Program Files\QGIS 3.28\bin\ogr2ogr.exe',
                    r'C:\OSGeo4W64\bin\ogr2ogr.exe',
                    r'C:\OSGeo4W\bin\ogr2ogr.exe',
                ]
                for path in common_paths:
                    if os.path.exists(path):
                        ogr2ogr_exe = path
                        break

            if not ogr2ogr_exe:
                feedback.pushInfo('  ogr2ogr not found, using OGR Python API...')
                return self.convert_to_dxf_python(input_shp, output_dxf, feedback)

            # Build ogr2ogr command
            # -f DXF: output format
            # -zfield ELEV: use ELEV field for Z values (creates proper 3D)
            cmd = [
                ogr2ogr_exe,
                '-f', 'DXF',
                output_dxf,
                input_shp,
                '-zfield', 'ELEV'
            ]

            feedback.pushInfo(f'  Running: {" ".join(cmd)}')

            # Run ogr2ogr
            result = subprocess.run(cmd, capture_output=True, text=True, shell=True)

            if result.returncode != 0:
                feedback.pushInfo(f'  ogr2ogr warning/error: {result.stderr}')
                # Check if file was created despite error
                if not os.path.exists(output_dxf):
                    feedback.pushInfo('  Falling back to Python API...')
                    return self.convert_to_dxf_python(input_shp, output_dxf, feedback)

            if os.path.exists(output_dxf):
                feedback.pushInfo('  DXF created successfully via ogr2ogr')
                return True
            else:
                feedback.pushInfo('  DXF not created, falling back to Python API...')
                return self.convert_to_dxf_python(input_shp, output_dxf, feedback)

        except Exception as e:
            feedback.pushInfo(f'  ogr2ogr error: {str(e)}, falling back to Python API...')
            return self.convert_to_dxf_python(input_shp, output_dxf, feedback)

    def convert_to_dxf_python(self, input_shp, output_dxf, feedback):
        """
        Convert shapefile to DXF using OGR Python API

        Args:
            input_shp: Path to input shapefile with Z coordinates
            output_dxf: Path for output DXF file
            feedback: Processing feedback object

        Returns:
            True if successful, False otherwise
        """
        try:
            # Open source shapefile
            src_ds = ogr.Open(input_shp)
            if src_ds is None:
                feedback.reportError(f'Could not open {input_shp}')
                return False

            src_layer = src_ds.GetLayer()

            # Get the DXF driver
            drv = ogr.GetDriverByName('DXF')
            if drv is None:
                feedback.reportError('DXF driver not available in OGR')
                src_ds = None
                return False

            # Remove existing DXF if present
            if os.path.exists(output_dxf):
                os.remove(output_dxf)

            # Create output DXF
            dst_ds = drv.CreateDataSource(output_dxf)
            if dst_ds is None:
                feedback.reportError(f'Could not create {output_dxf}')
                src_ds = None
                return False

            # Create layer with 3D line string type
            dst_layer = dst_ds.CreateLayer('contours', geom_type=ogr.wkbLineString25D)

            # Get the ELEV field index
            layer_defn = src_layer.GetLayerDefn()
            elev_idx = layer_defn.GetFieldIndex('ELEV')

            # Copy features with proper Z values
            feat_count = 0
            src_layer.ResetReading()

            for src_feat in src_layer:
                geom = src_feat.GetGeometryRef()
                if geom is None:
                    continue

                # Get elevation from ELEV field
                elev = src_feat.GetField(elev_idx) if elev_idx >= 0 else 0.0
                if elev is None:
                    elev = 0.0

                # Create new geometry with proper Z values
                geom_type = geom.GetGeometryType()

                if geom_type == ogr.wkbLineString or geom_type == ogr.wkbLineString25D:
                    new_geom = self.add_z_to_linestring(geom, elev)
                elif geom_type == ogr.wkbMultiLineString or geom_type == ogr.wkbMultiLineString25D:
                    new_geom = ogr.Geometry(ogr.wkbMultiLineString25D)
                    for i in range(geom.GetGeometryCount()):
                        line = geom.GetGeometryRef(i)
                        new_line = self.add_z_to_linestring(line, elev)
                        new_geom.AddGeometry(new_line)
                else:
                    new_geom = geom.Clone()

                # Create new feature
                dst_feat = ogr.Feature(dst_layer.GetLayerDefn())
                dst_feat.SetGeometry(new_geom)
                dst_layer.CreateFeature(dst_feat)
                dst_feat = None
                feat_count += 1

            # Cleanup
            src_ds = None
            dst_ds = None

            feedback.pushInfo(f'  Wrote {feat_count} features to DXF via Python API')
            return True

        except Exception as e:
            feedback.reportError(f'DXF Python conversion error: {str(e)}')
            import traceback
            feedback.reportError(traceback.format_exc())
            return False

    def add_z_to_linestring(self, geom, z_value):
        """Add Z value to all points in a linestring"""
        new_geom = ogr.Geometry(ogr.wkbLineString25D)
        for i in range(geom.GetPointCount()):
            x = geom.GetX(i)
            y = geom.GetY(i)
            new_geom.AddPoint(x, y, z_value)
        return new_geom

    def cleanup_shapefile(self, shp_path):
        """Remove shapefile and associated files"""
        import gc
        gc.collect()  # Force garbage collection to release file handles
        time.sleep(0.1)  # Small delay to allow file handles to be released

        base = shp_path.replace('.shp', '')
        # Include all possible shapefile sidecar files
        extensions = ['.shp', '.shx', '.dbf', '.prj', '.cpg', '.qix', '.sbn', '.sbx', '.fbn', '.fbx', '.ain', '.aih', '.atx', '.ixs', '.mxs', '.xml']
        for ext in extensions:
            f = base + ext
            if os.path.exists(f):
                try:
                    os.remove(f)
                except Exception:
                    # Try again after a brief pause
                    time.sleep(0.2)
                    try:
                        os.remove(f)
                    except Exception:
                        pass  # Give up silently

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

        feedback.pushInfo(f'  Converted to feet (multiplied by {METERS_TO_FEET})')
