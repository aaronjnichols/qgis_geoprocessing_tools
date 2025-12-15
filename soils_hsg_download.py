"""
***************************************************************************
    SSURGO Soil Data Downloader - Version 1.3
    
    Downloads SSURGO soils data for an AOI and maps hydrologic soil groups
    
    Version History:
    - v1.3 (Nov 2024): Fixed handling of None values in HSG (Water/Pits features)
    - v1.2 (Nov 2024): Fixed layer loading from processing.run() outputs
    - v1.1 (Nov 2024): Added geometry validation and alternative clipping methods
    - v1.0 (Nov 2024): Initial release
    
    ---------------------
    Date                 : November 2024
    Copyright            : (C) 2024
***************************************************************************
"""

from qgis.PyQt.QtCore import QCoreApplication
from qgis.core import (QgsProcessing,
                       QgsProcessingAlgorithm,
                       QgsProcessingParameterVectorLayer,
                       QgsProcessingParameterFeatureSink,
                       QgsProcessingParameterFileDestination,
                       QgsProcessingParameterBoolean,
                       QgsProcessingException,
                       QgsProcessingUtils,
                       QgsVectorLayer,
                       QgsFeature,
                       QgsGeometry,
                       QgsField,
                       QgsFields,
                       QgsWkbTypes,
                       QgsCoordinateReferenceSystem,
                       QgsCoordinateTransform,
                       QgsProject)
from qgis.PyQt.QtCore import QVariant
import processing
import requests
import json
import tempfile
import os
from xml.etree import ElementTree as ET


class SSURGODownloaderAlgorithm(QgsProcessingAlgorithm):
    """
    Downloads SSURGO soil data and hydrologic soil groups for an area of interest
    """

    # Constants used to refer to parameters
    INPUT_AOI = 'INPUT_AOI'
    OUTPUT_SOILS = 'OUTPUT_SOILS'
    OUTPUT_MAP = 'OUTPUT_MAP'
    ADD_TO_CANVAS = 'ADD_TO_CANVAS'

    def tr(self, string):
        """
        Returns a translatable string with the self.tr() function.
        """
        return QCoreApplication.translate('Processing', string)

    def createInstance(self):
        return SSURGODownloaderAlgorithm()

    def name(self):
        """
        Returns the algorithm name, used for identifying the algorithm.
        """
        return 'ssurgo_downloader'

    def displayName(self):
        """
        Returns the translated algorithm name.
        """
        return self.tr('Download SSURGO Soils Data')

    def group(self):
        """
        Returns the name of the group this algorithm belongs to.
        """
        return self.tr('Data Download Toolbox')

    def groupId(self):
        """
        Returns the unique ID of the group this algorithm belongs to.
        """
        return 'data_download_toolbox'

    def shortHelpString(self):
        """
        Returns a localised short helper string for the algorithm.
        """
        return self.tr("""
        Downloads SSURGO (Soil Survey Geographic) soil data from USDA-NRCS 
        for the specified area of interest and retrieves hydrologic soil 
        group classifications.
        
        <b>Parameters:</b>
        
        <b>Input AOI:</b> Vector layer defining the area of interest. 
        Will be automatically reprojected to WGS84 if needed.
        
        <b>Output Soils Layer:</b> Output vector layer containing soil 
        polygons with attributes including:
        - mukey: Map unit key
        - musym: Map unit symbol
        - muname: Map unit name
        - compname: Component name (dominant soil)
        - comppct_r: Component percentage
        - hydgrp: Hydrologic soil group (A, B, C, D, or combinations)
        
        <b>Output Map:</b> PNG file showing soils colored by hydrologic 
        soil group classification.
        
        <b>Hydrologic Soil Groups:</b>
        - A: Low runoff potential, high infiltration
        - B: Moderate infiltration rate
        - C: Slow infiltration rate  
        - D: High runoff potential, very slow infiltration
        
        <b>Data Source:</b> USDA-NRCS Soil Data Access (SDA)
        """)

    def initAlgorithm(self, config=None):
        """
        Define the inputs and outputs of the algorithm.
        """
        
        # Input AOI
        self.addParameter(
            QgsProcessingParameterVectorLayer(
                self.INPUT_AOI,
                self.tr('Input Area of Interest (AOI)'),
                [QgsProcessing.TypeVectorPolygon]
            )
        )
        
        # Output soils layer
        self.addParameter(
            QgsProcessingParameterFeatureSink(
                self.OUTPUT_SOILS,
                self.tr('Output Soils Layer'),
                QgsProcessing.TypeVectorPolygon
            )
        )
        
        # Output map image
        self.addParameter(
            QgsProcessingParameterFileDestination(
                self.OUTPUT_MAP,
                self.tr('Output Map Image'),
                self.tr('PNG files (*.png)'),
                optional=True
            )
        )
        
        # Add to canvas option
        self.addParameter(
            QgsProcessingParameterBoolean(
                self.ADD_TO_CANVAS,
                self.tr('Add result to map canvas'),
                defaultValue=True
            )
        )

    def processAlgorithm(self, parameters, context, feedback):
        """
        Main processing method.
        """
        
        # Get input layer
        aoi_layer = self.parameterAsVectorLayer(
            parameters,
            self.INPUT_AOI,
            context
        )
        
        if aoi_layer is None:
            raise QgsProcessingException(self.invalidSourceError(parameters, self.INPUT_AOI))
        
        feedback.pushInfo(f'Input AOI: {aoi_layer.name()}')
        feedback.pushInfo(f'Feature count: {aoi_layer.featureCount()}')
        feedback.pushInfo(f'CRS: {aoi_layer.crs().authid()}')
        
        # Check if we need to reproject to WGS84
        wgs84_crs = QgsCoordinateReferenceSystem('EPSG:4326')
        
        if aoi_layer.crs() != wgs84_crs:
            feedback.pushInfo('Reprojecting AOI to WGS84...')
            # Reproject the layer
            reprojected = processing.run(
                "native:reprojectlayer",
                {
                    'INPUT': aoi_layer,
                    'TARGET_CRS': wgs84_crs,
                    'OUTPUT': 'memory:'
                },
                context=context,
                feedback=feedback,
                is_child_algorithm=True
            )
            
            # Load the reprojected layer properly
            aoi_layer = QgsProcessingUtils.mapLayerFromString(reprojected['OUTPUT'], context)
        
        # Get bounding box
        extent = aoi_layer.extent()
        minx, miny, maxx, maxy = extent.xMinimum(), extent.yMinimum(), extent.xMaximum(), extent.yMaximum()
        
        feedback.pushInfo(f'Bounding box: [{minx:.6f}, {miny:.6f}, {maxx:.6f}, {maxy:.6f}]')
        
        # Step 1: Download spatial data from WFS
        feedback.pushInfo('\n' + '='*50)
        feedback.pushInfo('DOWNLOADING SPATIAL DATA FROM USDA-NRCS')
        feedback.pushInfo('='*50)
        
        wfs_url = "https://sdmdataaccess.nrcs.usda.gov/Spatial/SDMWGS84Geographic.wfs"
        
        params = {
            'SERVICE': 'WFS',
            'VERSION': '1.0.0',
            'REQUEST': 'GetFeature',
            'TYPENAME': 'MapUnitPoly',
            'BBOX': f'{minx},{miny},{maxx},{maxy}',
            'outputFormat': 'GML2'
        }
        
        feedback.pushInfo(f'WFS URL: {wfs_url}')
        feedback.pushInfo('Requesting soil polygons...')
        
        try:
            response = requests.get(wfs_url, params=params, timeout=180)
            
            if response.status_code != 200:
                raise QgsProcessingException(f'WFS request failed with status {response.status_code}')
            
            # Save GML to temporary file
            temp_gml = tempfile.NamedTemporaryFile(delete=False, suffix='.gml', mode='wb')
            temp_gml.write(response.content)
            temp_gml.close()
            
            feedback.pushInfo(f'Downloaded GML data ({len(response.content)} bytes)')
            
            # Load GML as vector layer
            gml_layer = QgsVectorLayer(temp_gml.name, 'ssurgo_temp', 'ogr')
            
            if not gml_layer.isValid():
                raise QgsProcessingException('Failed to parse GML response')
            
            feedback.pushInfo(f'Parsed {gml_layer.featureCount()} soil polygons')
            
            # Fix invalid geometries first
            feedback.pushInfo('Fixing invalid geometries...')
            try:
                fixed = processing.run(
                    "native:fixgeometries",
                    {
                        'INPUT': gml_layer,
                        'METHOD': 1,  # Structure method
                        'OUTPUT': 'memory:'
                    },
                    context=context,
                    feedback=feedback,
                    is_child_algorithm=True
                )
                
                # Load the fixed layer properly
                gml_layer = QgsProcessingUtils.mapLayerFromString(fixed['OUTPUT'], context)
                feedback.pushInfo(f'Geometries validated and fixed')
                
            except Exception as geom_error:
                feedback.pushWarning(f'Geometry fixing had issues: {str(geom_error)}')
                feedback.pushInfo('Continuing with original geometries...')
            
            # Clip to AOI using intersection (more robust than clip)
            feedback.pushInfo('Clipping to AOI boundary...')
            
            try:
                # First try intersection
                clipped = processing.run(
                    "native:intersection",
                    {
                        'INPUT': gml_layer,
                        'OVERLAY': aoi_layer,
                        'INPUT_FIELDS': [],  # Keep all fields from input
                        'OVERLAY_FIELDS': [],  # Don't keep overlay fields
                        'OVERLAY_FIELDS_PREFIX': '',
                        'OUTPUT': 'memory:'
                    },
                    context=context,
                    feedback=feedback,
                    is_child_algorithm=True
                )
                
                # Load the clipped layer properly
                soils_layer = QgsProcessingUtils.mapLayerFromString(clipped['OUTPUT'], context)
                
            except Exception as clip_error:
                feedback.pushWarning(f'Intersection failed: {str(clip_error)}')
                feedback.pushInfo('Trying alternative method: extract by location...')
                
                # Alternative: use extract by location (less precise but more robust)
                clipped = processing.run(
                    "native:extractbylocation",
                    {
                        'INPUT': gml_layer,
                        'PREDICATE': [0],  # intersect
                        'INTERSECT': aoi_layer,
                        'OUTPUT': 'memory:'
                    },
                    context=context,
                    feedback=feedback,
                    is_child_algorithm=True
                )
                
                # Load the layer properly
                soils_layer = QgsProcessingUtils.mapLayerFromString(clipped['OUTPUT'], context)
            
            feedback.pushInfo(f'Clipped to {soils_layer.featureCount()} polygons')
            
            # Get unique mukeys
            mukeys = []
            for feature in soils_layer.getFeatures():
                mukey = feature['mukey']
                if mukey not in mukeys:
                    mukeys.append(mukey)
            
            feedback.pushInfo(f'Unique map units: {len(mukeys)}')
            feedback.pushInfo(f'Map unit keys: {mukeys}')
            
        except Exception as e:
            raise QgsProcessingException(f'Error downloading spatial data: {str(e)}')
        
        # Step 2: Download tabular data from SDA
        feedback.pushInfo('\n' + '='*50)
        feedback.pushInfo('DOWNLOADING SOIL ATTRIBUTES')
        feedback.pushInfo('='*50)
        
        mukey_str = ','.join([f"'{m}'" for m in mukeys])
        
        sql = f"""
        SELECT 
            mu.mukey,
            mu.musym,
            mu.muname,
            c.cokey,
            c.compname,
            c.comppct_r,
            c.hydgrp
        FROM mapunit mu
        INNER JOIN component c ON mu.mukey = c.mukey
        WHERE mu.mukey IN ({mukey_str})
        AND c.majcompflag = 'Yes'
        ORDER BY mu.mukey, c.comppct_r DESC
        """
        
        sda_url = "https://sdmdataaccess.nrcs.usda.gov/Tabular/post.rest"
        
        try:
            response = requests.post(sda_url, data={'query': sql, 'format': 'JSON'}, timeout=60)
            
            if response.status_code != 200:
                raise QgsProcessingException(f'SDA request failed with status {response.status_code}')
            
            result = response.json()
            
            if 'Table' not in result or len(result['Table']) == 0:
                raise QgsProcessingException('No soil component data returned from SDA')
            
            # Parse the results
            col_names = ['mukey', 'musym', 'muname', 'cokey', 'compname', 'comppct_r', 'hydgrp']
            components = []
            
            for row in result['Table']:
                component = dict(zip(col_names, row))
                component['mukey'] = str(component['mukey'])
                component['comppct_r'] = float(component['comppct_r'])
                components.append(component)
            
            feedback.pushInfo(f'Retrieved {len(components)} soil components')
            
            # Get dominant component per map unit
            dominant_components = {}
            for comp in components:
                mukey = comp['mukey']
                if mukey not in dominant_components or comp['comppct_r'] > dominant_components[mukey]['comppct_r']:
                    dominant_components[mukey] = comp
            
            feedback.pushInfo(f'Identified {len(dominant_components)} dominant components')
            
            # Print component summary
            for mukey, comp in dominant_components.items():
                feedback.pushInfo(f"  - {comp['compname']} ({comp['comppct_r']}%) - HSG {comp['hydgrp']}")
            
        except Exception as e:
            raise QgsProcessingException(f'Error downloading tabular data: {str(e)}')
        
        # Step 3: Merge data and create output
        feedback.pushInfo('\n' + '='*50)
        feedback.pushInfo('MERGING DATA')
        feedback.pushInfo('='*50)
        
        # Define output fields
        fields = QgsFields()
        fields.append(QgsField('mukey', QVariant.String))
        fields.append(QgsField('musym', QVariant.String))
        fields.append(QgsField('muname', QVariant.String))
        fields.append(QgsField('compname', QVariant.String))
        fields.append(QgsField('comppct_r', QVariant.Double))
        fields.append(QgsField('hydgrp', QVariant.String))
        
        # Get output sink
        (sink, dest_id) = self.parameterAsSink(
            parameters,
            self.OUTPUT_SOILS,
            context,
            fields,
            QgsWkbTypes.MultiPolygon,
            wgs84_crs
        )
        
        if sink is None:
            raise QgsProcessingException(self.invalidSinkError(parameters, self.OUTPUT_SOILS))
        
        # Add features to output
        hsg_counts = {}
        
        for feature in soils_layer.getFeatures():
            mukey = str(feature['mukey'])
            
            if mukey in dominant_components:
                comp = dominant_components[mukey]
                
                out_feature = QgsFeature(fields)
                out_feature.setGeometry(feature.geometry())
                out_feature['mukey'] = mukey
                out_feature['musym'] = comp['musym']
                out_feature['muname'] = comp['muname']
                out_feature['compname'] = comp['compname']
                out_feature['comppct_r'] = comp['comppct_r']
                out_feature['hydgrp'] = comp['hydgrp']
                
                sink.addFeature(out_feature)
                
                # Count HSG
                hydgrp = comp['hydgrp']
                hsg_counts[hydgrp] = hsg_counts.get(hydgrp, 0) + 1
        
        feedback.pushInfo('Merged spatial and tabular data')
        feedback.pushInfo('\nHydrologic Soil Group Distribution:')
        
        # Separate None values from valid HSG values
        valid_hsg = {k: v for k, v in hsg_counts.items() if k is not None and k != 'None'}
        none_hsg = {k: v for k, v in hsg_counts.items() if k is None or k == 'None'}
        
        # Display valid HSG groups sorted
        for hsg, count in sorted(valid_hsg.items()):
            feedback.pushInfo(f'  HSG {hsg}: {count} polygon(s)')
        
        # Display None/Water features if any
        if none_hsg:
            for hsg, count in none_hsg.items():
                feedback.pushInfo(f'  HSG None (Water/Pits): {count} polygon(s)')
        
        # Step 4: Create map if requested
        output_map = self.parameterAsFileOutput(parameters, self.OUTPUT_MAP, context)
        
        if output_map:
            feedback.pushInfo('\n' + '='*50)
            feedback.pushInfo('CREATING MAP')
            feedback.pushInfo('='*50)
            
            try:
                import matplotlib
                matplotlib.use('Agg')  # Use non-interactive backend
                import matplotlib.pyplot as plt
                from matplotlib.patches import Patch
                
                # Create temporary layer from sink
                temp_layer = QgsVectorLayer(dest_id, 'soils', 'ogr')
                
                # Color scheme
                colors = {
                    'A': '#2ECC71',
                    'B': '#F1C40F',
                    'C': '#E67E22',
                    'D': '#E74C3C',
                    'A/D': '#90EE90',
                    'B/D': '#FFD700',
                    'C/D': '#FF6347'
                }
                
                fig, ax = plt.subplots(figsize=(16, 12))
                
                # Plot each HSG (filter out None values for map)
                legend_handles = []
                valid_hsgs = sorted([k for k in hsg_counts.keys() if k is not None and k != 'None'])
                
                for hsg in valid_hsgs:
                    color = colors.get(hsg, '#95A5A6')
                    
                    # Plot features with this HSG
                    for feature in temp_layer.getFeatures():
                        if feature['hydgrp'] == hsg:
                            geom = feature.geometry()
                            
                            # Convert to matplotlib format
                            if geom.isMultipart():
                                polygons = geom.asMultiPolygon()
                                for polygon in polygons:
                                    for ring in polygon:
                                        xs = [p.x() for p in ring]
                                        ys = [p.y() for p in ring]
                                        ax.fill(xs, ys, color=color, edgecolor='black', linewidth=0.5, alpha=0.85)
                            else:
                                polygon = geom.asPolygon()
                                for ring in polygon:
                                    xs = [p.x() for p in ring]
                                    ys = [p.y() for p in ring]
                                    ax.fill(xs, ys, color=color, edgecolor='black', linewidth=0.5, alpha=0.85)
                    
                    legend_handles.append(Patch(facecolor=color, edgecolor='black', label=f'HSG {hsg}'))
                
                # Plot Water/Pits (None HSG) if present
                if None in hsg_counts or 'None' in hsg_counts:
                    water_color = '#87CEEB'  # Light blue for water/pits
                    for feature in temp_layer.getFeatures():
                        if feature['hydgrp'] is None or feature['hydgrp'] == 'None':
                            geom = feature.geometry()
                            
                            if geom.isMultipart():
                                polygons = geom.asMultiPolygon()
                                for polygon in polygons:
                                    for ring in polygon:
                                        xs = [p.x() for p in ring]
                                        ys = [p.y() for p in ring]
                                        ax.fill(xs, ys, color=water_color, edgecolor='black', linewidth=0.5, alpha=0.85)
                            else:
                                polygon = geom.asPolygon()
                                for ring in polygon:
                                    xs = [p.x() for p in ring]
                                    ys = [p.y() for p in ring]
                                    ax.fill(xs, ys, color=water_color, edgecolor='black', linewidth=0.5, alpha=0.85)
                    
                    legend_handles.append(Patch(facecolor=water_color, edgecolor='black', label='Water/Pits'))
                
                # Plot AOI boundary
                for feature in aoi_layer.getFeatures():
                    geom = feature.geometry()
                    boundary = geom.boundary()
                    
                    if boundary.isMultipart():
                        lines = boundary.asMultiPolyline()
                        for line in lines:
                            xs = [p.x() for p in line]
                            ys = [p.y() for p in line]
                            ax.plot(xs, ys, color='darkblue', linewidth=3.5, zorder=10)
                    else:
                        line = boundary.asPolyline()
                        xs = [p.x() for p in line]
                        ys = [p.y() for p in line]
                        ax.plot(xs, ys, color='darkblue', linewidth=3.5, zorder=10)
                
                legend_handles.append(Patch(facecolor='none', edgecolor='darkblue', linewidth=3, label='AOI Boundary'))
                
                # Formatting
                ax.set_xlabel('Longitude (°)', fontsize=13, fontweight='bold')
                ax.set_ylabel('Latitude (°)', fontsize=13, fontweight='bold')
                ax.set_title('SSURGO Soils Map - Hydrologic Soil Groups', fontsize=18, fontweight='bold', pad=20)
                ax.legend(handles=legend_handles, loc='upper left', fontsize=11, framealpha=0.95, edgecolor='black')
                ax.grid(True, alpha=0.3, linestyle='--', linewidth=0.5)
                ax.set_aspect('equal')
                
                # Info box
                info_text = 'Hydrologic Soil Group (HSG):\n\n' + \
                           'A = Low runoff potential,\n    high infiltration rate\n\n' + \
                           'B = Moderate infiltration rate\n\n' + \
                           'C = Slow infiltration rate\n\n' + \
                           'D = High runoff potential,\n    very slow infiltration'
                
                props = dict(boxstyle='round,pad=0.8', facecolor='wheat', edgecolor='black', alpha=0.9, linewidth=1.5)
                ax.text(0.98, 0.02, info_text, transform=ax.transAxes, fontsize=9,
                       verticalalignment='bottom', horizontalalignment='right', bbox=props)
                
                plt.tight_layout()
                plt.savefig(output_map, dpi=300, bbox_inches='tight')
                plt.close()
                
                feedback.pushInfo(f'Map saved to: {output_map}')
                
            except Exception as e:
                feedback.pushWarning(f'Could not create map: {str(e)}')
        
        feedback.pushInfo('\n' + '='*50)
        feedback.pushInfo('PROCESSING COMPLETE!')
        feedback.pushInfo('='*50)
        
        return {self.OUTPUT_SOILS: dest_id}