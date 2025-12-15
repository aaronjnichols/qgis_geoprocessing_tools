"""
FEMA NFHL Data Downloader for QGIS
Download flood hazard data from FEMA National Flood Hazard Layer API

Author: Aaron Nichols, PE
Date: November 2025
"""

from qgis.PyQt.QtCore import QCoreApplication, QVariant, QDate
from qgis.core import (
    QgsProcessing,
    QgsProcessingAlgorithm,
    QgsProcessingParameterVectorLayer,
    QgsProcessingParameterFolderDestination,
    QgsProcessingParameterEnum,
    QgsProcessingParameterBoolean,
    QgsProcessingException,
    QgsVectorLayer,
    QgsVectorFileWriter,
    QgsCoordinateReferenceSystem,
    QgsCoordinateTransform,
    QgsProject,
    QgsGeometry,
    QgsFeature,
    QgsFields,
    QgsField,
    QgsWkbTypes,
    QgsProcessingMultiStepFeedback
)
import processing
import requests
import json
import os
from pathlib import Path
from datetime import datetime


class FemaNfhlDownloaderAlgorithm(QgsProcessingAlgorithm):
    """
    Download FEMA NFHL flood hazard data for a specified area of interest.
    """
    
    # Parameter names
    INPUT_AOI = 'INPUT_AOI'
    OUTPUT_FOLDER = 'OUTPUT_FOLDER'
    LAYER_SELECTION = 'LAYER_SELECTION'
    CLIP_LAYERS = 'CLIP_LAYERS'
    LOAD_LAYERS = 'LOAD_LAYERS'
    
    # FEMA NFHL API endpoint
    NFHL_BASE_URL = "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer"
    
    # Available layers with their IDs and default clipping behavior
    AVAILABLE_LAYERS = {
        'Flood Hazard Zones': {'id': 28, 'clip': True, 'priority': 1},
        'Flood Hazard Boundaries': {'id': 27, 'clip': True, 'priority': 2},
        'Base Flood Elevations': {'id': 16, 'clip': True, 'priority': 3},
        'Cross-Sections': {'id': 14, 'clip': True, 'priority': 4},
        'Water Lines': {'id': 20, 'clip': True, 'priority': 5},
        'Water Areas': {'id': 32, 'clip': True, 'priority': 6},
        'Levees': {'id': 23, 'clip': True, 'priority': 7},
        'General Structures': {'id': 24, 'clip': True, 'priority': 8},
        'Profile Baselines': {'id': 17, 'clip': True, 'priority': 9},
        'Subbasins': {'id': 31, 'clip': False, 'priority': 10},
        'Coastal Transects': {'id': 15, 'clip': True, 'priority': 11},
        'Limit of Moderate Wave Action': {'id': 19, 'clip': True, 'priority': 12},
        'Primary Frontal Dunes': {'id': 25, 'clip': True, 'priority': 13},
        'LOMAs': {'id': 34, 'clip': True, 'priority': 14},
        'LOMRs': {'id': 1, 'clip': True, 'priority': 15},
        'Alluvial Fans': {'id': 30, 'clip': True, 'priority': 16},
        'Transect Baselines': {'id': 18, 'clip': True, 'priority': 17},
        'Coastal Gages': {'id': 9, 'clip': True, 'priority': 18},
        'Gages': {'id': 10, 'clip': True, 'priority': 19},
        'High Water Marks': {'id': 12, 'clip': True, 'priority': 20},
        'Topographic Low Confidence Areas': {'id': 6, 'clip': True, 'priority': 21},
        'Hydrologic Reaches': {'id': 26, 'clip': True, 'priority': 22},
        'Political Jurisdictions': {'id': 22, 'clip': False, 'priority': 23},
        'FIRM Panels': {'id': 3, 'clip': False, 'priority': 24},
        'NFHL Availability': {'id': 0, 'clip': False, 'priority': 25},
        'Base Index': {'id': 4, 'clip': False, 'priority': 26},
        'PLSS': {'id': 5, 'clip': False, 'priority': 27},
    }

    def tr(self, string):
        """Returns a translatable string"""
        return QCoreApplication.translate('Processing', string)

    def createInstance(self):
        """Returns a new instance of the algorithm"""
        return FemaNfhlDownloaderAlgorithm()

    def name(self):
        """Returns the algorithm name"""
        return 'fema_nfhl_downloader'

    def displayName(self):
        """Returns the translated algorithm name"""
        return self.tr('Download FEMA NFHL Data')

    def group(self):
        """Returns the name of the group this algorithm belongs to"""
        return self.tr('Data Download Toolbox')

    def groupId(self):
        """Returns the unique ID of the group this algorithm belongs to"""
        return 'data_download_toolbox'

    def shortHelpString(self):
        """Returns a localised short helper string for the algorithm"""
        return self.tr("""
        Download flood hazard data from FEMA National Flood Hazard Layer (NFHL) API.
        
        <b>Parameters:</b>
        
        <b>Input AOI:</b> Vector layer defining your area of interest. The tool will 
        download all FEMA data that intersects this area.
        
        <b>Output Folder:</b> Directory where downloaded shapefiles will be saved.
        
        <b>Layers to Download:</b> Select specific layers or choose "All Layers" to 
        download everything available. Most important layers are at the top of the list.
        
        <b>Clip Layers to AOI:</b> When checked, flood hazard layers will be clipped 
        to your AOI boundary. Reference layers (political boundaries, FIRM panels) are 
        never clipped.
        
        <b>Load Downloaded Layers into QGIS:</b> When checked, all successfully 
        downloaded layers will be automatically added to your QGIS project. Uncheck 
        if you only want to save files without loading them.
        
        <b>Key Layers:</b>
        • Flood Hazard Zones - Primary SFHA mapping (Zones A, AE, etc.)
        • Base Flood Elevations - BFE lines for structure elevation requirements
        • Cross-Sections - Hydraulic model locations
        • Water Lines - Stream centerlines
        
        <b>Data Source:</b> FEMA NFHL REST API
        https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer
        
        <b>Output:</b> Shapefiles in WGS84 (EPSG:4326)
        
        <b>Note:</b> Download time varies based on area size and number of layers selected.
        Large areas may take several minutes.
        """)

    def initAlgorithm(self, config=None):
        """Define the inputs and output of the algorithm"""
        
        # Input AOI
        self.addParameter(
            QgsProcessingParameterVectorLayer(
                self.INPUT_AOI,
                self.tr('Input Area of Interest (AOI)'),
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
        
        # Layer selection (multi-select enum)
        layer_names = ['All Layers'] + sorted(
            self.AVAILABLE_LAYERS.keys(), 
            key=lambda x: self.AVAILABLE_LAYERS[x]['priority']
        )
        
        self.addParameter(
            QgsProcessingParameterEnum(
                self.LAYER_SELECTION,
                self.tr('Layers to Download'),
                options=layer_names,
                allowMultiple=True,
                defaultValue=[0]  # Default to "All Layers"
            )
        )
        
        # Clip layers option
        self.addParameter(
            QgsProcessingParameterBoolean(
                self.CLIP_LAYERS,
                self.tr('Clip layers to AOI (recommended for flood hazard layers)'),
                defaultValue=True
            )
        )
        
        # Load layers option
        self.addParameter(
            QgsProcessingParameterBoolean(
                self.LOAD_LAYERS,
                self.tr('Load downloaded layers into QGIS'),
                defaultValue=True
            )
        )

    def processAlgorithm(self, parameters, context, feedback):
        """Main processing function"""
        
        # Get parameters
        aoi_layer = self.parameterAsVectorLayer(parameters, self.INPUT_AOI, context)
        output_folder = self.parameterAsString(parameters, self.OUTPUT_FOLDER, context)
        selected_indices = self.parameterAsEnums(parameters, self.LAYER_SELECTION, context)
        clip_layers = self.parameterAsBool(parameters, self.CLIP_LAYERS, context)
        load_layers = self.parameterAsBool(parameters, self.LOAD_LAYERS, context)
        
        if not aoi_layer:
            raise QgsProcessingException(self.tr('Invalid AOI layer'))
        
        if not output_folder:
            raise QgsProcessingException(self.tr('Output folder not specified'))
        
        # Create output directory if it doesn't exist
        os.makedirs(output_folder, exist_ok=True)
        
        # Determine which layers to download
        layer_names = ['All Layers'] + sorted(
            self.AVAILABLE_LAYERS.keys(), 
            key=lambda x: self.AVAILABLE_LAYERS[x]['priority']
        )
        
        if 0 in selected_indices:  # "All Layers" selected
            layers_to_download = list(self.AVAILABLE_LAYERS.keys())
        else:
            layers_to_download = [layer_names[i] for i in selected_indices]
        
        feedback.pushInfo(f'Downloading {len(layers_to_download)} layer(s)')
        
        # Get AOI bounds in WGS84
        feedback.pushInfo('Preparing AOI...')
        aoi_wgs84 = self._get_aoi_wgs84(aoi_layer, feedback)
        bounds = aoi_wgs84.extent()
        
        bounds_dict = {
            'minx': bounds.xMinimum(),
            'miny': bounds.yMinimum(),
            'maxx': bounds.xMaximum(),
            'maxy': bounds.yMaximum()
        }
        
        feedback.pushInfo(f'AOI Bounds: W={bounds_dict["minx"]:.6f}, S={bounds_dict["miny"]:.6f}, '
                         f'E={bounds_dict["maxx"]:.6f}, N={bounds_dict["maxy"]:.6f}')
        
        # Setup multi-step feedback
        multi_feedback = QgsProcessingMultiStepFeedback(len(layers_to_download), feedback)
        
        # Download each layer
        success_count = 0
        failed_count = 0
        downloaded_layers = []  # Track successfully downloaded layer paths
        
        # Get geometry from first feature of AOI for clipping
        aoi_geom = None
        for feature in aoi_wgs84.getFeatures():
            aoi_geom = feature.geometry()
            break
        
        for i, layer_name in enumerate(layers_to_download):
            if multi_feedback.isCanceled():
                break
            
            multi_feedback.setCurrentStep(i)
            multi_feedback.pushInfo(f'\n[{i+1}/{len(layers_to_download)}] Downloading: {layer_name}')
            
            layer_info = self.AVAILABLE_LAYERS[layer_name]
            layer_id = layer_info['id']
            should_clip = clip_layers and layer_info['clip']
            
            clip_geom = aoi_geom if should_clip else None
            
            result = self._download_layer(
                layer_id, 
                layer_name, 
                bounds_dict, 
                output_folder,
                clip_geom,
                multi_feedback
            )
            
            if result:
                success_count += 1
                downloaded_layers.append(result)  # Store the path
            else:
                failed_count += 1
        
        # Summary
        feedback.pushInfo('\n' + '='*70)
        feedback.pushInfo('DOWNLOAD COMPLETE')
        feedback.pushInfo('='*70)
        feedback.pushInfo(f'Successfully downloaded: {success_count} layer(s)')
        if failed_count > 0:
            feedback.pushInfo(f'Failed or no data: {failed_count} layer(s)')
        feedback.pushInfo(f'Output directory: {output_folder}')
        
        # Create README file
        self._create_readme(output_folder, bounds_dict, success_count, failed_count)
        
        # Load layers into QGIS if requested
        if load_layers and len(downloaded_layers) > 0:
            feedback.pushInfo('\n' + '='*70)
            feedback.pushInfo('LOADING LAYERS INTO QGIS')
            feedback.pushInfo('='*70)
            
            for layer_path in downloaded_layers:
                layer_name = os.path.splitext(os.path.basename(layer_path))[0]
                layer = QgsVectorLayer(layer_path, layer_name, 'ogr')
                
                if layer.isValid():
                    QgsProject.instance().addMapLayer(layer)
                    feedback.pushInfo(f'  ✓ Loaded: {layer_name}')
                else:
                    feedback.pushInfo(f'  ✗ Failed to load: {layer_name}')
        
        return {self.OUTPUT_FOLDER: output_folder}

    def _get_aoi_wgs84(self, aoi_layer, feedback):
        """Convert AOI to WGS84 if needed"""
        target_crs = QgsCoordinateReferenceSystem('EPSG:4326')
        
        if aoi_layer.crs() != target_crs:
            feedback.pushInfo(f'Reprojecting AOI from {aoi_layer.crs().authid()} to EPSG:4326')
            
            # Create transform
            transform = QgsCoordinateTransform(
                aoi_layer.crs(),
                target_crs,
                QgsProject.instance()
            )
            
            # Create new layer in WGS84
            aoi_wgs84 = QgsVectorLayer(
                f'Polygon?crs=EPSG:4326',
                'aoi_wgs84',
                'memory'
            )
            
            provider = aoi_wgs84.dataProvider()
            provider.addAttributes(aoi_layer.fields())
            aoi_wgs84.updateFields()
            
            # Transform features
            for feature in aoi_layer.getFeatures():
                new_feature = QgsFeature(feature)
                geom = feature.geometry()
                geom.transform(transform)
                new_feature.setGeometry(geom)
                provider.addFeature(new_feature)
            
            return aoi_wgs84
        else:
            return aoi_layer

    def _download_layer(self, layer_id, layer_name, bounds, output_dir, clip_geom, feedback):
        """Download a specific layer from FEMA NFHL"""
        
        query_url = f"{self.NFHL_BASE_URL}/{layer_id}/query"
        bbox_str = f"{bounds['minx']},{bounds['miny']},{bounds['maxx']},{bounds['maxy']}"
        
        params = {
            'where': '1=1',
            'geometry': bbox_str,
            'geometryType': 'esriGeometryEnvelope',
            'spatialRel': 'esriSpatialRelIntersects',
            'outFields': '*',
            'returnGeometry': 'true',
            'f': 'geojson',
            'outSR': '4326'
        }
        
        try:
            response = requests.get(query_url, params=params, timeout=60)
            
            if response.status_code != 200:
                feedback.pushInfo(f'  ✗ HTTP Error: {response.status_code}')
                return None
            
            data = response.json()
            
            if 'features' not in data or len(data['features']) == 0:
                feedback.pushInfo(f'  - No features found')
                return None
            
            feedback.pushInfo(f'  Retrieved {len(data["features"])} features')
            
            # Convert to QGIS format
            layer = self._geojson_to_qgis_layer(data, layer_name, feedback)
            
            if not layer or not layer.isValid():
                feedback.pushInfo(f'  ✗ Failed to create layer')
                return None
            
            # Convert timestamp fields to proper dates
            layer = self._convert_timestamp_fields(layer, feedback)
            
            # Clip if requested
            if clip_geom:
                feedback.pushInfo(f'  Clipping to AOI...')
                layer = self._clip_layer(layer, clip_geom, feedback)
                
                if not layer or layer.featureCount() == 0:
                    feedback.pushInfo(f'  - No features after clipping')
                    return None
                
                feedback.pushInfo(f'  {layer.featureCount()} features after clipping')
            
            # Save as shapefile
            safe_name = layer_name.replace(' ', '_').replace('/', '_')
            output_path = os.path.join(output_dir, f'{safe_name}.shp')
            
            success = self._save_layer_as_shapefile(layer, output_path, feedback)
            
            if success and os.path.exists(output_path):
                feedback.pushInfo(f'  ✓ Saved: {safe_name}.shp')
                return output_path  # Return path for loading later
            else:
                feedback.pushInfo(f'  ✗ Failed to save shapefile')
                return None
            
        except requests.Timeout:
            feedback.pushInfo(f'  ✗ Request timeout')
            return None
        except Exception as e:
            feedback.pushInfo(f'  ✗ Error: {str(e)}')
            return None

    def _geojson_to_qgis_layer(self, geojson_data, layer_name, feedback):
        """Convert GeoJSON to QGIS vector layer"""
        
        from qgis.core import QgsJsonUtils
        
        # Convert to GeoJSON string
        geojson_str = json.dumps(geojson_data)
        
        # First, extract field definitions from GeoJSON
        fields = QgsJsonUtils.stringToFields(geojson_str, None)
        
        # Then parse features with the field definitions
        features = QgsJsonUtils.stringToFeatureList(geojson_str, fields, None)
        
        if not features:
            return None
        
        # Determine geometry type from first feature
        first_geom = features[0].geometry()
        if first_geom.isEmpty():
            return None
        
        geom_type = QgsWkbTypes.displayString(first_geom.wkbType())
        
        # Create memory layer
        layer = QgsVectorLayer(f'{geom_type}?crs=EPSG:4326', layer_name, 'memory')
        provider = layer.dataProvider()
        
        # Add fields from the parsed GeoJSON
        provider.addAttributes(fields)
        layer.updateFields()
        
        # Add all features
        provider.addFeatures(features)
        layer.updateExtents()
        
        return layer

    def _convert_timestamp_fields(self, layer, feedback):
        """Convert Unix timestamp fields (in milliseconds) to date fields"""
        
        # Common FEMA date field names
        date_field_names = [
            'EFF_DATE', 'EFFECTIVE_DATE', 'EFFECT_DATE', 'EFFDATE',
            'REV_DATE', 'REVISION_DATE', 'REVDATE',
            'STATUS_DATE', 'STATUSDATE',
            'CREATE_DATE', 'CREATEDATE',
            'INIT_DATE', 'INITDATE',
            'VERSION_DATE', 'VERSIONDATE'
        ]
        
        # Find fields that are likely timestamps
        fields_to_convert = []
        for field in layer.fields():
            field_name_upper = field.name().upper()
            # Check if field name suggests it's a date
            if any(date_name in field_name_upper for date_name in date_field_names):
                # Check if it's numeric type (timestamps come as numbers)
                if field.type() in [QVariant.Int, QVariant.LongLong, QVariant.Double]:
                    fields_to_convert.append(field.name())
        
        if not fields_to_convert:
            return layer  # No date fields to convert
        
        feedback.pushInfo(f'  Converting date fields: {", ".join(fields_to_convert)}')
        
        # Create new layer with date fields
        new_layer = QgsVectorLayer(
            f'{QgsWkbTypes.displayString(layer.wkbType())}?crs=EPSG:4326',
            layer.name(),
            'memory'
        )
        
        provider = new_layer.dataProvider()
        
        # Add fields, converting date fields to Date type
        new_fields = []
        for field in layer.fields():
            if field.name() in fields_to_convert:
                # Create date field
                date_field = QgsField(field.name(), QVariant.Date)
                new_fields.append(date_field)
            else:
                new_fields.append(field)
        
        provider.addAttributes(new_fields)
        new_layer.updateFields()
        
        # Copy features with converted dates
        for feature in layer.getFeatures():
            new_feature = QgsFeature(new_layer.fields())
            new_feature.setGeometry(feature.geometry())
            
            for field_name in feature.fields().names():
                value = feature[field_name]
                
                if field_name in fields_to_convert and value is not None:
                    try:
                        # Convert milliseconds to seconds
                        timestamp_seconds = int(value) / 1000
                        # Convert to datetime using UTC (FEMA timestamps are in UTC)
                        dt = datetime.utcfromtimestamp(timestamp_seconds)
                        # Convert to QDate
                        qdate = QDate(dt.year, dt.month, dt.day)
                        new_feature[field_name] = qdate
                    except (ValueError, TypeError, OSError):
                        # If conversion fails, leave as None
                        new_feature[field_name] = None
                else:
                    new_feature[field_name] = value
            
            provider.addFeature(new_feature)
        
        new_layer.updateExtents()
        return new_layer

    def _clip_layer(self, layer, clip_geom, feedback):
        """Clip layer to geometry"""
        
        # Create memory layer for clipped features
        clipped_layer = QgsVectorLayer(
            f'{QgsWkbTypes.displayString(layer.wkbType())}?crs=EPSG:4326',
            'clipped',
            'memory'
        )
        
        provider = clipped_layer.dataProvider()
        provider.addAttributes(layer.fields())
        clipped_layer.updateFields()
        
        # Clip features
        for feature in layer.getFeatures():
            geom = feature.geometry()
            
            if geom.intersects(clip_geom):
                clipped_geom = geom.intersection(clip_geom)
                
                if not clipped_geom.isEmpty():
                    new_feature = QgsFeature(feature)
                    new_feature.setGeometry(clipped_geom)
                    provider.addFeature(new_feature)
        
        return clipped_layer

    def _save_layer_as_shapefile(self, layer, output_path, feedback):
        """Save QGIS layer as shapefile"""
        
        # Delete existing shapefile if it exists (including all components)
        base_path = os.path.splitext(output_path)[0]
        for ext in ['.shp', '.shx', '.dbf', '.prj', '.cpg', '.qpj', '.shp.xml']:
            file_path = base_path + ext
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except Exception as e:
                    feedback.pushInfo(f'  Warning: Could not delete {ext}: {e}')
        
        # Ensure the layer is valid
        if not layer or not layer.isValid():
            feedback.pushInfo(f'  Error: Invalid layer')
            return False
        
        # Use processing.run for more robust export
        try:
            import processing
            result = processing.run("native:savefeatures", {
                'INPUT': layer,
                'OUTPUT': output_path,
                'LAYER_NAME': '',
                'DATASOURCE_OPTIONS': '',
                'LAYER_OPTIONS': ''
            })
            
            # Check if file was created
            if os.path.exists(output_path):
                return True
            else:
                feedback.pushInfo(f'  Error: File not created at {output_path}')
                return False
                
        except Exception as e:
            feedback.pushInfo(f'  Error during save: {str(e)}')
            return False

    def _create_readme(self, output_dir, bounds, success_count, failed_count):
        """Create README file with download information"""
        
        readme_path = os.path.join(output_dir, 'README.txt')
        
        with open(readme_path, 'w') as f:
            f.write('FEMA NFHL DATA DOWNLOAD\n')
            f.write('='*70 + '\n\n')
            f.write('Data Source: FEMA National Flood Hazard Layer (NFHL)\n')
            f.write(f'API: {self.NFHL_BASE_URL}\n')
            f.write(f'Coordinate System: EPSG:4326 (WGS84)\n\n')
            f.write('AOI Bounds (WGS84):\n')
            f.write(f'  West:  {bounds["minx"]:.6f}\n')
            f.write(f'  South: {bounds["miny"]:.6f}\n')
            f.write(f'  East:  {bounds["maxx"]:.6f}\n')
            f.write(f'  North: {bounds["maxy"]:.6f}\n\n')
            f.write('Download Summary:\n')
            f.write(f'  Successful: {success_count} layer(s)\n')
            f.write(f'  No data/Failed: {failed_count} layer(s)\n\n')
            f.write('Notes:\n')
            f.write('- All data is in WGS84 (EPSG:4326)\n')
            f.write('- Recommend reprojecting to local coordinate system for analysis\n')
            f.write('- Flood zones: A, AE, AH = Special Flood Hazard Areas (SFHA)\n')
            f.write('- Base Flood Elevations (BFE) are in feet NAVD88\n')
            f.write('- Date fields converted from Unix timestamps to YYYY-MM-DD format (UTC)\n')
            f.write('- For more information: 1-877-FEMA-MAP or https://hazards.fema.gov/\n')