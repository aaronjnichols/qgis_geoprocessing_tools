from qgis.core import (QgsProcessing, QgsProcessingAlgorithm, QgsProcessingParameterMultipleLayers,
                       QgsProcessingParameterNumber, QgsProcessingParameterRasterDestination,
                       QgsProcessingParameterVectorDestination, QgsProcessingParameterCrs,
                       QgsRasterLayer, QgsProject)
from qgis.PyQt.QtCore import QCoreApplication
import processing

class RasterCombineContourAlgorithm(QgsProcessingAlgorithm):
    INPUT_RASTERS = 'INPUT_RASTERS'
    CONTOUR_INTERVAL = 'CONTOUR_INTERVAL'
    OUTPUT_CRS = 'OUTPUT_CRS'
    OUTPUT_RASTER = 'OUTPUT_RASTER'
    OUTPUT_CONTOURS = 'OUTPUT_CONTOURS'

    def initAlgorithm(self, config=None):
        self.addParameter(QgsProcessingParameterMultipleLayers(self.INPUT_RASTERS, 'Input raster DEMs', QgsProcessing.TypeRaster))
        self.addParameter(QgsProcessingParameterNumber(self.CONTOUR_INTERVAL, 'Contour interval (feet)', type=QgsProcessingParameterNumber.Double, defaultValue=1.0, minValue=0.0))
        self.addParameter(QgsProcessingParameterCrs(self.OUTPUT_CRS, 'Output CRS'))
        self.addParameter(QgsProcessingParameterRasterDestination(self.OUTPUT_RASTER, 'Output combined raster'))
        self.addParameter(QgsProcessingParameterVectorDestination(self.OUTPUT_CONTOURS, 'Output contour shapefile'))

    def processAlgorithm(self, parameters, context, feedback):
        input_rasters = self.parameterAsLayerList(parameters, self.INPUT_RASTERS, context)
        contour_interval = self.parameterAsDouble(parameters, self.CONTOUR_INTERVAL, context)
        output_crs = self.parameterAsCrs(parameters, self.OUTPUT_CRS, context)
        output_raster_path = self.parameterAsOutputLayer(parameters, self.OUTPUT_RASTER, context)
        output_contours_path = self.parameterAsOutputLayer(parameters, self.OUTPUT_CONTOURS, context)

        # Combine rasters
        combined_raster = processing.run("gdal:merge", {'INPUT': [raster.source() for raster in input_rasters], 'PCT': False, 'SEPARATE': False, 'NODATA_INPUT': None, 'NODATA_OUTPUT': None, 'OPTIONS': '', 'DATA_TYPE': 5, 'OUTPUT': 'TEMPORARY_OUTPUT'}, context=context, feedback=feedback)['OUTPUT']

        # Convert raster values from meters to feet
        converted_raster = processing.run("gdal:rastercalculator", {'INPUT_A': combined_raster, 'BAND_A': 1, 'INPUT_B': None, 'BAND_B': -1, 'INPUT_C': None, 'BAND_C': -1, 'INPUT_D': None, 'BAND_D': -1, 'INPUT_E': None, 'BAND_E': -1, 'INPUT_F': None, 'BAND_F': -1, 'FORMULA': 'A * 3.28084', 'NO_DATA': None, 'RTYPE': 5, 'OPTIONS': '', 'OUTPUT': output_raster_path}, context=context, feedback=feedback)['OUTPUT']

        # Assign output CRS
        raster_layer = QgsRasterLayer(converted_raster, 'Converted Raster')
        raster_layer.setCrs(output_crs)
        raster_layer.saveDefaultStyle()

        # Create contours
        contour_layer = processing.run("gdal:contour", {
            'INPUT': converted_raster,
            'BAND': 1,
            'INTERVAL': contour_interval,
            'FIELD_NAME': 'ELEV',
            'CREATE_3D': False,
            'IGNORE_NODATA': False,
            'NODATA': None,
            'OFFSET': 0,
            'EXTRA': '',
            'OUTPUT': output_contours_path
        }, context=context, feedback=feedback)['OUTPUT']

        return {self.OUTPUT_RASTER: converted_raster, self.OUTPUT_CONTOURS: contour_layer}

    def name(self):
        return 'usgs_lidar_to_contours'

    def displayName(self):
        return 'USGS LiDAR to Contours'

    def group(self):
        return 'Custom Tools'

    def groupId(self):
        return 'customtools'

    def createInstance(self):
        return RasterCombineContourAlgorithm()

    def tr(self, string):
        return QCoreApplication.translate('Processing', string)

    def shortHelpString(self):
        return self.tr("Combines multiple raster DEMs, converts values from meters to feet, assigns the specified output CRS, and creates contours at a given interval.")