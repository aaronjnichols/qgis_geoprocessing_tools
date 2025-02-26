from qgis.PyQt.QtCore import QCoreApplication
from qgis.core import (QgsProcessing, QgsProcessingAlgorithm, QgsProcessingParameterRasterLayer,
                       QgsProcessingParameterNumber, QgsProcessingParameterFileDestination,
                       QgsRasterLayer, QgsMessageLog, Qgis, QgsProcessingException, QgsRectangle)
import processing
from osgeo import gdal
import numpy as np

class DEMComparisonTool(QgsProcessingAlgorithm):
    INPUT_EXISTING = 'INPUT_EXISTING'
    INPUT_PROPOSED = 'INPUT_PROPOSED'
    OUTPUT_DIFFERENCE = 'OUTPUT_DIFFERENCE'

    def initAlgorithm(self, config=None):
        self.addParameter(QgsProcessingParameterRasterLayer(self.INPUT_EXISTING, 'Existing DEM'))
        self.addParameter(QgsProcessingParameterRasterLayer(self.INPUT_PROPOSED, 'Proposed DEM'))
        self.addParameter(QgsProcessingParameterFileDestination(self.OUTPUT_DIFFERENCE, 'Output Difference Raster', 'TIF files (*.tif)'))

    def processAlgorithm(self, parameters, context, feedback):
        existing_dem = self.parameterAsRasterLayer(parameters, self.INPUT_EXISTING, context)
        proposed_dem = self.parameterAsRasterLayer(parameters, self.INPUT_PROPOSED, context)
        output_path = self.parameterAsOutputLayer(parameters, self.OUTPUT_DIFFERENCE, context)

        if existing_dem.crs() != proposed_dem.crs():
            raise QgsProcessingException("The coordinate systems of the input rasters do not match. Please ensure both rasters are in the same coordinate system.")

        # Get the overlapping extent
        overlap = existing_dem.extent().intersect(proposed_dem.extent())

        # Use GDAL to read and process the rasters
        existing_ds = gdal.Open(existing_dem.source())
        proposed_ds = gdal.Open(proposed_dem.source())

        # Get geotransform and calculate new dimensions
        existing_geotransform = existing_ds.GetGeoTransform()
        proposed_geotransform = proposed_ds.GetGeoTransform()

        # Calculate pixel coordinates for the overlap extent
        pixel_size = min(abs(existing_geotransform[1]), abs(proposed_geotransform[1]))
        x_min = max(existing_geotransform[0], proposed_geotransform[0])
        y_max = min(existing_geotransform[3], proposed_geotransform[3])
        x_max = min(existing_geotransform[0] + existing_geotransform[1] * existing_ds.RasterXSize,
                    proposed_geotransform[0] + proposed_geotransform[1] * proposed_ds.RasterXSize)
        y_min = max(existing_geotransform[3] + existing_geotransform[5] * existing_ds.RasterYSize,
                    proposed_geotransform[3] + proposed_geotransform[5] * proposed_ds.RasterYSize)

        # Calculate new dimensions
        width = int((x_max - x_min) / pixel_size)
        height = int((y_max - y_min) / pixel_size)

        # Create the output raster
        driver = gdal.GetDriverByName('GTiff')
        output_ds = driver.Create(output_path, width, height, 1, gdal.GDT_Float32)
        output_ds.SetGeoTransform((x_min, pixel_size, 0, y_max, 0, -pixel_size))
        output_ds.SetProjection(existing_ds.GetProjection())

        # Read data from input rasters
        existing_data = existing_ds.GetRasterBand(1).ReadAsArray(
            int((x_min - existing_geotransform[0]) / existing_geotransform[1]),
            int((y_max - existing_geotransform[3]) / existing_geotransform[5]),
            width, height)
        proposed_data = proposed_ds.GetRasterBand(1).ReadAsArray(
            int((x_min - proposed_geotransform[0]) / proposed_geotransform[1]),
            int((y_max - proposed_geotransform[3]) / proposed_geotransform[5]),
            width, height)

        # Calculate difference
        difference = proposed_data - existing_data

        # Write difference to output raster
        output_ds.GetRasterBand(1).WriteArray(difference)
        output_ds.FlushCache()

        # Calculate volumes
        cell_area = pixel_size * pixel_size
        cut_volume = np.sum(difference[difference < 0]) * cell_area / 27  # Convert to cubic yards
        fill_volume = np.sum(difference[difference > 0]) * cell_area / 27  # Convert to cubic yards
        net_volume = fill_volume + cut_volume  # Note: cut_volume is negative

        feedback.pushInfo(f"Cut Volume: {abs(cut_volume):.2f} cubic yards")
        feedback.pushInfo(f"Fill Volume: {fill_volume:.2f} cubic yards")
        feedback.pushInfo(f"Net Volume: {net_volume:.2f} cubic yards")

        # Clean up
        existing_ds = None
        proposed_ds = None
        output_ds = None

        return {self.OUTPUT_DIFFERENCE: output_path}

    def name(self):
        return 'demcomparisontool'

    def displayName(self):
        return 'DEM Comparison Tool'

    def group(self):
        return 'Custom Tools'

    def groupId(self):
        return 'customtools'

    def shortHelpString(self):
        return "This tool compares two DEMs, calculates the difference, and provides volume calculations."

    def createInstance(self):
        return DEMComparisonTool()