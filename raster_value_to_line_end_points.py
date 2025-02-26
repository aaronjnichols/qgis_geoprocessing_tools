from qgis.PyQt.QtCore import QCoreApplication
from qgis.core import (QgsProcessing,
                       QgsProcessingAlgorithm,
                       QgsProcessingParameterFeatureSource,
                       QgsProcessingParameterRasterLayer,
                       QgsField,
                       QgsPointXY,
                       QgsProcessingException,
                       QgsVectorLayer,
                       QgsFeatureRequest,
                       QgsRaster,
                       QgsProcessingParameterFeatureSink)
from PyQt5.QtCore import QVariant

class SampleRasterAtLineEndpoints(QgsProcessingAlgorithm):
    INPUT_LINE_LAYER = 'INPUT_LINE_LAYER'
    INPUT_RASTER_LAYER = 'INPUT_RASTER_LAYER'
    OUTPUT_LAYER = 'OUTPUT_LAYER'

    def tr(self, string):
        return QCoreApplication.translate('SampleRasterAtLineEndpoints', string)

    def createInstance(self):
        return SampleRasterAtLineEndpoints()

    def name(self):
        return 'sampleRasterAtLineEndpoints'

    def displayName(self):
        return self.tr('Sample Raster at Line Endpoints')

    def group(self):
        return self.tr('Custom Scripts')

    def groupId(self):
        return 'customscripts'

    def initAlgorithm(self, config=None):
        self.addParameter(
            QgsProcessingParameterFeatureSource(
                self.INPUT_LINE_LAYER,
                self.tr('Input Line Layer'),
                [QgsProcessing.TypeVectorLine]
            )
        )

        self.addParameter(
            QgsProcessingParameterRasterLayer(
                self.INPUT_RASTER_LAYER,
                self.tr('Input Raster Layer')
            )
        )

    def processAlgorithm(self, parameters, context, feedback):
        line_layer = self.parameterAsVectorLayer(parameters, self.INPUT_LINE_LAYER, context)
        raster_layer = self.parameterAsRasterLayer(parameters, self.INPUT_RASTER_LAYER, context)

        # Verify that layer is valid
        if not line_layer or not line_layer.isValid():
            raise QgsProcessingException(self.invalidSourceError(parameters, self.INPUT_LINE_LAYER))
        if not raster_layer or not raster_layer.isValid():
            raise QgsProcessingException(self.invalidSourceError(parameters, self.INPUT_RASTER_LAYER))

        # Add fields to the line layer
        field_names = ['StartVal', 'EndVal', 'Slope', 'Length']
        field_type = QVariant.Double
        for field_name in field_names:
            if line_layer.fields().indexFromName(field_name) == -1:
                line_layer.dataProvider().addAttributes([QgsField(field_name, field_type)])
        line_layer.updateFields()

        # Start editing the line layer
        line_layer.startEditing()

        # Process each feature
        for feature in line_layer.getFeatures():
            geom = feature.geometry()
            # Check if geometry is multipart and get start and end points accordingly
            if geom.isMultipart():
                lines = geom.asMultiPolyline()
                start_point = lines[0][0]  # First point of the first part
                end_point = lines[-1][-1]  # Last point of the last part
            else:
                line = geom.asPolyline()
                start_point = line[0]  # First point
                end_point = line[-1]  # Last point

            # Sample raster values
            start_val = self.get_raster_value_at_point(raster_layer, QgsPointXY(start_point))
            end_val = self.get_raster_value_at_point(raster_layer, QgsPointXY(end_point))

            # Calculate length of the line in the layer's units
            length = geom.length()

            # Calculate the slope
            slope = (start_val - end_val) / length if length != 0 else 0

            # Update the feature's attributes
            feature.setAttribute(feature.fields().indexFromName('StartVal'), start_val)
            feature.setAttribute(feature.fields().indexFromName('EndVal'), end_val)
            feature.setAttribute(feature.fields().indexFromName('Slope'), slope)
            feature.setAttribute(feature.fields().indexFromName('Length'), length)
            line_layer.updateFeature(feature)

        # Commit changes
        line_layer.commitChanges()

        return {self.OUTPUT_LAYER: line_layer.id()}

    def get_raster_value_at_point(self, raster_layer, point):
        ident = raster_layer.dataProvider().identify(point, QgsRaster.IdentifyFormatValue)
        if ident.isValid():
            # The result of identify is a dictionary with band numbers as keys
            # We assume here that you're interested in the value of the first band
            return ident.results()[1]
        return None

    def shortHelpString(self):
        return self.tr("This tool samples raster values at the start and end points of each line in the provided line layer, calculates the length of each line, computes the slope, and updates the layer with these values.")

