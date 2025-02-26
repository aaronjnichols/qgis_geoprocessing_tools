import os
from qgis.PyQt.QtCore import QCoreApplication, QVariant
from qgis.core import (QgsProcessing, QgsProcessingAlgorithm, QgsProcessingParameterFeatureSource,
                       QgsProcessingParameterRasterLayer, QgsProcessingParameterFileDestination,
                       QgsProcessingParameterField, QgsVectorLayer, QgsRasterLayer, QgsPointXY, 
                       QgsGeometry, QgsProcessingProvider, QgsCoordinateTransform, QgsProject)
import processing
import pandas as pd
import xlsxwriter
import math

class ProfileGeneratorAlgorithm(QgsProcessingAlgorithm):
    INPUT_LINES = 'INPUT_LINES'
    INPUT_DEM = 'INPUT_DEM'
    NAME_FIELD = 'NAME_FIELD'
    OUTPUT_EXCEL = 'OUTPUT_EXCEL'

    def initAlgorithm(self, config=None):
        self.addParameter(QgsProcessingParameterFeatureSource(
            self.INPUT_LINES,
            'Input MultiLineString layer',
            [QgsProcessing.TypeVectorLine]
        ))
        self.addParameter(QgsProcessingParameterRasterLayer(
            self.INPUT_DEM,
            'Input DEM'
        ))
        self.addParameter(QgsProcessingParameterField(
            self.NAME_FIELD,
            'Name Field',
            parentLayerParameterName=self.INPUT_LINES,
            type=QgsProcessingParameterField.Any
        ))
        self.addParameter(QgsProcessingParameterFileDestination(
            self.OUTPUT_EXCEL,
            'Output Excel file',
            'Microsoft Excel (*.xlsx)'
        ))

    def processAlgorithm(self, parameters, context, feedback):
        lines_layer = self.parameterAsSource(parameters, self.INPUT_LINES, context)
        dem_layer = self.parameterAsRasterLayer(parameters, self.INPUT_DEM, context)
        name_field = self.parameterAsString(parameters, self.NAME_FIELD, context)
        output_excel = self.parameterAsFileOutput(parameters, self.OUTPUT_EXCEL, context)

        if lines_layer.sourceCrs() != dem_layer.crs():
            transform = QgsCoordinateTransform(lines_layer.sourceCrs(), dem_layer.crs(), QgsProject.instance())
        else:
            transform = None

        no_data_value = dem_layer.dataProvider().sourceNoDataValue(1)

        writer = pd.ExcelWriter(output_excel, engine='xlsxwriter')

        sorted_features = sorted(lines_layer.getFeatures(), key=lambda f: f[name_field])
        total_features = lines_layer.featureCount()

        for current, feature in enumerate(sorted_features):
            if feedback.isCanceled():
                break

            feedback.setProgress(int(current / total_features * 100))

            feature_name = feature[name_field]
            sheet_name = self.clean_sheet_name(str(feature_name))

            geom = feature.geometry()
            if geom.isMultipart():
                lines = geom.asMultiPolyline()
            else:
                lines = [geom.asPolyline()]

            profile_data = []
            total_distance = 0
            for line in lines:
                for i in range(len(line) - 1):
                    start_point = line[i]
                    end_point = line[i+1]
                    if transform:
                        start_point = transform.transform(start_point)
                        end_point = transform.transform(end_point)
                    
                    segment_length = start_point.distance(end_point)
                    num_points = math.ceil(segment_length / 4)
                    
                    for j in range(num_points):
                        distance = j * 4
                        if distance > segment_length:
                            break
                        point = QgsPointXY(
                            start_point.x() + (end_point.x() - start_point.x()) * distance / segment_length,
                            start_point.y() + (end_point.y() - start_point.y()) * distance / segment_length
                        )
                        elevation = self.get_elevation_at_point(dem_layer, point, no_data_value)
                        if elevation is not None:
                            profile_data.append([total_distance + distance, elevation])
                
                total_distance += segment_length

            df = pd.DataFrame(profile_data, columns=['Distance', 'Elevation'])
            df.to_excel(writer, sheet_name=sheet_name, index=False)

            workbook = writer.book
            worksheet = writer.sheets[sheet_name]

            chart = workbook.add_chart({'type': 'scatter', 'subtype': 'straight_with_markers'})

            chart.add_series({
                'name':       feature_name,
                'categories': [sheet_name, 1, 0, len(df), 0],
                'values':     [sheet_name, 1, 1, len(df), 1],
                'marker':     {'type': 'none'},
                'line':       {'width': 1.5},
            })

            chart.set_x_axis({
                'name': 'Distance (ft)',
                'major_unit': 10,
                'major_gridlines': {'visible': True},
            })
            chart.set_y_axis({
                'name': 'Elevation (ft)',
                'major_gridlines': {'visible': True},
            })

            chart.set_title({'name': f'Elevation Profile - {feature_name}'})
            chart.set_size({'width': 720, 'height': 576})
            worksheet.insert_chart('D2', chart)

        writer.close()

        return {self.OUTPUT_EXCEL: output_excel}

    def get_elevation_at_point(self, dem_layer, point, no_data_value):
        elevation = dem_layer.dataProvider().sample(point, 1)[0]
        return None if elevation == no_data_value else elevation

    def clean_sheet_name(self, name):
        invalid_chars = [':', '/', '\\', '?', '*', '[', ']']
        cleaned_name = ''.join(c for c in name if c not in invalid_chars)
        return cleaned_name[:31]

    def name(self):
        return 'generateprofileswithtemplots'

    def displayName(self):
        return 'Generate Profiles from Lines with XY Scatter Plots'

    def group(self):
        return 'Custom Tools'

    def groupId(self):
        return 'customtools'

    def shortHelpString(self):
        return "Generates elevation profiles from a MultiLineString layer using a DEM and creates XY scatter plots with lines. Uses a specified field for naming sheets and plots, sorted by the name field. Samples elevation every 1 foot along the line."

    def createInstance(self):
        return ProfileGeneratorAlgorithm()

class ProfileGeneratorProvider(QgsProcessingProvider):
    def loadAlgorithms(self, *args, **kwargs):
        self.addAlgorithm(ProfileGeneratorAlgorithm())

    def id(self):
        return 'profilegeneratorwithplots'

    def name(self):
        return 'Profile Generator with XY Scatter Plots'

    def icon(self):
        return QgsProcessingProvider.icon(self)

def classFactory(iface):
    return ProfileGeneratorProvider()