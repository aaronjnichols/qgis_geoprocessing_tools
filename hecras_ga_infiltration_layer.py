from qgis.PyQt.QtCore import QCoreApplication, QVariant
from qgis.core import (QgsProcessing, QgsFeatureSink, QgsProcessingAlgorithm,
                       QgsProcessingParameterFeatureSource, QgsProcessingParameterFeatureSink,
                       QgsProcessingParameterField, QgsProcessingParameterFileDestination,
                       QgsVectorLayer, QgsField, QgsFeature, QgsGeometry, QgsProcessingUtils,
                       QgsProcessingException)
import processing
import csv

class GreenAmptInfiltrationAlgorithm(QgsProcessingAlgorithm):
    INPUT_SOILS = 'INPUT_SOILS'
    INPUT_LANDUSE = 'INPUT_LANDUSE'
    OUTPUT = 'OUTPUT'
    OUTPUT_CSV = 'OUTPUT_CSV'
    
    # Field parameters
    SOIL_ID_FIELD = 'SOIL_ID_FIELD'
    WILTING_POINT_FIELD = 'WILTING_POINT_FIELD'
    FIELD_CAPACITY_FIELD = 'FIELD_CAPACITY_FIELD'
    SATURATED_CONTENT_FIELD = 'SATURATED_CONTENT_FIELD'
    CAPILLARY_SUCTION_FIELD = 'CAPILLARY_SUCTION_FIELD'
    HYDRAULIC_CONDUCTIVITY_FIELD = 'HYDRAULIC_CONDUCTIVITY_FIELD'
    ROCK_OUTCROP_FIELD = 'ROCK_OUTCROP_FIELD'
    LANDUSE_TYPE_FIELD = 'LANDUSE_TYPE_FIELD'
    INITIAL_SATURATION_FIELD = 'INITIAL_SATURATION_FIELD'
    PERCENT_IMPERVIOUS_FIELD = 'PERCENT_IMPERVIOUS_FIELD'

    def initAlgorithm(self, config=None):
        self.addParameter(QgsProcessingParameterFeatureSource(self.INPUT_SOILS, 'Input soils layer', [QgsProcessing.TypeVectorAnyGeometry]))
        self.addParameter(QgsProcessingParameterFeatureSource(self.INPUT_LANDUSE, 'Input land use layer', [QgsProcessing.TypeVectorAnyGeometry]))
        
        # Add field selection parameters for soils layer
        self.addParameter(QgsProcessingParameterField(self.SOIL_ID_FIELD, 'Soil ID field', parentLayerParameterName=self.INPUT_SOILS))
        self.addParameter(QgsProcessingParameterField(self.WILTING_POINT_FIELD, 'Wilting Point field', parentLayerParameterName=self.INPUT_SOILS))
        self.addParameter(QgsProcessingParameterField(self.FIELD_CAPACITY_FIELD, 'Field Capacity field', parentLayerParameterName=self.INPUT_SOILS))
        self.addParameter(QgsProcessingParameterField(self.SATURATED_CONTENT_FIELD, 'Saturated Content field', parentLayerParameterName=self.INPUT_SOILS))
        self.addParameter(QgsProcessingParameterField(self.CAPILLARY_SUCTION_FIELD, 'Capillary Suction Head field', parentLayerParameterName=self.INPUT_SOILS))
        self.addParameter(QgsProcessingParameterField(self.HYDRAULIC_CONDUCTIVITY_FIELD, 'Hydraulic Conductivity field', parentLayerParameterName=self.INPUT_SOILS))
        self.addParameter(QgsProcessingParameterField(self.ROCK_OUTCROP_FIELD, 'Rock Outcrop field', parentLayerParameterName=self.INPUT_SOILS))
        
        # Add field selection parameters for land use layer
        self.addParameter(QgsProcessingParameterField(self.LANDUSE_TYPE_FIELD, 'Land Use Type field', parentLayerParameterName=self.INPUT_LANDUSE))
        self.addParameter(QgsProcessingParameterField(self.INITIAL_SATURATION_FIELD, 'Initial Saturation field', parentLayerParameterName=self.INPUT_LANDUSE))
        self.addParameter(QgsProcessingParameterField(self.PERCENT_IMPERVIOUS_FIELD, 'Percent Impervious field', parentLayerParameterName=self.INPUT_LANDUSE))
        
        self.addParameter(QgsProcessingParameterFeatureSink(self.OUTPUT, 'Output Layer'))
        self.addParameter(QgsProcessingParameterFileDestination(self.OUTPUT_CSV, 'Output CSV File', 'CSV files (*.csv)'))

    def processAlgorithm(self, parameters, context, feedback):
        try:
            soils_layer = self.parameterAsSource(parameters, self.INPUT_SOILS, context)
            landuse_layer = self.parameterAsSource(parameters, self.INPUT_LANDUSE, context)

            # Get field names from parameters
            soil_id_field = self.parameterAsString(parameters, self.SOIL_ID_FIELD, context)
            wilting_point_field = self.parameterAsString(parameters, self.WILTING_POINT_FIELD, context)
            field_capacity_field = self.parameterAsString(parameters, self.FIELD_CAPACITY_FIELD, context)
            saturated_content_field = self.parameterAsString(parameters, self.SATURATED_CONTENT_FIELD, context)
            capillary_suction_field = self.parameterAsString(parameters, self.CAPILLARY_SUCTION_FIELD, context)
            hydraulic_conductivity_field = self.parameterAsString(parameters, self.HYDRAULIC_CONDUCTIVITY_FIELD, context)
            rock_outcrop_field = self.parameterAsString(parameters, self.ROCK_OUTCROP_FIELD, context)
            landuse_type_field = self.parameterAsString(parameters, self.LANDUSE_TYPE_FIELD, context)
            initial_saturation_field = self.parameterAsString(parameters, self.INITIAL_SATURATION_FIELD, context)
            percent_impervious_field = self.parameterAsString(parameters, self.PERCENT_IMPERVIOUS_FIELD, context)

            # Perform union
            feedback.pushInfo('Performing union of soils and land use layers...')
            union_result = processing.run("native:union", {
                'INPUT': parameters[self.INPUT_SOILS],
                'OVERLAY': parameters[self.INPUT_LANDUSE],
                'OUTPUT': 'memory:'
            }, context=context, feedback=feedback)
            union_layer = union_result['OUTPUT']

            # Add new fields
            union_layer.dataProvider().addAttributes([
                QgsField("IniWatCont", QVariant.Double),
                QgsField("TotalImprv", QVariant.Double),
                QgsField("LU_Soil_ID", QVariant.String)
            ])
            union_layer.updateFields()

            # Get field indexes
            ini_wat_cont_idx = union_layer.fields().indexFromName("IniWatCont")
            total_imprv_idx = union_layer.fields().indexFromName("TotalImprv")
            lu_soil_id_idx = union_layer.fields().indexFromName("LU_Soil_ID")

            # Calculate new field values
            feedback.pushInfo('Calculating field values...')
            union_layer.startEditing()
            feature_count = union_layer.featureCount()
            for current, feature in enumerate(union_layer.getFeatures()):
                if feedback.isCanceled():
                    break

                # Calculate IniWatCont
                initial_saturation = feature[initial_saturation_field]
                if initial_saturation == "dry":
                    ini_wat_cont = feature[wilting_point_field]
                elif initial_saturation == "saturated":
                    ini_wat_cont = feature[saturated_content_field]
                else:  # "normal"
                    ini_wat_cont = feature[field_capacity_field]

                # Calculate TotalImprv
                percent_impervious = feature[percent_impervious_field]
                rock_outcrop = feature[rock_outcrop_field]
                
                # Handle null values
                if percent_impervious is None:
                    percent_impervious = 0
                    feedback.pushInfo(f"Null value found in {percent_impervious_field} for feature {feature.id()}. Using 0.")
                if rock_outcrop is None:
                    rock_outcrop = 0
                    feedback.pushInfo(f"Null value found in {rock_outcrop_field} for feature {feature.id()}. Using 0.")
                
                total_imprv = min(percent_impervious + rock_outcrop, 100)

                # Calculate LU_Soil_ID
                lu_soil_id = f"{feature[landuse_type_field]}: {feature[soil_id_field]}"

                # Update feature
                union_layer.changeAttributeValue(feature.id(), ini_wat_cont_idx, ini_wat_cont)
                union_layer.changeAttributeValue(feature.id(), total_imprv_idx, total_imprv)
                union_layer.changeAttributeValue(feature.id(), lu_soil_id_idx, lu_soil_id)

                # Update progress
                feedback.setProgress(int(current / feature_count * 100))

            union_layer.commitChanges()

            # Save output layer
            (sink, dest_id) = self.parameterAsSink(parameters, self.OUTPUT, context,
                                                   union_layer.fields(), union_layer.wkbType(), union_layer.crs())

            if sink is None:
                raise QgsProcessingException(self.invalidSinkError(parameters, self.OUTPUT))

            feedback.pushInfo('Saving output layer...')
            features = union_layer.getFeatures()
            for feature in features:
                if feedback.isCanceled():
                    break
                sink.addFeature(feature, QgsFeatureSink.FastInsert)

            # Create CSV output
            feedback.pushInfo('Creating CSV output...')
            csv_output_path = self.parameterAsFileOutput(parameters, self.OUTPUT_CSV, context)
            unique_rows = {}
            for feature in union_layer.getFeatures():
                if feedback.isCanceled():
                    break
                lu_soil_id = feature["LU_Soil_ID"]
                if lu_soil_id not in unique_rows:
                    unique_rows[lu_soil_id] = {
                        "LU_Soil_ID": lu_soil_id,
                        "IniWatCont": feature["IniWatCont"],
                        "Hydraulic Conductivity": feature[hydraulic_conductivity_field],
                        "Saturated Content": feature[saturated_content_field],
                        "Capillary Suction": feature[capillary_suction_field]
                    }

            # Write CSV
            with open(csv_output_path, 'w', newline='') as csvfile:
                fieldnames = ["LU_Soil_ID", "IniWatCont", "Hydraulic Conductivity", "Saturated Content", "Capillary Suction"]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for row in unique_rows.values():
                    writer.writerow(row)

            return {self.OUTPUT: dest_id, self.OUTPUT_CSV: csv_output_path}

        except Exception as e:
            feedback.reportError(f'An error occurred: {str(e)}')
            raise e

    def name(self):
        return 'greenamptinfiltration'

    def displayName(self):
        return 'Green and Ampt Infiltration Layer'

    def group(self):
        return 'Hydrology'

    def groupId(self):
        return 'hydrology'

    def createInstance(self):
        return GreenAmptInfiltrationAlgorithm()

    def shortHelpString(self):
        return "This algorithm creates a Green and Ampt infiltration layer for a HEC-RAS model using soil and land use data."

    def helpUrl(self):
        return "https://your_help_url.com"

    def tr(self, string):
        return QCoreApplication.translate('Processing', string)