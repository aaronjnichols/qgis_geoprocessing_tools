# -*- coding: utf-8 -*-
"""
Calculate Subbasin Curve Numbers

This script calculates area-weighted Curve Numbers (CN) for subbasins based on soils and land use data.
It updates the subbasins layer with a new "CN" field and exports a summary spreadsheet.

Author: Your Name
Date: YYYY-MM-DD
"""

from qgis.PyQt.QtCore import QCoreApplication, QVariant
from qgis.PyQt.QtGui import QIcon
from qgis.core import (
    QgsProcessing,
    QgsProcessingAlgorithm,
    QgsProcessingParameterVectorLayer,
    QgsProcessingParameterField,
    QgsProcessingParameterFileDestination,
    QgsProcessingParameterMultipleLayers,
    QgsProcessingParameterFolderDestination,
    QgsCoordinateReferenceSystem,
    QgsProject,
    QgsVectorLayer,
    QgsField,
    QgsFeature,
    QgsGeometry,
    QgsExpression,
    QgsExpressionContext,
    QgsExpressionContextUtils,
    QgsWkbTypes,
    QgsVectorFileWriter,
    QgsProcessingException,
    QgsCoordinateTransformContext
)
from qgis import processing
import os
import xlsxwriter

class CalculateSubbasinCN(QgsProcessingAlgorithm):
    """Calculate Subbasin Curve Numbers."""

    # Define unique algorithm name
    def name(self):
        return 'calculate_subbasin_cn'

    # Define display name
    def displayName(self):
        return 'Calculate Subbasin Curve Numbers'

    # Define group
    def group(self):
        return 'Custom Tools'

    # Define group ID
    def groupId(self):
        return 'customtools'

    # Define short help string
    def shortHelpString(self):
        return """
        Calculates area-weighted Curve Numbers (CN) for subbasins based on soils and land use data.
        Updates the Subbasins layer with a new "CN" field and exports a summary spreadsheet.
        """

    # Initialize algorithm parameters
    def initAlgorithm(self, config=None):
        # Input layers
        self.addParameter(
            QgsProcessingParameterVectorLayer(
                'SUBBASINS',
                'Subbasins Vector Layer',
                [QgsProcessing.TypeVectorAnyGeometry],
                False
            )
        )
        
        self.addParameter(
            QgsProcessingParameterVectorLayer(
                'SOILS',
                'Soils Vector Layer',
                [QgsProcessing.TypeVectorAnyGeometry],
                False
            )
        )
        
        self.addParameter(
            QgsProcessingParameterVectorLayer(
                'LANDUSE',
                'Land Use Vector Layer',
                [QgsProcessing.TypeVectorAnyGeometry],
                False
            )
        )
        
        # Fields from layers
        self.addParameter(
            QgsProcessingParameterField(
                name='SUBBASINS_NAME_FIELD',
                description='Subbasins Name Field',
                parentLayerParameterName='SUBBASINS',
                type=QgsProcessingParameterField.String,
                allowMultiple=False
            )
        )
        
        self.addParameter(
            QgsProcessingParameterField(
                name='SOILS_HSG_FIELD',
                description='Soils Hydrologic Soil Group Field',
                parentLayerParameterName='SOILS',
                type=QgsProcessingParameterField.String,
                allowMultiple=False
            )
        )
        
        self.addParameter(
            QgsProcessingParameterField(
                name='SOILS_ID_FIELD',
                description='Soils ID Field',
                parentLayerParameterName='SOILS',
                type=QgsProcessingParameterField.String,
                allowMultiple=False
            )
        )
        
        self.addParameter(
            QgsProcessingParameterField(
                name='LANDUSE_NAME_FIELD',
                description='Land Use Name Field',
                parentLayerParameterName='LANDUSE',
                type=QgsProcessingParameterField.String,
                allowMultiple=False
            )
        )
        
        self.addParameter(
            QgsProcessingParameterField(
                name='CN_A_FIELD',
                description='Curve Number for HSG Type A Field',
                parentLayerParameterName='LANDUSE',
                type=QgsProcessingParameterField.Numeric,
                allowMultiple=False
            )
        )
        
        self.addParameter(
            QgsProcessingParameterField(
                name='CN_B_FIELD',
                description='Curve Number for HSG Type B Field',
                parentLayerParameterName='LANDUSE',
                type=QgsProcessingParameterField.Numeric,
                allowMultiple=False
            )
        )
        
        self.addParameter(
            QgsProcessingParameterField(
                name='CN_C_FIELD',
                description='Curve Number for HSG Type C Field',
                parentLayerParameterName='LANDUSE',
                type=QgsProcessingParameterField.Numeric,
                allowMultiple=False
            )
        )
        
        self.addParameter(
            QgsProcessingParameterField(
                name='CN_D_FIELD',
                description='Curve Number for HSG Type D Field',
                parentLayerParameterName='LANDUSE',
                type=QgsProcessingParameterField.Numeric,
                allowMultiple=False
            )
        )
        
        # Output parameters
        self.addParameter(
            QgsProcessingParameterFileDestination(
                name='OUTPUT_UNION',
                description='Output Unioned Vector Layer',
                fileFilter='GPKG Files (*.gpkg);;Shapefiles (*.shp)',
                defaultValue=None
            )
        )
        
        self.addParameter(
            QgsProcessingParameterFolderDestination(
                name='OUTPUT_FOLDER',
                description='Output Folder for Summary Spreadsheet',
                defaultValue=None
            )
        )

    # The main algorithm logic
    def processAlgorithm(self, parameters, context, feedback):
        # Get input layers
        subbasins_layer = self.parameterAsVectorLayer(parameters, 'SUBBASINS', context)
        soils_layer = self.parameterAsVectorLayer(parameters, 'SOILS', context)
        landuse_layer = self.parameterAsVectorLayer(parameters, 'LANDUSE', context)
        
        # Get field names from parameters
        subbasins_name_field = self.parameterAsString(parameters, 'SUBBASINS_NAME_FIELD', context)
        soils_hsg_field = self.parameterAsString(parameters, 'SOILS_HSG_FIELD', context)
        cn_a_field = self.parameterAsString(parameters, 'CN_A_FIELD', context)
        cn_b_field = self.parameterAsString(parameters, 'CN_B_FIELD', context)
        cn_c_field = self.parameterAsString(parameters, 'CN_C_FIELD', context)
        cn_d_field = self.parameterAsString(parameters, 'CN_D_FIELD', context)
        
        # Step 1: Union soils and land use
        feedback.setProgressText("Unioning soils and land use...")
        soils_landuse = processing.run("native:union", {
            'INPUT': soils_layer,
            'OVERLAY': landuse_layer,
            'OUTPUT': 'memory:'
        }, context=context, feedback=feedback)['OUTPUT']
        
        # Step 2: Union with subbasins
        feedback.setProgressText("Unioning with subbasins...")
        unioned = processing.run("native:union", {
            'INPUT': soils_landuse,
            'OVERLAY': subbasins_layer,
            'OUTPUT': 'memory:'
        }, context=context, feedback=feedback)['OUTPUT']
        
        # Step 3: Remove features with NULL/blank subbasin names
        feedback.setProgressText("Removing features outside subbasins...")
        unioned.startEditing()
        features_to_delete = []
        for feature in unioned.getFeatures():
            subbasin_name = feature[subbasins_name_field]
            if subbasin_name is None or str(subbasin_name).strip() == '':
                features_to_delete.append(feature.id())
        unioned.deleteFeatures(features_to_delete)
        unioned.commitChanges()
        
        # Step 4: Calculate CN based on HSG
        feedback.setProgressText("Calculating CN values...")
        # Add CN field if it doesn't exist
        if 'CN' not in [field.name() for field in unioned.fields()]:
            unioned.dataProvider().addAttributes([QgsField('CN', QVariant.Double)])
            unioned.updateFields()
        
        cn_idx = unioned.fields().indexFromName('CN')
        unioned.startEditing()
        for feature in unioned.getFeatures():
            hsg = feature[soils_hsg_field]
            cn_value = None
            if hsg == 'A':
                cn_value = feature[cn_a_field]
            elif hsg == 'B':
                cn_value = feature[cn_b_field]
            elif hsg == 'C':
                cn_value = feature[cn_c_field]
            elif hsg == 'D':
                cn_value = feature[cn_d_field]
            
            if cn_value is not None:
                unioned.changeAttributeValue(feature.id(), cn_idx, float(cn_value))
        unioned.commitChanges()
        
        # Step 5: Calculate areas in acres
        feedback.setProgressText("Calculating areas...")
        unioned.startEditing()
        # Add area field if it doesn't exist
        if 'area_ac' not in [field.name() for field in unioned.fields()]:
            unioned.dataProvider().addAttributes([QgsField('area_ac', QVariant.Double)])
            unioned.updateFields()
        
        area_idx = unioned.fields().indexFromName('area_ac')
        for feature in unioned.getFeatures():
            # Convert square meters to acres
            area_acres = feature.geometry().area() * 0.000247105
            unioned.changeAttributeValue(feature.id(), area_idx, area_acres)
        unioned.commitChanges()
        
        # Step 6: Calculate weighted CN per subbasin
        feedback.setProgressText("Calculating weighted CNs...")
        subbasin_data = {}
        for feature in unioned.getFeatures():
            subbasin_name = feature[subbasins_name_field]
            cn = feature['CN']
            area = feature['area_ac']
            
            if cn is not None and area is not None:
                if subbasin_name not in subbasin_data:
                    subbasin_data[subbasin_name] = {'cn_area': 0, 'total_area': 0}
                subbasin_data[subbasin_name]['cn_area'] += float(cn) * float(area)
                subbasin_data[subbasin_name]['total_area'] += float(area)
        
        # Update subbasins layer with weighted CN
        if 'CN' not in [field.name() for field in subbasins_layer.fields()]:
            subbasins_layer.dataProvider().addAttributes([QgsField('CN', QVariant.Double)])
            subbasins_layer.updateFields()
        
        cn_idx = subbasins_layer.fields().indexFromName('CN')
        subbasins_layer.startEditing()
        for feature in subbasins_layer.getFeatures():
            subbasin_name = feature[subbasins_name_field]
            if subbasin_name in subbasin_data:
                data = subbasin_data[subbasin_name]
                if data['total_area'] > 0:
                    weighted_cn = data['cn_area'] / data['total_area']
                    subbasins_layer.changeAttributeValue(feature.id(), cn_idx, weighted_cn)
        subbasins_layer.commitChanges()
        
        return {'UNIONED_LAYER': parameters['OUTPUT_UNION']}

    # Define outputs
    def createInstance(self):
        return CalculateSubbasinCN()


