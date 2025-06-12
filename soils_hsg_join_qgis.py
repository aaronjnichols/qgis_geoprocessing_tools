# path/to/your/qgis/processing/scripts/join_soil_hsg.py
import csv
from qgis.PyQt.QtCore import QCoreApplication, QVariant
from qgis.core import (QgsProcessing, QgsProcessingAlgorithm,
                       QgsProcessingParameterVectorLayer,
                       QgsProcessingParameterFile,
                       QgsProcessingParameterNumber,
                       QgsProcessingParameterField,
                       QgsProcessingParameterString,
                       QgsProcessingParameterFeatureSink,
                       QgsProcessingException,
                       QgsFeature, QgsFields, QgsField,
                       QgsVectorLayer,
                       QgsWkbTypes,
                       QgsFeatureSink)
from qgis.utils import iface # Optional: for messages

class JoinSoilHsgAlgorithm(QgsProcessingAlgorithm):
    """
    Joins Hydrologic Soil Group (HSG) data from a delimited text file
    to a soil polygon vector layer based on a common key field.
    """
    # Define parameter names as constants
    INPUT_LAYER = 'INPUT_LAYER'
    INPUT_LAYER_KEY_FIELD = 'INPUT_LAYER_KEY_FIELD'
    TEXT_FILE = 'TEXT_FILE'
    TEXT_FILE_KEY_COL = 'TEXT_FILE_KEY_COL'
    TEXT_FILE_HSG_COL = 'TEXT_FILE_HSG_COL'
    OUTPUT_HSG_FIELD = 'OUTPUT_HSG_FIELD'
    OUTPUT = 'OUTPUT'

    def tr(self, string):
        """
        Returns a translatable string with the self.tr() function.
        """
        return QCoreApplication.translate('Processing', string)

    def createInstance(self):
        return JoinSoilHsgAlgorithm()

    def name(self):
        """
        Returns the unique algorithm name.
        """
        return 'joinsoilhsg'

    def displayName(self):
        """
        Returns the translated algorithm name.
        """
        return self.tr('Join Soil HSG from Text')

    def group(self):
        """
        Returns the name of the group this algorithm belongs to.
        """
        return self.tr('Vector Table')

    def groupId(self):
        """
        Returns the unique ID of the group.
        """
        return 'vectortable'

    def shortHelpString(self):
        """
        Returns a short description of the algorithm.
        """
        return self.tr('Joins HSG data from a delimited text file (like muaggatt.txt) to a soil polygon layer based on a common key (e.g., MUSYM).')

    def initAlgorithm(self, config=None):
        """
        Defines the input and output parameters of the algorithm.
        """
        # Input Vector Layer (Soil Polygons)
        self.addParameter(
            QgsProcessingParameterVectorLayer(
                self.INPUT_LAYER,
                self.tr('Input Soil Polygon Layer'),
                [QgsProcessing.TypeVectorPolygon], # Accept only polygon layers
                # defaultValue='path/to/your/default/soilmu_a_aoi.shp' # Optional default
            )
        )

        # Key Field in the Input Layer
        self.addParameter(
            QgsProcessingParameterField(
                self.INPUT_LAYER_KEY_FIELD,
                self.tr('Key Field in Soil Layer (e.g., MUSYM)'),
                parentLayerParameterName=self.INPUT_LAYER,
                type=QgsProcessingParameterField.Any, # Allow any field type initially
                # fieldType=QgsProcessingParameterField.String, # Ideally String, but check later
                defaultValue='MUSYM'
            )
        )

        # Input Text File (muaggatt.txt)
        self.addParameter(
            QgsProcessingParameterFile(
                self.TEXT_FILE,
                self.tr('Input Text File (e.g., muaggatt.txt)'),
                behavior=QgsProcessingParameterFile.File,
                fileFilter='Text files (*.txt *.csv)',
                # defaultValue='path/to/your/default/muaggatt.txt' # Optional default
            )
        )

        # Column Index for Key in Text File
        self.addParameter(
            QgsProcessingParameterNumber(
                self.TEXT_FILE_KEY_COL,
                self.tr('Column Index (0-based) for Key in Text File'),
                type=QgsProcessingParameterNumber.Integer,
                defaultValue=0,
                minValue=0
            )
        )

        # Column Index for HSG in Text File
        self.addParameter(
            QgsProcessingParameterNumber(
                self.TEXT_FILE_HSG_COL,
                self.tr('Column Index (0-based) for HSG in Text File'),
                type=QgsProcessingParameterNumber.Integer,
                defaultValue=17,
                minValue=0
            )
        )

        # Name for the new HSG field in the output
        self.addParameter(
            QgsProcessingParameterString(
                self.OUTPUT_HSG_FIELD,
                self.tr('Name for Output HSG Field'),
                defaultValue='HydSoilGrp'
            )
        )

        # Output Layer
        self.addParameter(
            QgsProcessingParameterFeatureSink(
                self.OUTPUT,
                self.tr('Output Layer with HSG'),
                type=QgsProcessing.TypeVectorAnyGeometry # Allow various output formats
            )
        )

    def processAlgorithm(self, parameters, context, feedback):
        """
        The main processing logic.
        """
        # --- Get Parameters ---
        source_layer = self.parameterAsVectorLayer(parameters, self.INPUT_LAYER, context)
        if source_layer is None:
            raise QgsProcessingException(self.invalidSourceError(parameters, self.INPUT_LAYER))

        source_key_field = self.parameterAsString(parameters, self.INPUT_LAYER_KEY_FIELD, context)
        text_file_path = self.parameterAsString(parameters, self.TEXT_FILE, context)
        text_key_col_idx = self.parameterAsInt(parameters, self.TEXT_FILE_KEY_COL, context)
        text_hsg_col_idx = self.parameterAsInt(parameters, self.TEXT_FILE_HSG_COL, context)
        output_hsg_field_name = self.parameterAsString(parameters, self.OUTPUT_HSG_FIELD, context)

        # Validate source key field exists
        source_field_index = source_layer.fields().lookupField(source_key_field)
        if source_field_index == -1:
             raise QgsProcessingException(f"Field '{source_key_field}' not found in layer '{source_layer.name()}'.")


        # --- Read Text File into Lookup Dictionary ---
        feedback.pushInfo(f"Reading text file: {text_file_path}...")
        hsg_lookup = {}
        max_col_index = max(text_key_col_idx, text_hsg_col_idx)
        try:
            with open(text_file_path, mode='r', encoding='utf-8', newline='') as csvfile:
                # Handle pipe delimiter and quoted fields
                reader = csv.reader(csvfile, delimiter='|', quotechar='"', quoting=csv.QUOTE_ALL)
                line_num = 0
                for row in reader:
                    line_num += 1
                    if len(row) > max_col_index:
                        key_val = row[text_key_col_idx].strip()
                        hsg_val = row[text_hsg_col_idx].strip()
                        if key_val: # Only add if key is not empty
                            hsg_lookup[key_val] = hsg_val
                    else:
                        feedback.pushWarning(f"Skipping line {line_num} in text file: Not enough columns (expected at least {max_col_index + 1}, found {len(row)}).")

        except FileNotFoundError:
            raise QgsProcessingException(f"Error: Text file not found at {text_file_path}")
        except Exception as e:
            raise QgsProcessingException(f"Error reading text file: {e}")

        if not hsg_lookup:
             feedback.pushWarning("Warning: HSG lookup table from text file is empty. No data will be joined.")
        else:
            feedback.pushInfo(f"Successfully read {len(hsg_lookup)} unique keys from text file.")

        # --- Prepare Output ---
        source_fields = source_layer.fields()
        sink_fields = QgsFields(source_fields) # Copy original fields

        # Add the new HSG field
        if sink_fields.lookupField(output_hsg_field_name) != -1:
            feedback.pushWarning(f"Field '{output_hsg_field_name}' already exists. Its values may be overwritten.")
            # Or raise QgsProcessingException if overwriting is not desired
        else:
            sink_fields.append(QgsField(output_hsg_field_name, QVariant.String))

        # Get the output sink object
        (sink, dest_id) = self.parameterAsSink(
            parameters,
            self.OUTPUT,
            context,
            sink_fields,
            source_layer.wkbType(), # Use same geometry type as input
            source_layer.sourceCrs() # Use same CRS as input
        )
        if sink is None:
            raise QgsProcessingException(self.invalidSinkError(parameters, self.OUTPUT))

        # --- Process Features ---
        feedback.pushInfo("Starting feature join...")
        total = 100.0 / source_layer.featureCount() if source_layer.featureCount() else 0
        processed_count = 0
        merged_count = 0

        source_features = source_layer.getFeatures()
        for current, source_feat in enumerate(source_features):
            # Check for cancellation
            if feedback.isCanceled():
                break

            # Create new feature and set geometry
            sink_feat = QgsFeature(sink_fields)
            sink_feat.setGeometry(source_feat.geometry())

            # Copy attributes from source feature
            for i in range(len(source_fields)):
                sink_feat.setAttribute(i, source_feat.attribute(i))

            # Get the key value from the source feature
            key_value_raw = source_feat.attribute(source_key_field)
            key_value_str = str(key_value_raw).strip() # Convert to string and strip

            # Look up HSG value
            hsg_value = hsg_lookup.get(key_value_str) # Returns None if not found

            # Set the HSG attribute in the sink feature
            sink_feat.setAttribute(sink_fields.lookupField(output_hsg_field_name), hsg_value)

            if hsg_value is not None:
                merged_count += 1

            # Add the feature to the output sink
            sink.addFeature(sink_feat, QgsFeatureSink.FastInsert)
            processed_count += 1

            # Update progress feedback
            feedback.setProgress(int(current * total))

        # --- Final Feedback ---
        if feedback.isCanceled():
            feedback.pushInfo("Processing cancelled.")
        else:
            feedback.pushInfo(f"Processed {processed_count} features.")
            feedback.pushInfo(f"Successfully merged HSG data for {merged_count} features.")
            if processed_count > merged_count:
                 feedback.pushWarning(f"{processed_count - merged_count} features did not have a matching key in the text file.")

        # Return the output layer ID
        return {self.OUTPUT: dest_id}
