from qgis.PyQt.QtCore import QCoreApplication
from qgis.core import (
    QgsProcessing, QgsProcessingAlgorithm,
    QgsProcessingParameterFeatureSource, QgsProcessingParameterRasterLayer,
    QgsProcessingParameterNumber, QgsProcessingParameterBoolean,
    QgsProcessingException, QgsVectorLayer, QgsVectorDataProvider,
    QgsField, QgsPointXY, QgsCoordinateTransform, QgsDistanceArea,
    QgsProject, QgsRaster
)
from PyQt5.QtCore import QVariant
import math

class SampleRasterAtLineEndpoints(QgsProcessingAlgorithm):
    INPUT_LINE_LAYER = 'INPUT_LINE_LAYER'
    INPUT_RASTER_LAYER = 'INPUT_RASTER_LAYER'
    INPUT_BAND = 'INPUT_BAND'
    VERT_UNIT_FACTOR = 'VERT_UNIT_FACTOR'
    USE_GEODESIC = 'USE_GEODESIC'
    OUTPUT_LAYER = 'OUTPUT_LAYER'  # just to return the edited layer id

    def tr(self, s): return QCoreApplication.translate('SampleRasterAtLineEndpoints', s)
    def createInstance(self): return SampleRasterAtLineEndpoints()
    def name(self): return 'sampleRasterAtLineEndpoints_inplace'
    def displayName(self): return self.tr('Sample Raster at Line Endpoints')
    def group(self): return self.tr('Custom Scripts')
    def groupId(self): return 'customscripts'

    def initAlgorithm(self, config=None):
        self.addParameter(QgsProcessingParameterFeatureSource(
            self.INPUT_LINE_LAYER, self.tr('Input Line Layer'), [QgsProcessing.TypeVectorLine]))
        self.addParameter(QgsProcessingParameterRasterLayer(
            self.INPUT_RASTER_LAYER, self.tr('Input Raster (DEM)')))
        self.addParameter(QgsProcessingParameterNumber(
            self.INPUT_BAND, self.tr('Raster band to sample'),
            type=QgsProcessingParameterNumber.Integer, defaultValue=1, minValue=1))
        self.addParameter(QgsProcessingParameterNumber(
            self.VERT_UNIT_FACTOR, self.tr('Vertical conversion factor (multiplies sampled values)'),
            type=QgsProcessingParameterNumber.Double, defaultValue=1.0))
        self.addParameter(QgsProcessingParameterBoolean(
            self.USE_GEODESIC, self.tr('Measure length geodesically in meters'), defaultValue=True))

    def processAlgorithm(self, parameters, context, feedback):
        line_layer = self.parameterAsVectorLayer(parameters, self.INPUT_LINE_LAYER, context)
        raster_layer = self.parameterAsRasterLayer(parameters, self.INPUT_RASTER_LAYER, context)
        band = self.parameterAsInt(parameters, self.INPUT_BAND, context)
        vert_factor = self.parameterAsDouble(parameters, self.VERT_UNIT_FACTOR, context)
        use_geodesic = self.parameterAsBoolean(parameters, self.USE_GEODESIC, context)

        if not isinstance(line_layer, QgsVectorLayer) or not line_layer.isValid():
            raise QgsProcessingException(self.invalidSourceError(parameters, self.INPUT_LINE_LAYER))
        if not raster_layer or not raster_layer.isValid():
            raise QgsProcessingException(self.invalidSourceError(parameters, self.INPUT_RASTER_LAYER))

        prov = line_layer.dataProvider()
        caps = prov.capabilities()
        if not (caps & QgsVectorDataProvider.AddAttributes):
            raise QgsProcessingException(self.tr('Layer does not allow adding fields.'))
        if not (caps & QgsVectorDataProvider.ChangeAttributeValues):
            raise QgsProcessingException(self.tr('Layer does not allow changing attribute values.'))

        # Band sanity
        try:
            bc = raster_layer.dataProvider().bandCount()
            if band > bc:
                feedback.pushInfo(self.tr(f'Requested band {band} > raster band count {bc}; using band 1.'))
                band = 1
        except Exception:
            pass

        # Start edit before adding fields
        started_edit = False
        if not line_layer.isEditable():
            if not line_layer.startEditing():
                raise QgsProcessingException(self.tr('Could not start an edit session on the layer.'))
            started_edit = True

        # Ensure fields exist
        for name in ('StartVal','EndVal','Slope','Length'):
            if line_layer.fields().indexFromName(name) == -1:
                if not prov.addAttributes([QgsField(name, QVariant.Double)]):
                    if started_edit: line_layer.rollBack()
                    raise QgsProcessingException(self.tr('Failed to add required fields.'))
        line_layer.updateFields()
        idx_start = line_layer.fields().indexFromName('StartVal')
        idx_end   = line_layer.fields().indexFromName('EndVal')
        idx_slope = line_layer.fields().indexFromName('Slope')
        idx_len   = line_layer.fields().indexFromName('Length')

        # Transform & measurer
        try:
            to_raster_ct = QgsCoordinateTransform(line_layer.crs(), raster_layer.crs(), context.transformContext())
        except Exception as e:
            if started_edit: line_layer.rollBack()
            raise QgsProcessingException(self.tr(f'Failed to build coordinate transform: {e}'))

        dist = QgsDistanceArea()
        dist.setSourceCrs(line_layer.crs(), context.transformContext())
        ellipsoid = QgsProject.instance().ellipsoid() or 'WGS84'
        dist.setEllipsoid(ellipsoid if use_geodesic else 'NONE')

        # Nudge size ~¾ px
        try:
            px = abs(raster_layer.rasterUnitsPerPixelX())
            py = abs(raster_layer.rasterUnitsPerPixelY())
            nudge_step = 0.75 * max(px, py)
        except Exception:
            nudge_step = None

        line_layer.beginEditCommand(self.tr('Sample DEM at endpoints'))

        total = line_layer.featureCount() or 0
        processed = 0

        for feat in line_layer.getFeatures():
            if feedback.isCanceled(): break

            geom = feat.geometry()
            if not geom or geom.isEmpty():
                self._apply_attrs(line_layer, feat, idx_start, idx_end, idx_slope, idx_len, None, None, None, None, feedback)
                processed += 1; feedback.setProgress(100.0 * processed / max(total,1)); continue

            try:
                if not geom.isGeosValid(): geom = geom.makeValid()
            except Exception: pass

            start_pt, end_pt = self._robust_endpoints(geom)
            if start_pt is None or end_pt is None:
                self._apply_attrs(line_layer, feat, idx_start, idx_end, idx_slope, idx_len, None, None, None, None, feedback)
                processed += 1; feedback.setProgress(100.0 * processed / max(total,1)); continue

            try:
                s_r = to_raster_ct.transform(QgsPointXY(start_pt))
                e_r = to_raster_ct.transform(QgsPointXY(end_pt))
            except Exception:
                s_r, e_r = None, None

            start_val = self._sample_value(raster_layer, s_r, band)
            if start_val is None and s_r and e_r and nudge_step:
                s_r = self._nudge_toward(s_r, e_r, nudge_step)
                start_val = self._sample_value(raster_layer, s_r, band)

            end_val = self._sample_value(raster_layer, e_r, band)
            if end_val is None and s_r and e_r and nudge_step:
                e_r = self._nudge_toward(e_r, s_r, nudge_step)
                end_val = self._sample_value(raster_layer, e_r, band)

            if start_val is not None: start_val *= vert_factor
            if end_val   is not None: end_val   *= vert_factor

            try:
                length = dist.measureLength(geom)
            except Exception:
                length = None

            slope = None
            if start_val is not None and end_val is not None and length and length > 0:
                slope = (start_val - end_val) / float(length)

            self._apply_attrs(line_layer, feat, idx_start, idx_end, idx_slope, idx_len,
                              start_val, end_val, slope, length, feedback)

            if processed < 3:
                feedback.pushInfo(f"Feat {feat.id()} | s={start_val}, e={end_val}, L={length}, slope={slope}")

            processed += 1
            if total: feedback.setProgress(100.0 * processed / total)

        line_layer.endEditCommand()

        if started_edit:
            if not line_layer.commitChanges():
                line_layer.rollBack()
                raise QgsProcessingException(self.tr('Commit failed; changes were rolled back.'))

        # Force UI refresh in some cases (GPKG+OneDrive can be sluggish)
        try:
            line_layer.triggerRepaint()
        except Exception:
            pass

        return {self.OUTPUT_LAYER: line_layer.id()}

    # ---------- helpers ----------
    def _apply_attrs(self, layer, feat, i_s, i_e, i_m, i_l, sv, ev, sl, ln, feedback):
        """Write values robustly: per-field change; if that fails, fall back to updateFeature()."""
        ok = True
        try:
            ok &= layer.changeAttributeValue(feat.id(), i_s, sv)
            ok &= layer.changeAttributeValue(feat.id(), i_e, ev)
            ok &= layer.changeAttributeValue(feat.id(), i_m, sl)
            ok &= layer.changeAttributeValue(feat.id(), i_l, ln)
        except Exception:
            ok = False

        if not ok:
            # Fallback path
            try:
                f2 = feat
                # copy current attrs, set only our fields
                attrs = f2.attributes()
                if i_s >= 0: attrs[i_s] = sv
                if i_e >= 0: attrs[i_e] = ev
                if i_m >= 0: attrs[i_m] = sl
                if i_l >= 0: attrs[i_l] = ln
                f2.setAttributes(attrs)
                ok2 = layer.updateFeature(f2)
                if not ok2:
                    feedback.pushInfo(f"Write failed on FID {feat.id()} (both paths).")
            except Exception:
                feedback.pushInfo(f"Write exception on FID {feat.id()}.")

    def _robust_endpoints(self, geom):
        first = None; last = None; count = 0
        try:
            for v in geom.vertices():
                pt = QgsPointXY(v)
                if first is None: first = pt
                last = pt
                count += 1
        except Exception:
            return None, None
        if count >= 2 and first and last and (first.x()!=last.x() or first.y()!=last.y()):
            return first, last
        return None, None

    def _nudge_toward(self, p_from, p_to, d):
        try:
            vx = p_to.x() - p_from.x()
            vy = p_to.y() - p_from.y()
            n = math.hypot(vx, vy)
            if n == 0: return p_from
            return QgsPointXY(p_from.x() + (vx/n)*d, p_from.y() + (vy/n)*d)
        except Exception:
            return p_from

    def _sample_value(self, raster_layer, point_xy, band):
        try:
            if point_xy is None: return None
            provider = raster_layer.dataProvider()
            ident = provider.identify(point_xy, QgsRaster.IdentifyFormatValue)
            if not ident.isValid(): return None
            res = ident.results()
            if not res: return None
            val = res.get(band, res.get(1, None))
            if val is None: return None
            try:
                if provider.sourceHasNoDataValue(band):
                    ndv = provider.sourceNoDataValue(band)
                    if val == ndv: return None
            except Exception:
                pass
            if isinstance(val, float) and math.isnan(val): return None
            return float(val)
        except Exception:
            return None

    def shortHelpString(self):
        return self.tr(
            "Edits the input line layer in place and writes StartVal/EndVal/Slope/Length. "
            "Uses per-field updates (safer on GPKG/OGR) with a fallback via updateFeature()."
        )
