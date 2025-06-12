import geopandas as gpd
from shapely.geometry import LineString, Point
import os

# Read the input shapefile
folder_path = r'C:\_Projects\25000326 - Take 5 Aubrey\Drainage Report\GIS\SHP\hechms'
line_layer = os.path.join(folder_path, 'Tc.shp')
gdf = gpd.read_file(line_layer)

def cut(line, distance):
    """Cut a LineString at a specified distance from its start."""
    if distance <= 0.0 or distance >= line.length:
        return [line]
    coords = list(line.coords)
    for i, p in enumerate(coords):
        pd = line.project(Point(p))
        if pd == distance:
            return [
                LineString(coords[:i+1]),
                LineString(coords[i:])
            ]
        if pd > distance:
            prev_p = coords[i-1]
            curr_p = coords[i]
            seg = LineString([prev_p, curr_p])
            prev_dist = line.project(Point(prev_p))
            dist_to_prev = distance - prev_dist
            ratio = dist_to_prev / seg.length
            x = prev_p[0] + (curr_p[0] - prev_p[0]) * ratio
            y = prev_p[1] + (curr_p[1] - prev_p[1]) * ratio
            pt = (x, y)
            first = coords[:i] + [pt]
            second = [pt] + coords[i:]
            return [
                LineString(first),
                LineString(second)
            ]
    return [line]

# Split each feature at 100 units from its start
rows = []
for idx, row in gdf.iterrows():
    geom = row.geometry
    if geom.geom_type == 'MultiLineString':
        for part in geom.geoms:
            segments = cut(part, 100)
            for seg in segments:
                new_row = row.copy()
                new_row.geometry = seg
                rows.append(new_row)
    elif geom.geom_type == 'LineString':
        segments = cut(geom, 100)
        for seg in segments:
            new_row = row.copy()
            new_row.geometry = seg
            rows.append(new_row)
    else:
        # Keep non-line geometries unchanged
        rows.append(row.copy())

# Build a new GeoDataFrame
new_gdf = gpd.GeoDataFrame(rows, columns=gdf.columns, crs=gdf.crs)

# Drop problematic 'fid' column if present
if 'fid' in new_gdf.columns:
    new_gdf = new_gdf.drop(columns=['fid'])

# Add or update length field (units same as CRS)
new_gdf['calc_length'] = new_gdf.geometry.length

# Save to GeoPackage
output_path = os.path.join(folder_path, 'Tc_Split.gpkg')
new_gdf.to_file(output_path, layer='split_lines', driver='GPKG')

print(f"GeoPackage created at: {output_path}")