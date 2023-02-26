from pyproj import Transformer

def bbox_diffs(search_radius):
    # Tel Aviv Center
    center_x, center_y = 34.75, 32.05
    crs = f'+proj=tmerc +lat_0={center_y} +lon_0={center_x} +k_0=1 +x_0=0 +y_0=0 +ellps=WGS84 +units=m +no_defs'
    transformer = Transformer.from_crs('EPSG:4326', crs, always_xy=True)
    inv_transformer = Transformer.from_crs(crs, 'EPSG:4326', always_xy=True)
    diff_x, diff_y = inv_transformer.transform(search_radius, search_radius)
    diff_x -= center_x
    diff_y -= center_y
    return diff_x, diff_y
