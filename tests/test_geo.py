from buscore.geo import RouteGeometry

# прямая «улица» на север от точки в Алматы, ~1.1 км
POLY = [[43.2380, 76.9450], [43.2480, 76.9450]]

def test_distance_to_route_center_is_zero():
    g = RouteGeometry(POLY)
    assert g.distance_m(43.2430, 76.9450) < 5

def test_distance_to_route_offset_200m():
    g = RouteGeometry(POLY)
    # сдвиг по долготе ~0.00246° ≈ 200 м на этой широте
    d = g.distance_m(43.2430, 76.9450 + 0.00246)
    assert 180 < d < 220

def test_point_in_circle():
    g = RouteGeometry(POLY)
    assert g.haversine_m(43.2380, 76.9450, 43.2380, 76.9455) < 60
