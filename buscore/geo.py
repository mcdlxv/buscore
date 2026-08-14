import math
from shapely.geometry import LineString, Point

class RouteGeometry:
    """Полилиния маршрута в локальной метрической проекции.

    Равнопромежуточная проекция относительно первой точки маршрута:
    x = (lon - lon0) * 111320 * cos(lat0), y = (lat - lat0) * 110540.
    Для города (< 50 км) погрешность незначима.
    """

    def __init__(self, polyline: list[list[float]]):
        self.lat0, self.lon0 = polyline[0][0], polyline[0][1]
        self._kx = 111_320 * math.cos(math.radians(self.lat0))
        self._ky = 110_540
        self.line = LineString([self._xy(lat, lon) for lat, lon in polyline])

    def _xy(self, lat: float, lon: float) -> tuple[float, float]:
        return ((lon - self.lon0) * self._kx, (lat - self.lat0) * self._ky)

    def distance_m(self, lat: float, lon: float) -> float:
        return self.line.distance(Point(self._xy(lat, lon)))

    def haversine_m(self, lat1, lon1, lat2, lon2) -> float:
        r = 6_371_000
        p1, p2 = math.radians(lat1), math.radians(lat2)
        dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
        a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
        return 2 * r * math.asin(math.sqrt(a))
