"""Geographic zones: a 3x3 grid over the simulated city (Colombo, Sri Lanka).

The bounding box covers the land area from Kotahena/Kelaniya in the north to
Dehiwala/Kottawa in the south, and from the coast at Borella/Dehiwala inland to
Malabe. Each cell is labelled with a well-known neighbourhood that lies inside it
(labels are approximate: a cell is a rectangle, not an administrative boundary).

zone_for(lat, lon) is the reference implementation. The Spark streaming job
builds its column expression from the same constants so both always agree.
"""

from __future__ import annotations

CITY_NAME = "Colombo"
CITY_LAT_MIN = 6.84
CITY_LAT_MAX = 6.98
CITY_LON_MIN = 79.86
CITY_LON_MAX = 79.97
GRID_SIZE = 3

OUTSIDE_ZONE = "outside"

# ZONE_NAMES[row][col]; row 0 = south (low latitude), col 0 = west (low longitude)
ZONE_NAMES: tuple[tuple[str, ...], ...] = (
    ("Dehiwala", "Maharagama", "Kottawa"),
    ("Borella", "Battaramulla", "Malabe"),
    ("Kotahena", "Kelaniya", "Mulleriyawa"),
)

ALL_ZONES: tuple[str, ...] = tuple(z for row in ZONE_NAMES for z in row) + (OUTSIDE_ZONE,)


def grid_index(value: float, low: float, high: float, n: int = GRID_SIZE) -> int:
    """Cell index 0..n-1 of value inside [low, high]; the upper edge belongs to the last cell."""
    idx = int((value - low) / (high - low) * n)
    return min(max(idx, 0), n - 1)


def zone_for(latitude: float, longitude: float) -> str:
    if not (CITY_LAT_MIN <= latitude <= CITY_LAT_MAX and CITY_LON_MIN <= longitude <= CITY_LON_MAX):
        return OUTSIDE_ZONE
    row = grid_index(latitude, CITY_LAT_MIN, CITY_LAT_MAX)
    col = grid_index(longitude, CITY_LON_MIN, CITY_LON_MAX)
    return ZONE_NAMES[row][col]
