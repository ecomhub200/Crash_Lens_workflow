"""
CRASH LENS — Geography Resolution Module (geo_resolver.py)
===========================================================
Universal geography resolution for crash data normalization.
Any state's normalize.py imports this module to derive missing columns:

  - Physical Juris Name  (formatted "NNN. County/City/Town Name")
  - Juris Code           (numeric prefix from Physical Juris Name)
  - FIPS                 (3-digit county FIPS)
  - Place FIPS           (5-digit place FIPS for cities/towns)
  - DOT District         (from hierarchy.json)
  - Planning District    (from hierarchy.json)
  - MPO Name             (from hierarchy.json + us_mpos.json centroid match)
  - Ownership            (4-tier derivation: SYSTEM → FC+juris → juris-only → route name)
  - Area Type            (Urban/Rural from Census urbanized area data)

Data sources (all in the same GitHub repo):
  states/geography/us_counties.json          — 3,222 county-level records
  states/geography/us_places.json            — 32,333 city/town/CDP records
  states/geography/us_mpos.json              — 410 MPO records
  states/geography/us_county_subdivisions.json — 36,421 subdivision records
  states/geography/us_states.json            — 52 state records
  states/{state}/hierarchy.json              — per-state DOT regions & planning districts

Usage in a state normalize.py:
  from geo_resolver import GeoResolver

  resolver = GeoResolver(
      state_fips='10',            # Delaware
      state_abbr='DE',
      geo_dir='states/geography', # path to geography JSON folder
      hierarchy_path='states/delaware/hierarchy.json'  # optional
  )

  # For each crash row after initial column mapping:
  result = resolver.resolve_row(row)
  row['Physical Juris Name'] = result['physical_juris_name']
  row['Juris Code']          = result['juris_code']
  row['FIPS']                = result['fips']
  row['Place FIPS']          = result['place_fips']
  row['DOT District']        = result['dot_district']
  row['Planning District']   = result['planning_district']
  row['MPO Name']            = result['mpo_name']
  row['Ownership']           = result['ownership']
  row['Area Type']           = result['area_type']

Author: CrashLens Pipeline
Version: 2.0
"""

import json
import math
import os
import re
import logging
from typing import Optional, Dict, List, Tuple, Any

logger = logging.getLogger('crashlens.geo_resolver')

# ═══════════════════════════════════════════════════════════════
#  CONSTANTS
# ═══════════════════════════════════════════════════════════════

# LSADC codes from Census Bureau
LSADC_COUNTY = '06'           # County
LSADC_INDEPENDENT_CITY = '25' # Independent city (county-equivalent, e.g. Virginia)
LSADC_BOROUGH_AK = '04'       # Borough (Alaska)
LSADC_PARISH = '13'           # Parish (Louisiana)
LSADC_MUNICIPIO = '12'        # Municipio (Puerto Rico)
LSADC_CENSUS_AREA = '15'      # Census Area (Alaska)
LSADC_BOROUGH_CT_NJ = '14'    # Borough (Connecticut planning region / NJ county)
LSADC_TOWN_INC = '43'         # Incorporated town
LSADC_VILLAGE = '47'          # Incorporated village
LSADC_CDP = '57'              # Census Designated Place (unincorporated)

# Jurisdiction types for ownership derivation
JTYPE_COUNTY = 'county'
JTYPE_CITY = 'city'
JTYPE_TOWN = 'town'
JTYPE_VILLAGE = 'village'
JTYPE_CDP = 'cdp'
JTYPE_UNKNOWN = 'unknown'

# CrashLens standard Ownership values (must match frontend exactly)
OWN_STATE = '1. State Hwy Agency'
OWN_COUNTY = '2. County Hwy Agency'
OWN_CITY_TOWN = '3. City or Town Hwy Agency'
OWN_FEDERAL = '4. Federal Roads'
OWN_TOLL = '5. Toll Roads Maintained by Others'
OWN_PRIVATE = '6. Private/Unknown Roads'

# CrashLens standard Functional Class prefixes (for ownership derivation)
FC_INTERSTATE = '1'
FC_FREEWAY = '2'
FC_PRINCIPAL_ARTERIAL = '3'
FC_MINOR_ARTERIAL = '4'
FC_MAJOR_COLLECTOR = '5'
FC_MINOR_COLLECTOR = '6'
FC_LOCAL = '7'

# Route name patterns for ownership fallback
RE_STATE_ROUTE = re.compile(
    r'^(I-|US-|US |SR-|SR |SH-|SH |STATE |INTERSTATE |RTE |RT )',
    re.IGNORECASE
)
RE_COUNTY_ROUTE = re.compile(
    r'^(CR-|CR |CO RD|COUNTY |CTY )',
    re.IGNORECASE
)
RE_FEDERAL_ROUTE = re.compile(
    r'^(BIA-|BIA |TRIBAL |FOREST |NPS )',
    re.IGNORECASE
)

# ═══════════════════════════════════════════════════════════════
#  HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════

def _safe_float(val: Any, default: float = 0.0) -> float:
    """Safely convert a value to float."""
    try:
        f = float(val)
        return f if math.isfinite(f) else default
    except (ValueError, TypeError):
        return default


def _haversine_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Approximate distance in degrees (fast, no trig for short distances)."""
    return math.sqrt((lat1 - lat2) ** 2 + (lon1 - lon2) ** 2)


def _haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Haversine distance in miles (accurate for MPO radius checks)."""
    R = 3959  # Earth radius in miles
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


def _clean_name(name: str) -> str:
    """Normalize a jurisdiction name for fuzzy matching."""
    return re.sub(r'\s+', ' ', name.lower().strip())


def _strip_suffix(name: str) -> str:
    """Remove common suffixes for matching: 'County', 'city', 'town', etc."""
    return re.sub(
        r'\b(county|city|town|village|borough|parish|municipio|cdp|ccd)\b',
        '', name.lower()
    ).strip()


def _load_json(path: str) -> Any:
    """Load a JSON file, return empty dict/list on failure."""
    if not os.path.exists(path):
        logger.warning(f"Geography file not found: {path}")
        return {}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except (json.JSONDecodeError, IOError) as e:
        logger.error(f"Failed to load {path}: {e}")
        return {}


def _extract_records(data: Any) -> List[Dict]:
    """Extract records from JSON (handles both {records:[...]} and [...] formats)."""
    if isinstance(data, dict) and 'records' in data:
        return data['records']
    if isinstance(data, list):
        return data
    return []


# ═══════════════════════════════════════════════════════════════
#  JURISDICTION TYPE DETECTOR
# ═══════════════════════════════════════════════════════════════

class JurisTypeDetector:
    """
    Determines jurisdiction type (county/city/town/village/cdp) from:
      1. Census LSADC code (most reliable)
      2. Name pattern matching ("City of X", "X County", "Town of X")
      3. Source data fields (if state provides jurisdiction type)
    """

    # LSADC → jurisdiction type mapping
    LSADC_MAP = {
        LSADC_COUNTY: JTYPE_COUNTY,
        LSADC_INDEPENDENT_CITY: JTYPE_CITY,
        LSADC_BOROUGH_AK: JTYPE_COUNTY,       # Boroughs in AK function as counties
        LSADC_PARISH: JTYPE_COUNTY,            # LA parishes = counties
        LSADC_MUNICIPIO: JTYPE_COUNTY,         # PR municipios = counties
        LSADC_CENSUS_AREA: JTYPE_COUNTY,       # AK census areas = counties
        LSADC_BOROUGH_CT_NJ: JTYPE_COUNTY,     # CT/NJ boroughs = county-level
        LSADC_TOWN_INC: JTYPE_TOWN,
        LSADC_VILLAGE: JTYPE_VILLAGE,
        LSADC_CDP: JTYPE_CDP,
    }

    # Name patterns (checked in order)
    NAME_PATTERNS = [
        (re.compile(r'\bcity of\b', re.I), JTYPE_CITY),
        (re.compile(r'\bcity$', re.I), JTYPE_CITY),
        (re.compile(r'\btown of\b', re.I), JTYPE_TOWN),
        (re.compile(r'\btown$', re.I), JTYPE_TOWN),
        (re.compile(r'\bvillage of\b', re.I), JTYPE_VILLAGE),
        (re.compile(r'\bvillage$', re.I), JTYPE_VILLAGE),
        (re.compile(r'\bcounty$', re.I), JTYPE_COUNTY),
        (re.compile(r'\bparish$', re.I), JTYPE_COUNTY),
        (re.compile(r'\bborough$', re.I), JTYPE_COUNTY),
    ]

    @classmethod
    def from_lsadc(cls, lsadc: str) -> str:
        """Get jurisdiction type from Census LSADC code."""
        return cls.LSADC_MAP.get(lsadc, JTYPE_UNKNOWN)

    @classmethod
    def from_name(cls, name: str) -> str:
        """Infer jurisdiction type from name string."""
        if not name:
            return JTYPE_UNKNOWN
        for pattern, jtype in cls.NAME_PATTERNS:
            if pattern.search(name):
                return jtype
        return JTYPE_UNKNOWN

    @classmethod
    def detect(cls, name: str = '', lsadc: str = '', source_type: str = '') -> str:
        """
        Detect jurisdiction type using all available signals.
        Priority: LSADC > source_type > name pattern.
        """
        # LSADC is the most reliable
        if lsadc:
            result = cls.from_lsadc(lsadc)
            if result != JTYPE_UNKNOWN:
                return result

        # Source data may provide a type field (e.g. "municipality", "township")
        if source_type:
            st = source_type.lower().strip()
            if st in ('city', 'municipality', 'metro', 'urban'):
                return JTYPE_CITY
            if st in ('town', 'township'):
                return JTYPE_TOWN
            if st in ('village',):
                return JTYPE_VILLAGE
            if st in ('county', 'parish', 'borough'):
                return JTYPE_COUNTY

        # Fall back to name pattern
        return cls.from_name(name)


# ═══════════════════════════════════════════════════════════════
#  OWNERSHIP DERIVER
# ═══════════════════════════════════════════════════════════════

class OwnershipDeriver:
    """
    Derives the CrashLens Ownership column using a 4-tier fallback:

    Tier 1: SYSTEM column + jurisdiction type
      - State-maintained system → "1. State Hwy Agency"
      - Non-state + county → "2. County Hwy Agency"
      - Non-state + city/town → "3. City or Town Hwy Agency"

    Tier 2: Functional Class + jurisdiction type
      - FC 1-2 (Interstate, Freeway) → always State
      - FC 3 (Principal Arterial) → usually State
      - FC 4 (Minor Arterial) → State if state route, else juris type
      - FC 5 (Major Collector) → State in counties, City/Town otherwise
      - FC 6-7 (Minor Collector, Local) → matches juris type

    Tier 3: Jurisdiction type only (no FC or SYSTEM)
      - County → "2. County Hwy Agency"
      - City/Town → "3. City or Town Hwy Agency"

    Tier 4: Route name pattern (last resort)
      - I-*/US-*/SR-* → State
      - CR-* → County
      - Unknown → "1. State Hwy Agency" (safe default)
    """

    # Keywords indicating state-maintained systems
    STATE_SYSTEM_KEYWORDS = [
        'interstate', 'primary', 'secondary', 'state',
        'us route', 'us highway', 'state route', 'state highway',
        'shs',  # State Highway System
        'nhs',  # National Highway System
    ]

    # Keywords indicating NOT state-maintained
    NON_STATE_KEYWORDS = [
        'local', 'municipal', 'city', 'town', 'county',
        'private', 'unknown',
    ]

    @classmethod
    def derive(cls,
               juris_type: str,
               system: str = '',
               functional_class: str = '',
               route_name: str = '',
               ownership_hint: str = '') -> str:
        """
        Derive Ownership using 4-tier fallback.

        Args:
            juris_type:       'county', 'city', 'town', 'village', 'cdp', 'unknown'
            system:           Raw SYSTEM column value (e.g. "DOT Interstate", "Non-DOT secondary")
            functional_class: Normalized FC string (e.g. "1-Interstate (A,1)") or raw FC code
            route_name:       Route name (e.g. "I-95", "SR-7", "CR-123")
            ownership_hint:   If source data has a raw ownership/maintainer field
        Returns:
            CrashLens standard ownership string
        """

        # ── Tier 0: Direct ownership from source data ──
        if ownership_hint:
            hint = ownership_hint.lower().strip()
            if any(k in hint for k in ['state', 'dot', 'highway agency']):
                return OWN_STATE
            if 'county' in hint:
                return OWN_COUNTY
            if any(k in hint for k in ['city', 'town', 'municipal']):
                return OWN_CITY_TOWN
            if 'federal' in hint or 'tribal' in hint:
                return OWN_FEDERAL
            if 'toll' in hint or 'turnpike' in hint:
                return OWN_TOLL
            if 'private' in hint:
                return OWN_PRIVATE

        # ── Tier 1: SYSTEM column ──
        if system:
            sys_lower = system.lower().strip()
            is_state_system = any(kw in sys_lower for kw in cls.STATE_SYSTEM_KEYWORDS)
            is_non_state = any(kw in sys_lower for kw in cls.NON_STATE_KEYWORDS)

            # Also handle DOT-style: "DOT Interstate" / "Non-DOT secondary"
            if sys_lower.startswith('vdot') or sys_lower.startswith('cdot') or \
               sys_lower.startswith('mdot') or sys_lower.startswith('deldot') or \
               sys_lower.endswith('dot interstate') or sys_lower.endswith('dot primary') or \
               sys_lower.endswith('dot secondary'):
                is_state_system = True

            if sys_lower.startswith('non'):
                is_non_state = True

            if is_state_system and not is_non_state:
                return OWN_STATE
            elif is_non_state:
                return cls._owner_from_juris_type(juris_type)

        # ── Tier 2: Functional Class ──
        fc_code = cls._extract_fc_code(functional_class)
        if fc_code:
            if fc_code in (FC_INTERSTATE, FC_FREEWAY):
                # Always state-maintained
                return OWN_STATE
            elif fc_code == FC_PRINCIPAL_ARTERIAL:
                # Usually state-maintained, but can be city in urban areas
                if juris_type in (JTYPE_CITY, JTYPE_TOWN, JTYPE_VILLAGE):
                    # Check route name for state designation
                    if route_name and RE_STATE_ROUTE.match(route_name):
                        return OWN_STATE
                    # Principal arterials in cities are often still state-maintained
                    return OWN_STATE
                return OWN_STATE
            elif fc_code == FC_MINOR_ARTERIAL:
                # Often state-maintained, but depends
                if route_name and RE_STATE_ROUTE.match(route_name):
                    return OWN_STATE
                if juris_type in (JTYPE_CITY, JTYPE_TOWN, JTYPE_VILLAGE):
                    return OWN_CITY_TOWN
                # Counties: minor arterials are usually state secondary roads
                return OWN_STATE
            elif fc_code == FC_MAJOR_COLLECTOR:
                # In counties: usually state secondary system
                # In cities/towns: local maintenance
                if juris_type == JTYPE_COUNTY:
                    return OWN_STATE
                elif juris_type in (JTYPE_CITY, JTYPE_TOWN, JTYPE_VILLAGE):
                    return OWN_CITY_TOWN
                return OWN_STATE
            elif fc_code in (FC_MINOR_COLLECTOR, FC_LOCAL):
                # Owner matches jurisdiction type
                return cls._owner_from_juris_type(juris_type)

        # ── Tier 3: Jurisdiction type only ──
        if juris_type != JTYPE_UNKNOWN:
            return cls._owner_from_juris_type(juris_type)

        # ── Tier 4: Route name pattern ──
        if route_name:
            if RE_STATE_ROUTE.match(route_name):
                return OWN_STATE
            if RE_COUNTY_ROUTE.match(route_name):
                return OWN_COUNTY
            if RE_FEDERAL_ROUTE.match(route_name):
                return OWN_FEDERAL

        # ── Default: State (safe default — majority of crash records) ──
        return OWN_STATE

    @staticmethod
    def _owner_from_juris_type(juris_type: str) -> str:
        """Map jurisdiction type to ownership."""
        if juris_type == JTYPE_COUNTY:
            return OWN_COUNTY
        elif juris_type in (JTYPE_CITY, JTYPE_TOWN, JTYPE_VILLAGE):
            return OWN_CITY_TOWN
        elif juris_type == JTYPE_CDP:
            # CDPs are unincorporated → county jurisdiction
            return OWN_COUNTY
        return OWN_STATE  # safe default

    @staticmethod
    def _extract_fc_code(fc_value: str) -> str:
        """Extract single-digit FC code from various formats."""
        if not fc_value:
            return ''
        fc = str(fc_value).strip()
        # Handle CrashLens standard format: "1-Interstate (A,1)"
        if fc and fc[0].isdigit():
            return fc[0]
        # Handle raw numeric: "1", "2", etc.
        if fc.isdigit() and len(fc) == 1:
            return fc
        # Handle text descriptions
        fc_lower = fc.lower()
        if 'interstate' in fc_lower:
            return FC_INTERSTATE
        if 'freeway' in fc_lower or 'expressway' in fc_lower:
            return FC_FREEWAY
        if 'principal arterial' in fc_lower:
            return FC_PRINCIPAL_ARTERIAL
        if 'minor arterial' in fc_lower:
            return FC_MINOR_ARTERIAL
        if 'major collector' in fc_lower:
            return FC_MAJOR_COLLECTOR
        if 'minor collector' in fc_lower:
            return FC_MINOR_COLLECTOR
        if 'local' in fc_lower:
            return FC_LOCAL
        return ''


# ═══════════════════════════════════════════════════════════════
#  MAIN RESOLVER CLASS
# ═══════════════════════════════════════════════════════════════

class GeoResolver:
    """
    Main geography resolution engine for CrashLens normalization.

    Loads Census Bureau geography files + state hierarchy once,
    then provides fast per-row resolution for all derived columns.
    """

    def __init__(self,
                 state_fips: str,
                 state_abbr: str,
                 geo_dir: str = 'states/geography',
                 hierarchy_path: str = '',
                 custom_juris_map: Optional[Dict] = None):
        """
        Initialize the resolver with state config and geography data.

        Args:
            state_fips:       2-digit state FIPS (e.g. '51' for Virginia)
            state_abbr:       2-letter state abbreviation (e.g. 'VA')
            geo_dir:          Path to directory containing us_counties.json, etc.
            hierarchy_path:   Path to the state's hierarchy.json (optional)
            custom_juris_map: Optional dict mapping source jurisdiction names/codes
                              to Physical Juris Name (overrides auto-detection)
        """
        self.state_fips = state_fips.zfill(2)
        self.state_abbr = state_abbr.upper()
        self.custom_juris_map = custom_juris_map or {}
        self.dot_name = f'{self.state_abbr}DOT'

        # ── Load geography files ──
        logger.info(f"Loading geography data for {self.state_abbr} (FIPS {self.state_fips})...")

        all_counties = _extract_records(_load_json(os.path.join(geo_dir, 'us_counties.json')))
        all_places = _extract_records(_load_json(os.path.join(geo_dir, 'us_places.json')))
        all_mpos = _extract_records(_load_json(os.path.join(geo_dir, 'us_mpos.json')))

        # Filter to this state
        self.counties = [c for c in all_counties if c.get('STATE') == self.state_fips]
        self.places = [p for p in all_places if p.get('STATE') == self.state_fips]
        self.mpos = [m for m in all_mpos if m.get('STATE') == self.state_abbr]

        logger.info(f"  Counties: {len(self.counties)}, Places: {len(self.places)}, MPOs: {len(self.mpos)}")

        # ── Build fast-lookup indexes ──
        self._build_indexes()

        # ── Load hierarchy ──
        self.hierarchy = {}
        self.fips_to_region = {}
        self.fips_to_pd = {}
        self.fips_to_mpo = {}
        if hierarchy_path:
            self._load_hierarchy(hierarchy_path)

        # ── Build MPO radius table for centroid matching ──
        self._build_mpo_radius_table()

        # ── Cache for repeated jurisdiction lookups ──
        self._juris_cache: Dict[str, Dict] = {}

        logger.info(f"GeoResolver ready: {len(self.counties)} counties, "
                     f"{len(self.fips_to_region)} region mappings, "
                     f"{len(self.fips_to_mpo)} MPO mappings, "
                     f"{len(self.mpo_radius_table)} MPO radius entries")

    # ═══════════════════════════════════════════════════════════
    #  INDEX BUILDING
    # ═══════════════════════════════════════════════════════════

    def _build_indexes(self):
        """Build lookup indexes for fast O(1) access."""

        # County by FIPS: (state_fips, county_fips) → record
        self.county_by_fips: Dict[str, Dict] = {}
        for c in self.counties:
            self.county_by_fips[c['COUNTY']] = c

        # County by name (lowercase basename) → record
        # When a basename collides (e.g. "Fairfax" = both county and independent city),
        # prefer the actual county (LSADC=06) over the independent city (LSADC=25)
        self.county_by_name: Dict[str, Dict] = {}
        self.county_by_fullname: Dict[str, Dict] = {}
        # First pass: add all counties
        for c in self.counties:
            basename = (c.get('BASENAME') or '').lower().strip()
            fullname = (c.get('NAME') or '').lower().strip()
            if fullname:
                self.county_by_fullname[fullname] = c
            if basename:
                existing = self.county_by_name.get(basename)
                if existing is None:
                    self.county_by_name[basename] = c
                elif c.get('LSADC') == LSADC_COUNTY and existing.get('LSADC') != LSADC_COUNTY:
                    # Actual county wins over independent city for bare name lookups
                    self.county_by_name[basename] = c

        # Separate index for independent cities (LSADC=25) — used by "City of X" lookups
        self.ind_city_by_name: Dict[str, Dict] = {}
        for c in self.counties:
            if c.get('LSADC') == LSADC_INDEPENDENT_CITY:
                basename = (c.get('BASENAME') or '').lower().strip()
                if basename:
                    self.ind_city_by_name[basename] = c

        # County centroids for proximity matching: list of (lat, lon, county_record)
        self.county_centroids: List[Tuple[float, float, Dict]] = []
        for c in self.counties:
            lat = _safe_float(c.get('CENTLAT') or c.get('INTPTLAT'))
            lon = _safe_float(c.get('CENTLON') or c.get('INTPTLON'))
            if lat != 0.0 and lon != 0.0:
                self.county_centroids.append((lat, lon, c))

        # Place by name (lowercase basename) → record
        # Multiple places can share a name; keep the first (most populous is usually first)
        self.place_by_name: Dict[str, Dict] = {}
        self.place_by_fullname: Dict[str, Dict] = {}
        for p in self.places:
            basename = (p.get('BASENAME') or '').lower().strip()
            fullname = (p.get('NAME') or '').lower().strip()
            if basename and basename not in self.place_by_name:
                self.place_by_name[basename] = p
            if fullname and fullname not in self.place_by_fullname:
                self.place_by_fullname[fullname] = p

    def _load_hierarchy(self, path: str):
        """Load hierarchy.json and build reverse-lookup indexes."""
        data = _load_json(path)
        if not data:
            logger.warning(f"No hierarchy data loaded from {path}")
            return
        self.hierarchy = data

        # Extract DOT agency name if available
        state_info = data.get('state', {})
        if state_info.get('dot'):
            self.dot_name = state_info['dot']

        # Build: county_fips → (region_name, planning_district, mpo)
        regions = data.get('regions', {})
        for region_key, region_val in regions.items():
            region_name = region_val.get('name') or region_val.get('shortName') or region_key
            planning_district = region_val.get('planningDistrict') or region_name
            mpo = region_val.get('mpo') or ''

            for county_fips in region_val.get('counties', []):
                # Normalize FIPS to 3 digits
                cf = str(county_fips).zfill(3)
                self.fips_to_region[cf] = region_name
                self.fips_to_pd[cf] = planning_district
                if mpo:
                    self.fips_to_mpo[cf] = mpo

            # Also handle county names mapping
            county_names = region_val.get('countyNames', {})
            for fips_str, name in county_names.items():
                cf = str(fips_str).zfill(3)
                if cf not in self.fips_to_region:
                    self.fips_to_region[cf] = region_name
                    self.fips_to_pd[cf] = planning_district

        logger.info(f"  Hierarchy: {len(regions)} regions, "
                     f"{len(self.fips_to_region)} county→region, "
                     f"{len(self.fips_to_mpo)} county→MPO mappings")

    def _build_mpo_radius_table(self):
        """
        Build MPO effective radius table for centroid proximity matching.
        Uses MPO area (sq miles) to compute an effective radius:
          radius = sqrt(area / π) * 1.5  (1.5x buffer for irregular shapes)
        """
        self.mpo_radius_table: List[Tuple[float, float, float, Dict]] = []
        for m in self.mpos:
            lat = _safe_float(m.get('CENTLAT') or m.get('INTPTLAT'))
            lon = _safe_float(m.get('CENTLON') or m.get('INTPTLON'))
            area = _safe_float(m.get('AREA'), 0.0)
            if lat == 0.0 or lon == 0.0:
                continue
            if area > 0:
                radius_miles = math.sqrt(area / math.pi) * 1.5
            else:
                radius_miles = 25.0  # default 25-mile radius if no area data
            self.mpo_radius_table.append((lat, lon, radius_miles, m))

    # ═══════════════════════════════════════════════════════════
    #  FIPS RESOLUTION (the foundation)
    # ═══════════════════════════════════════════════════════════

    def resolve_fips(self,
                     juris_name: str = '',
                     county_fips: str = '',
                     lat: float = 0.0,
                     lon: float = 0.0) -> Dict:
        """
        Resolve FIPS and jurisdiction info from available signals.

        Priority:
          1. Direct FIPS code (if source data provides it)
          2. Name matching against Census counties/places
          3. Centroid proximity to nearest county

        Returns dict with: fips, county_name, place_fips, place_name,
                           juris_type, lsadc, source_method
        """
        result = {
            'fips': '',
            'county_name': '',
            'county_fullname': '',
            'place_fips': '',
            'place_name': '',
            'juris_type': JTYPE_UNKNOWN,
            'lsadc': '',
            'source_method': 'unresolved',
        }

        # ── Method 1: Direct FIPS ──
        if county_fips:
            cf = str(county_fips).zfill(3)
            county = self.county_by_fips.get(cf)
            if county:
                result['fips'] = cf
                result['county_name'] = county.get('BASENAME', '')
                result['county_fullname'] = county.get('NAME', '')
                result['lsadc'] = county.get('LSADC', '')
                result['juris_type'] = JurisTypeDetector.from_lsadc(result['lsadc'])
                result['source_method'] = 'direct_fips'
                return result

        # ── Method 2: Name matching ──
        if juris_name:
            name_result = self._match_by_name(juris_name)
            if name_result:
                result.update(name_result)
                return result

        # ── Method 3: Centroid proximity ──
        if lat != 0.0 and lon != 0.0:
            centroid_result = self._match_by_centroid(lat, lon)
            if centroid_result:
                result.update(centroid_result)
                return result

        return result

    def _match_by_name(self, name: str) -> Optional[Dict]:
        """Match a jurisdiction name against Census county and place databases."""
        clean = _clean_name(name)
        stripped = _strip_suffix(name)

        # Try exact county fullname match
        county = self.county_by_fullname.get(clean)
        if county:
            return self._county_to_result(county, 'name_exact')

        # Try county basename match
        county = self.county_by_name.get(stripped)
        if county:
            return self._county_to_result(county, 'name_basename')

        # Try place fullname match
        place = self.place_by_fullname.get(clean)
        if place:
            return self._place_to_result(place, 'place_name_exact')

        # Try place basename match
        place = self.place_by_name.get(stripped)
        if place:
            return self._place_to_result(place, 'place_name_basename')

        # Try with "City of X" → look up in independent city index first
        if 'city of ' in clean:
            basename = clean.replace('city of ', '').strip()
            # Check independent city index (Virginia-style)
            ind_city = self.ind_city_by_name.get(basename)
            if ind_city:
                return self._county_to_result(ind_city, 'city_of_transform')
            # Also check regular county index
            county = self.county_by_name.get(basename)
            if county and county.get('LSADC') == LSADC_INDEPENDENT_CITY:
                return self._county_to_result(county, 'city_of_transform')
            # Check places
            place = self.place_by_name.get(basename)
            if place:
                return self._place_to_result(place, 'city_of_transform')

        # Try with "Town of X" → "X"
        if 'town of ' in clean:
            basename = clean.replace('town of ', '').strip()
            place = self.place_by_name.get(basename)
            if place:
                return self._place_to_result(place, 'town_of_transform')

        # Try fuzzy: remove all non-alphanumeric
        alpha_only = re.sub(r'[^a-z0-9]', '', clean)
        for county in self.counties:
            county_alpha = re.sub(r'[^a-z0-9]', '', (county.get('BASENAME') or '').lower())
            if county_alpha and county_alpha == alpha_only:
                return self._county_to_result(county, 'fuzzy_alpha')

        return None

    def _match_by_centroid(self, lat: float, lon: float) -> Optional[Dict]:
        """Find the nearest county by centroid proximity."""
        if not self.county_centroids:
            return None

        best_dist = float('inf')
        best_county = None

        for clat, clon, county in self.county_centroids:
            dist = _haversine_deg(lat, lon, clat, clon)
            if dist < best_dist:
                best_dist = dist
                best_county = county

        # Threshold: 0.5 degrees ≈ 35 miles (reasonable for county-level)
        if best_county and best_dist < 0.5:
            return self._county_to_result(best_county, 'centroid')

        return None

    def _county_to_result(self, county: Dict, method: str) -> Dict:
        """Convert a Census county record to a resolution result."""
        fips = county.get('COUNTY', '')
        lsadc = county.get('LSADC', '')
        return {
            'fips': fips,
            'county_name': county.get('BASENAME', ''),
            'county_fullname': county.get('NAME', ''),
            'place_fips': '',
            'place_name': '',
            'juris_type': JurisTypeDetector.from_lsadc(lsadc),
            'lsadc': lsadc,
            'source_method': method,
        }

    def _place_to_result(self, place: Dict, method: str) -> Dict:
        """
        Convert a Census place record to a resolution result.
        Places don't have a COUNTY field, so we find the parent county by centroid.
        """
        place_fips = place.get('PLACE', '')
        place_name = place.get('NAME', '')
        lsadc = place.get('LSADC', '')
        juris_type = JurisTypeDetector.from_lsadc(lsadc)

        # Clean the place name (Census includes suffix like "Alexandria city")
        place_basename = place.get('BASENAME', '')

        # Find parent county via centroid proximity
        plat = _safe_float(place.get('CENTLAT') or place.get('INTPTLAT'))
        plon = _safe_float(place.get('CENTLON') or place.get('INTPTLON'))

        county_fips = ''
        county_name = ''
        county_fullname = ''
        if plat != 0.0 and plon != 0.0 and self.county_centroids:
            best_dist = float('inf')
            best_county = None
            for clat, clon, county in self.county_centroids:
                # Skip independent cities when looking for parent county
                if county.get('LSADC') == LSADC_INDEPENDENT_CITY:
                    continue
                dist = _haversine_deg(plat, plon, clat, clon)
                if dist < best_dist:
                    best_dist = dist
                    best_county = county
            if best_county and best_dist < 0.5:
                county_fips = best_county.get('COUNTY', '')
                county_name = best_county.get('BASENAME', '')
                county_fullname = best_county.get('NAME', '')

        return {
            'fips': county_fips,
            'county_name': county_name,
            'county_fullname': county_fullname,
            'place_fips': place_fips,
            'place_name': place_name,
            'juris_type': juris_type,
            'lsadc': lsadc,
            'source_method': method,
        }

    # ═══════════════════════════════════════════════════════════
    #  MPO RESOLUTION
    # ═══════════════════════════════════════════════════════════

    def resolve_mpo(self, county_fips: str, lat: float = 0.0, lon: float = 0.0) -> str:
        """
        Resolve MPO name for a jurisdiction.
        Priority: hierarchy.json explicit mapping > centroid proximity to us_mpos.json.
        """
        # Check hierarchy first
        cf = county_fips.zfill(3) if county_fips else ''
        if cf and cf in self.fips_to_mpo:
            return self.fips_to_mpo[cf]

        # Centroid proximity against MPO radius table
        if lat != 0.0 and lon != 0.0 and self.mpo_radius_table:
            for mlat, mlon, radius_miles, mpo_rec in self.mpo_radius_table:
                dist = _haversine_miles(lat, lon, mlat, mlon)
                if dist <= radius_miles:
                    return mpo_rec.get('MPO_NAME') or mpo_rec.get('NAME', '')

        return ''

    # ═══════════════════════════════════════════════════════════
    #  PHYSICAL JURIS NAME FORMATTING
    # ═══════════════════════════════════════════════════════════

    def format_physical_juris_name(self,
                                   county_fips: str,
                                   juris_type: str,
                                   county_name: str,
                                   place_name: str = '',
                                   juris_code_override: int = -1) -> Tuple[str, int]:
        """
        Format the Physical Juris Name in CrashLens standard format:
          Counties:  "NNN. County Name"        (e.g. "043. Henrico County")
          Cities:    "NNN. City of Name"        (e.g. "100. City of Alexandria")
          Towns:     "NNN. Town of Name"        (e.g. "140. Town of Abingdon")

        The NNN Juris Code numbering is STATE-SPECIFIC:
          - VDOT uses: 000-099 for counties, 100-139 for cities, 140-339 for towns
          - Other states may use FIPS directly, or sequential numbering

        Args:
            county_fips:         3-digit FIPS
            juris_type:          'county', 'city', 'town', etc.
            county_name:         Base county name (e.g. "Henrico")
            place_name:          Base place name for cities/towns
            juris_code_override: If source data has its own code, use it

        Returns:
            Tuple of (formatted_name, juris_code)
        """
        fips_int = int(county_fips) if county_fips and county_fips.isdigit() else 0

        if juris_code_override >= 0:
            code = juris_code_override
        else:
            # Default: use FIPS as the juris code
            code = fips_int

        if juris_type == JTYPE_CITY and place_name:
            name = f"{code:03d}. City of {place_name}"
        elif juris_type == JTYPE_TOWN and place_name:
            name = f"{code:03d}. Town of {place_name}"
        elif juris_type == JTYPE_VILLAGE and place_name:
            name = f"{code:03d}. Village of {place_name}"
        elif juris_type == JTYPE_COUNTY:
            # Ensure "County" suffix is present
            cname = county_name
            if cname and 'county' not in cname.lower() and 'parish' not in cname.lower() \
               and 'borough' not in cname.lower():
                cname = f"{cname} County"
            name = f"{code:03d}. {cname}"
        elif juris_type == JTYPE_CITY:
            # Independent city without separate place name
            # Strip Census suffixes like "city", "City" from county_name
            cname = re.sub(r'\s+(city|City)$', '', county_name).strip()
            name = f"{code:03d}. City of {cname}"
        else:
            # Fallback: use whatever name we have
            name = f"{code:03d}. {county_name or place_name or 'Unknown'}"

        return name, code

    # ═══════════════════════════════════════════════════════════
    #  MAIN ROW-LEVEL RESOLVER
    # ═══════════════════════════════════════════════════════════

    def resolve_row(self, row: Dict[str, str],
                    source_juris_field: str = '',
                    source_county_fips_field: str = '',
                    source_system_field: str = '',
                    source_fc_field: str = '',
                    source_route_field: str = '',
                    source_ownership_field: str = '',
                    source_juris_type_field: str = '') -> Dict[str, str]:
        """
        Resolve all derived geography columns for a single crash row.

        Args:
            row:  Dict of column_name → value for the current crash record.
            source_*_field:  Names of source columns to read from the row.
                             If empty, the resolver checks common field names.

        Returns:
            Dict with all resolved values:
              physical_juris_name, juris_code, fips, place_fips,
              dot_district, planning_district, mpo_name, ownership, area_type
        """
        # ── Extract signals from the row ──
        juris_name = self._get_field(row, source_juris_field, [
            'Physical Juris Name', 'physical_juris_name',
            'COUNTY NAME', 'county_name', 'COUNTY_NAME',
            'county_desc', 'COUNTY_DESC', 'jurisdiction',
            'JURISDICTION', 'MUNICIPALITY', 'municipality',
        ])
        county_fips_raw = self._get_field(row, source_county_fips_field, [
            'FIPS', 'fips', 'COUNTY_FIPS', 'county_fips',
            'COUNTY CODE', 'county_code', 'CNTY_CD',
        ])
        lat = _safe_float(self._get_field(row, '', ['y', 'Y', 'LATITUDE', 'latitude', 'lat', 'LAT']))
        lon = _safe_float(self._get_field(row, '', ['x', 'X', 'LONGITUDE', 'longitude', 'lon', 'LON']))

        system_val = self._get_field(row, source_system_field, [
            'SYSTEM', 'system', 'ROUTE_TYPE', 'route_type',
            'ROAD_SYSTEM', 'road_system', 'SYS_CD',
        ])
        fc_val = self._get_field(row, source_fc_field, [
            'Functional Class', 'functional_class', 'FUNC_CLASS',
            'func_class', 'FC', 'FCLASS', 'fclass',
        ])
        route_val = self._get_field(row, source_route_field, [
            'RTE Name', 'rte_name', 'ROUTE_NAME', 'route_name',
            'ROAD_NAME', 'road_name', 'STREET_NAME',
        ])
        ownership_hint = self._get_field(row, source_ownership_field, [
            'Ownership', 'ownership', 'OWNER', 'owner',
            'MAINTAINED_BY', 'maintained_by',
        ])
        source_juris_type = self._get_field(row, source_juris_type_field, [
            'JURIS_TYPE', 'juris_type', 'MUNICIPALITY_TYPE',
        ])

        # ── Check cache ──
        cache_key = f"{juris_name}|{county_fips_raw}|{round(lat,3)}|{round(lon,3)}"
        if cache_key in self._juris_cache:
            cached = self._juris_cache[cache_key].copy()
            # Still need to derive per-row ownership (depends on FC, SYSTEM, route)
            cached['ownership'] = OwnershipDeriver.derive(
                juris_type=cached.get('_juris_type', JTYPE_UNKNOWN),
                system=system_val,
                functional_class=fc_val,
                route_name=route_val,
                ownership_hint=ownership_hint,
            )
            return cached

        # ── Check custom mapping first ──
        if juris_name and juris_name in self.custom_juris_map:
            custom = self.custom_juris_map[juris_name]
            # custom can be a string (Physical Juris Name) or a dict with full mapping
            if isinstance(custom, str):
                juris_name = custom
            elif isinstance(custom, dict):
                # Full override
                result = {
                    'physical_juris_name': custom.get('physical_juris_name', ''),
                    'juris_code': str(custom.get('juris_code', '')),
                    'fips': custom.get('fips', ''),
                    'place_fips': custom.get('place_fips', ''),
                    'dot_district': custom.get('dot_district', ''),
                    'planning_district': custom.get('planning_district', ''),
                    'mpo_name': custom.get('mpo_name', ''),
                    'ownership': '',
                    'area_type': custom.get('area_type', ''),
                    '_juris_type': custom.get('juris_type', JTYPE_UNKNOWN),
                }
                result['ownership'] = OwnershipDeriver.derive(
                    juris_type=result['_juris_type'],
                    system=system_val, functional_class=fc_val,
                    route_name=route_val, ownership_hint=ownership_hint,
                )
                self._juris_cache[cache_key] = result
                return result

        # ── Resolve FIPS ──
        fips_result = self.resolve_fips(
            juris_name=juris_name,
            county_fips=county_fips_raw,
            lat=lat, lon=lon,
        )
        county_fips = fips_result['fips']
        juris_type = fips_result['juris_type']

        # Refine juris_type from source data or name if Census didn't determine it
        if juris_type == JTYPE_UNKNOWN:
            juris_type = JurisTypeDetector.detect(
                name=juris_name,
                source_type=source_juris_type,
            )

        # ── Format Physical Juris Name ──
        display_name = juris_name  # Start with source name
        if fips_result['county_name'] or fips_result['place_name']:
            # Use census data for clean formatting
            if juris_type in (JTYPE_CITY, JTYPE_TOWN, JTYPE_VILLAGE) and fips_result['place_name']:
                basename = fips_result['place_name'].replace(' city', '').replace(' town', '') \
                           .replace(' village', '').replace(' CDP', '').strip()
                formatted, juris_code = self.format_physical_juris_name(
                    county_fips, juris_type, fips_result['county_name'], basename
                )
            else:
                formatted, juris_code = self.format_physical_juris_name(
                    county_fips, juris_type, fips_result['county_fullname'] or fips_result['county_name']
                )
            display_name = formatted
        else:
            # No census match — format from raw name
            juris_code = int(county_fips) if county_fips and county_fips.isdigit() else 0
            display_name = f"{juris_code:03d}. {juris_name}" if juris_name else ''

        # ── Resolve hierarchy columns ──
        cf3 = county_fips.zfill(3) if county_fips else ''
        dot_district = self.fips_to_region.get(cf3, '')
        planning_district = self.fips_to_pd.get(cf3, '')

        # ── Resolve MPO ──
        mpo_name = self.resolve_mpo(county_fips, lat, lon)

        # ── Derive Ownership ──
        ownership = OwnershipDeriver.derive(
            juris_type=juris_type,
            system=system_val,
            functional_class=fc_val,
            route_name=route_val,
            ownership_hint=ownership_hint,
        )

        # ── Derive Area Type ──
        # Simple heuristic: cities and towns = Urban, counties default to Rural
        # (A more accurate method uses Census urbanized area boundaries)
        if juris_type in (JTYPE_CITY, JTYPE_TOWN, JTYPE_VILLAGE):
            area_type = 'Urban'
        else:
            area_type = 'Rural'

        # ── Assemble result ──
        result = {
            'physical_juris_name': display_name,
            'juris_code': str(juris_code),
            'fips': county_fips,
            'place_fips': fips_result['place_fips'],
            'dot_district': dot_district,
            'planning_district': planning_district,
            'mpo_name': mpo_name,
            'ownership': ownership,
            'area_type': area_type,
            '_juris_type': juris_type,       # internal, not written to output
            '_source_method': fips_result['source_method'],  # for debugging
        }

        # Cache
        self._juris_cache[cache_key] = result
        return result

    @staticmethod
    def _get_field(row: Dict[str, str], explicit_field: str,
                   fallback_names: List[str]) -> str:
        """Get a field value from a row, trying explicit name then fallbacks."""
        if explicit_field and explicit_field in row:
            return (row[explicit_field] or '').strip()
        for name in fallback_names:
            if name in row:
                val = (row[name] or '').strip()
                if val:
                    return val
        return ''

    # ═══════════════════════════════════════════════════════════
    #  BULK OPERATIONS
    # ═══════════════════════════════════════════════════════════

    def resolve_all(self, rows: List[Dict[str, str]], **kwargs) -> List[Dict[str, str]]:
        """
        Resolve geography for all rows and apply columns in-place.

        Args:
            rows:   List of crash row dicts (modified in-place)
            **kwargs: Passed to resolve_row() for field name config

        Returns:
            The same rows list with geography columns populated.
        """
        total = len(rows)
        resolved = 0
        unresolved = 0

        for i, row in enumerate(rows):
            result = self.resolve_row(row, **kwargs)

            # Apply resolved values (only if not already set)
            if not row.get('Physical Juris Name'):
                row['Physical Juris Name'] = result['physical_juris_name']
            if not row.get('Juris Code'):
                row['Juris Code'] = result['juris_code']
            if not row.get('FIPS'):
                row['FIPS'] = result['fips']
            if not row.get('Place FIPS'):
                row['Place FIPS'] = result['place_fips']
            if not row.get('DOT District'):
                row['DOT District'] = result['dot_district']
            if not row.get('Planning District'):
                row['Planning District'] = result['planning_district']
            if not row.get('MPO Name'):
                row['MPO Name'] = result['mpo_name']
            if not row.get('Ownership'):
                row['Ownership'] = result['ownership']
            if not row.get('Area Type'):
                row['Area Type'] = result['area_type']

            if result['fips']:
                resolved += 1
            else:
                unresolved += 1

            if (i + 1) % 10000 == 0:
                logger.info(f"  Geography resolution: {i+1:,}/{total:,} rows processed")

        logger.info(f"Geography resolution complete: {resolved:,} resolved, "
                     f"{unresolved:,} unresolved out of {total:,} rows")
        return rows

    # ═══════════════════════════════════════════════════════════
    #  VALIDATION & REPORTING
    # ═══════════════════════════════════════════════════════════

    def get_resolution_report(self) -> Dict:
        """Generate a summary report of cached resolution results."""
        methods = {}
        juris_types = {}
        ownership_dist = {}
        mpo_assigned = 0
        district_assigned = 0

        for result in self._juris_cache.values():
            method = result.get('_source_method', 'unknown')
            methods[method] = methods.get(method, 0) + 1

            jtype = result.get('_juris_type', 'unknown')
            juris_types[jtype] = juris_types.get(jtype, 0) + 1

            own = result.get('ownership', '')
            ownership_dist[own] = ownership_dist.get(own, 0) + 1

            if result.get('mpo_name'):
                mpo_assigned += 1
            if result.get('dot_district'):
                district_assigned += 1

        total = len(self._juris_cache)
        resolved = sum(1 for r in self._juris_cache.values() if r.get('fips'))

        return {
            'total_jurisdictions': total,
            'resolved': resolved,
            'unresolved': total - resolved,
            'coverage_pct': round((resolved / total * 100) if total > 0 else 0, 1),
            'resolution_methods': methods,
            'jurisdiction_types': juris_types,
            'ownership_distribution': ownership_dist,
            'mpo_assigned': mpo_assigned,
            'district_assigned': district_assigned,
            'cache_size': total,
        }


# ═══════════════════════════════════════════════════════════════
#  CONVENIENCE FUNCTION (one-call usage)
# ═══════════════════════════════════════════════════════════════

def resolve_geography(rows: List[Dict[str, str]],
                      state_fips: str,
                      state_abbr: str,
                      geo_dir: str = 'states/geography',
                      hierarchy_path: str = '',
                      **kwargs) -> Tuple[List[Dict[str, str]], Dict]:
    """
    One-call convenience function to resolve all geography columns.

    Usage:
      rows, report = resolve_geography(
          rows=normalized_data,
          state_fips='10',
          state_abbr='DE',
          geo_dir='states/geography',
          hierarchy_path='states/delaware/hierarchy.json'
      )

    Returns:
      Tuple of (rows_with_geography, resolution_report)
    """
    resolver = GeoResolver(
        state_fips=state_fips,
        state_abbr=state_abbr,
        geo_dir=geo_dir,
        hierarchy_path=hierarchy_path,
    )
    rows = resolver.resolve_all(rows, **kwargs)
    report = resolver.get_resolution_report()
    return rows, report


# ═══════════════════════════════════════════════════════════════
#  CLI ENTRY POINT (for testing)
# ═══════════════════════════════════════════════════════════════

if __name__ == '__main__':
    import argparse
    import csv

    parser = argparse.ArgumentParser(description='CrashLens Geography Resolution Module')
    parser.add_argument('--csv', required=True, help='Input CSV path')
    parser.add_argument('--state-fips', required=True, help='2-digit state FIPS (e.g. 51)')
    parser.add_argument('--state-abbr', required=True, help='2-letter state abbreviation (e.g. VA)')
    parser.add_argument('--geo-dir', default='states/geography', help='Path to geography JSON folder')
    parser.add_argument('--hierarchy', default='', help='Path to hierarchy.json')
    parser.add_argument('--output', default='', help='Output CSV path (default: input_resolved.csv)')
    parser.add_argument('--report', default='', help='Output JSON report path')
    parser.add_argument('--limit', type=int, default=0, help='Process only N rows (for testing)')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(name)s] %(message)s')

    # Load CSV
    logger.info(f"Loading CSV: {args.csv}")
    with open(args.csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    logger.info(f"Loaded {len(rows):,} rows")

    if args.limit > 0:
        rows = rows[:args.limit]
        logger.info(f"Limited to {len(rows):,} rows for testing")

    # Resolve
    rows, report = resolve_geography(
        rows=rows,
        state_fips=args.state_fips,
        state_abbr=args.state_abbr,
        geo_dir=args.geo_dir,
        hierarchy_path=args.hierarchy,
    )

    # Print report
    print("\n=== RESOLUTION REPORT ===")
    print(json.dumps(report, indent=2))

    # Save output CSV
    output_path = args.output or args.csv.replace('.csv', '_resolved.csv')
    fieldnames = list(rows[0].keys()) if rows else []
    with open(output_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    logger.info(f"Saved resolved CSV: {output_path}")

    # Save report
    if args.report:
        with open(args.report, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2)
        logger.info(f"Saved report: {args.report}")
