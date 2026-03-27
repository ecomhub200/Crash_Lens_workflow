/**
 * CRASH LENS - US FIPS State & County Database
 *
 * Comprehensive database of all 50 US states + DC with:
 *   - State FIPS codes, names, abbreviations
 *   - Approximate map centers and bounding boxes
 *   - All counties with FIPS codes (dynamically loaded from TIGERweb)
 *   - DOT naming conventions
 *
 * Sources:
 *   - FCC FIPS codes: https://transition.fcc.gov/oet/info/maps/census/fips/fips.txt
 *   - US Census Bureau ANSI/FIPS codes
 *   - USDOT FIPS Reference Table
 *
 * USAGE:
 *   FIPSDatabase.getState('08')           → Colorado state info
 *   FIPSDatabase.getStateByAbbr('CO')     → Colorado state info
 *   FIPSDatabase.getAllStates()            → All 51 entries (50 states + DC)
 *   FIPSDatabase.getCounties('08')        → Colorado counties (from cache or TIGERweb)
 *   FIPSDatabase.lookupCounty('08', '035') → Douglas County info
 */

const FIPSDatabase = (() => {
    'use strict';

    // ─── County cache (populated on demand from TIGERweb or bundled data) ───
    const countyCache = {};

    // ─── All 50 US States + DC ───
    // Map centers are approximate geographic centroids; bounding boxes are approximate
    const STATES = {
        '01': {
            fips: '01', name: 'Alabama', abbr: 'AL',
            dotName: 'ALDOT', dotFullName: 'Alabama Department of Transportation',
            center: [32.739579, -86.843447], zoom: 7,
            bounds: { latMin: 30.22, latMax: 35.01, lonMin: -88.47, lonMax: -84.89 }
        },
        '02': {
            fips: '02', name: 'Alaska', abbr: 'AK',
            dotName: 'AKDOT', dotFullName: 'Alaska Department of Transportation and Public Facilities',
            center: [63.347356, -152.839733], zoom: 4,
            bounds: { latMin: 51.21, latMax: 71.39, lonMin: -179.15, lonMax: -129.98 }
        },
        '04': {
            fips: '04', name: 'Arizona', abbr: 'AZ',
            dotName: 'ADOT', dotFullName: 'Arizona Department of Transportation',
            center: [34.203936, -111.606345], zoom: 7,
            bounds: { latMin: 31.33, latMax: 37.00, lonMin: -114.81, lonMax: -109.04 }
        },
        '05': {
            fips: '05', name: 'Arkansas', abbr: 'AR',
            dotName: 'ArDOT', dotFullName: 'Arkansas Department of Transportation',
            center: [34.895526, -92.444626], zoom: 7,
            bounds: { latMin: 33.00, latMax: 36.50, lonMin: -94.62, lonMax: -89.64 }
        },
        '06': {
            fips: '06', name: 'California', abbr: 'CA',
            dotName: 'Caltrans', dotFullName: 'California Department of Transportation',
            center: [37.155177, -119.543418], zoom: 6,
            bounds: { latMin: 32.53, latMax: 42.01, lonMin: -124.48, lonMax: -114.13 }
        },
        '08': {
            fips: '08', name: 'Colorado', abbr: 'CO',
            dotName: 'CDOT', dotFullName: 'Colorado Department of Transportation',
            center: [38.993767, -105.508712], zoom: 7,
            bounds: { latMin: 36.99, latMax: 41.01, lonMin: -109.06, lonMax: -102.04 }
        },
        '09': {
            fips: '09', name: 'Connecticut', abbr: 'CT',
            dotName: 'CTDOT', dotFullName: 'Connecticut Department of Transportation',
            center: [41.579864, -72.746657], zoom: 9,
            bounds: { latMin: 40.95, latMax: 42.05, lonMin: -73.73, lonMax: -71.79 }
        },
        '10': {
            fips: '10', name: 'Delaware', abbr: 'DE',
            dotName: 'DelDOT', dotFullName: 'Delaware Department of Transportation',
            center: [38.998566, -75.441644], zoom: 9,
            bounds: { latMin: 38.45, latMax: 39.84, lonMin: -75.79, lonMax: -75.05 }
        },
        '11': {
            fips: '11', name: 'District of Columbia', abbr: 'DC',
            dotName: 'DDOT', dotFullName: 'District Department of Transportation',
            center: [38.904247, -77.016517], zoom: 12,
            bounds: { latMin: 38.79, latMax: 38.99, lonMin: -77.12, lonMax: -76.91 }
        },
        '12': {
            fips: '12', name: 'Florida', abbr: 'FL',
            dotName: 'FDOT', dotFullName: 'Florida Department of Transportation',
            center: [28.398978, -82.514300], zoom: 7,
            bounds: { latMin: 24.40, latMax: 31.00, lonMin: -87.63, lonMax: -80.03 }
        },
        '13': {
            fips: '13', name: 'Georgia', abbr: 'GA',
            dotName: 'GDOT', dotFullName: 'Georgia Department of Transportation',
            center: [32.629579, -83.423511], zoom: 7,
            bounds: { latMin: 30.36, latMax: 35.00, lonMin: -85.61, lonMax: -80.84 }
        },
        '15': {
            fips: '15', name: 'Hawaii', abbr: 'HI',
            dotName: 'HDOT', dotFullName: 'Hawaii Department of Transportation',
            center: [19.828167, -155.495042], zoom: 7,
            bounds: { latMin: 18.91, latMax: 22.24, lonMin: -160.25, lonMax: -154.81 }
        },
        '16': {
            fips: '16', name: 'Idaho', abbr: 'ID',
            dotName: 'ITD', dotFullName: 'Idaho Transportation Department',
            center: [44.348422, -114.558854], zoom: 6,
            bounds: { latMin: 41.99, latMax: 49.00, lonMin: -117.24, lonMax: -111.04 }
        },
        '17': {
            fips: '17', name: 'Illinois', abbr: 'IL',
            dotName: 'IDOT', dotFullName: 'Illinois Department of Transportation',
            center: [40.102875, -89.152611], zoom: 7,
            bounds: { latMin: 36.97, latMax: 42.51, lonMin: -91.51, lonMax: -87.02 }
        },
        '18': {
            fips: '18', name: 'Indiana', abbr: 'IN',
            dotName: 'INDOT', dotFullName: 'Indiana Department of Transportation',
            center: [39.901314, -86.291913], zoom: 7,
            bounds: { latMin: 37.77, latMax: 41.76, lonMin: -88.10, lonMax: -84.78 }
        },
        '19': {
            fips: '19', name: 'Iowa', abbr: 'IA',
            dotName: 'Iowa DOT', dotFullName: 'Iowa Department of Transportation',
            center: [42.070024, -93.493347], zoom: 7,
            bounds: { latMin: 40.38, latMax: 43.50, lonMin: -96.64, lonMax: -90.14 }
        },
        '20': {
            fips: '20', name: 'Kansas', abbr: 'KS',
            dotName: 'KDOT', dotFullName: 'Kansas Department of Transportation',
            center: [38.498546, -98.383430], zoom: 7,
            bounds: { latMin: 36.99, latMax: 40.00, lonMin: -102.05, lonMax: -94.59 }
        },
        '21': {
            fips: '21', name: 'Kentucky', abbr: 'KY',
            dotName: 'KYTC', dotFullName: 'Kentucky Transportation Cabinet',
            center: [37.533684, -85.292980], zoom: 7,
            bounds: { latMin: 36.50, latMax: 39.15, lonMin: -89.57, lonMax: -81.96 }
        },
        '22': {
            fips: '22', name: 'Louisiana', abbr: 'LA',
            dotName: 'LADOTD', dotFullName: 'Louisiana Department of Transportation and Development',
            center: [30.708319, -91.604621], zoom: 7,
            bounds: { latMin: 28.93, latMax: 33.02, lonMin: -94.04, lonMax: -88.82 }
        },
        '23': {
            fips: '23', name: 'Maine', abbr: 'ME',
            dotName: 'MaineDOT', dotFullName: 'Maine Department of Transportation',
            center: [45.409284, -68.666616], zoom: 7,
            bounds: { latMin: 42.98, latMax: 47.46, lonMin: -71.08, lonMax: -66.95 }
        },
        '24': {
            fips: '24', name: 'Maryland', abbr: 'MD',
            dotName: 'MDOT', dotFullName: 'Maryland Department of Transportation',
            center: [38.946658, -76.674494], zoom: 8,
            bounds: { latMin: 37.91, latMax: 39.72, lonMin: -79.49, lonMax: -75.05 }
        },
        '25': {
            fips: '25', name: 'Massachusetts', abbr: 'MA',
            dotName: 'MassDOT', dotFullName: 'Massachusetts Department of Transportation',
            center: [42.156520, -71.489592], zoom: 8,
            bounds: { latMin: 41.24, latMax: 42.89, lonMin: -73.51, lonMax: -69.93 }
        },
        '26': {
            fips: '26', name: 'Michigan', abbr: 'MI',
            dotName: 'MDOT', dotFullName: 'Michigan Department of Transportation',
            center: [44.844177, -85.660491], zoom: 7,
            bounds: { latMin: 41.70, latMax: 48.26, lonMin: -90.42, lonMax: -82.12 }
        },
        '27': {
            fips: '27', name: 'Minnesota', abbr: 'MN',
            dotName: 'MnDOT', dotFullName: 'Minnesota Department of Transportation',
            center: [46.315957, -94.199604], zoom: 7,
            bounds: { latMin: 43.50, latMax: 49.38, lonMin: -97.24, lonMax: -89.49 }
        },
        '28': {
            fips: '28', name: 'Mississippi', abbr: 'MS',
            dotName: 'MDOT', dotFullName: 'Mississippi Department of Transportation',
            center: [32.686471, -89.656138], zoom: 7,
            bounds: { latMin: 30.17, latMax: 35.00, lonMin: -91.66, lonMax: -88.10 }
        },
        '29': {
            fips: '29', name: 'Missouri', abbr: 'MO',
            dotName: 'MoDOT', dotFullName: 'Missouri Department of Transportation',
            center: [38.350750, -92.456783], zoom: 7,
            bounds: { latMin: 35.99, latMax: 40.61, lonMin: -95.77, lonMax: -89.10 }
        },
        '30': {
            fips: '30', name: 'Montana', abbr: 'MT',
            dotName: 'MDT', dotFullName: 'Montana Department of Transportation',
            center: [47.051177, -109.634817], zoom: 6,
            bounds: { latMin: 44.36, latMax: 49.00, lonMin: -116.05, lonMax: -104.04 }
        },
        '31': {
            fips: '31', name: 'Nebraska', abbr: 'NE',
            dotName: 'NDOT', dotFullName: 'Nebraska Department of Transportation',
            center: [41.543305, -99.811865], zoom: 7,
            bounds: { latMin: 39.99, latMax: 43.00, lonMin: -104.05, lonMax: -95.31 }
        },
        '32': {
            fips: '32', name: 'Nevada', abbr: 'NV',
            dotName: 'NDOT', dotFullName: 'Nevada Department of Transportation',
            center: [39.331093, -116.615147], zoom: 7,
            bounds: { latMin: 35.00, latMax: 42.00, lonMin: -120.01, lonMax: -114.04 }
        },
        '33': {
            fips: '33', name: 'New Hampshire', abbr: 'NH',
            dotName: 'NHDOT', dotFullName: 'New Hampshire Department of Transportation',
            center: [43.672794, -71.584189], zoom: 8,
            bounds: { latMin: 42.70, latMax: 45.31, lonMin: -72.56, lonMax: -70.70 }
        },
        '34': {
            fips: '34', name: 'New Jersey', abbr: 'NJ',
            dotName: 'NJDOT', dotFullName: 'New Jersey Department of Transportation',
            center: [40.107274, -74.665201], zoom: 8,
            bounds: { latMin: 38.93, latMax: 41.36, lonMin: -75.56, lonMax: -73.89 }
        },
        '35': {
            fips: '35', name: 'New Mexico', abbr: 'NM',
            dotName: 'NMDOT', dotFullName: 'New Mexico Department of Transportation',
            center: [34.434684, -106.131618], zoom: 7,
            bounds: { latMin: 31.33, latMax: 37.00, lonMin: -109.05, lonMax: -103.00 }
        },
        '36': {
            fips: '36', name: 'New York', abbr: 'NY',
            dotName: 'NYSDOT', dotFullName: 'New York State Department of Transportation',
            center: [42.913397, -75.596272], zoom: 7,
            bounds: { latMin: 40.50, latMax: 45.02, lonMin: -79.76, lonMax: -71.86 }
        },
        '37': {
            fips: '37', name: 'North Carolina', abbr: 'NC',
            dotName: 'NCDOT', dotFullName: 'North Carolina Department of Transportation',
            center: [35.539710, -79.130864], zoom: 7,
            bounds: { latMin: 33.84, latMax: 36.59, lonMin: -84.32, lonMax: -75.46 }
        },
        '38': {
            fips: '38', name: 'North Dakota', abbr: 'ND',
            dotName: 'NDDOT', dotFullName: 'North Dakota Department of Transportation',
            center: [47.442174, -100.460826], zoom: 7,
            bounds: { latMin: 45.94, latMax: 49.00, lonMin: -104.05, lonMax: -96.55 }
        },
        '39': {
            fips: '39', name: 'Ohio', abbr: 'OH',
            dotName: 'ODOT', dotFullName: 'Ohio Department of Transportation',
            center: [40.414930, -82.711997], zoom: 7,
            bounds: { latMin: 38.40, latMax: 41.98, lonMin: -84.82, lonMax: -80.52 }
        },
        '40': {
            fips: '40', name: 'Oklahoma', abbr: 'OK',
            dotName: 'ODOT', dotFullName: 'Oklahoma Department of Transportation',
            center: [35.590081, -97.486779], zoom: 7,
            bounds: { latMin: 33.62, latMax: 37.00, lonMin: -103.00, lonMax: -94.43 }
        },
        '41': {
            fips: '41', name: 'Oregon', abbr: 'OR',
            dotName: 'ODOT', dotFullName: 'Oregon Department of Transportation',
            center: [43.971712, -120.622958], zoom: 7,
            bounds: { latMin: 41.99, latMax: 46.29, lonMin: -124.57, lonMax: -116.46 }
        },
        '42': {
            fips: '42', name: 'Pennsylvania', abbr: 'PA',
            dotName: 'PennDOT', dotFullName: 'Pennsylvania Department of Transportation',
            center: [40.904604, -77.827523], zoom: 7,
            bounds: { latMin: 39.72, latMax: 42.27, lonMin: -80.52, lonMax: -74.69 }
        },
        '44': {
            fips: '44', name: 'Rhode Island', abbr: 'RI',
            dotName: 'RIDOT', dotFullName: 'Rhode Island Department of Transportation',
            center: [41.596485, -71.526490], zoom: 10,
            bounds: { latMin: 41.15, latMax: 42.02, lonMin: -71.86, lonMax: -71.12 }
        },
        '45': {
            fips: '45', name: 'South Carolina', abbr: 'SC',
            dotName: 'SCDOT', dotFullName: 'South Carolina Department of Transportation',
            center: [33.874178, -80.854264], zoom: 7,
            bounds: { latMin: 32.03, latMax: 35.22, lonMin: -83.35, lonMax: -78.54 }
        },
        '46': {
            fips: '46', name: 'South Dakota', abbr: 'SD',
            dotName: 'SDDOT', dotFullName: 'South Dakota Department of Transportation',
            center: [44.446796, -100.238176], zoom: 7,
            bounds: { latMin: 42.48, latMax: 45.95, lonMin: -104.06, lonMax: -96.44 }
        },
        '47': {
            fips: '47', name: 'Tennessee', abbr: 'TN',
            dotName: 'TDOT', dotFullName: 'Tennessee Department of Transportation',
            center: [35.858460, -86.349634], zoom: 7,
            bounds: { latMin: 34.98, latMax: 36.68, lonMin: -90.31, lonMax: -81.65 }
        },
        '48': {
            fips: '48', name: 'Texas', abbr: 'TX',
            dotName: 'TxDOT', dotFullName: 'Texas Department of Transportation',
            center: [31.434703, -99.281824], zoom: 6,
            bounds: { latMin: 25.84, latMax: 36.50, lonMin: -106.65, lonMax: -93.51 }
        },
        '49': {
            fips: '49', name: 'Utah', abbr: 'UT',
            dotName: 'UDOT', dotFullName: 'Utah Department of Transportation',
            center: [39.334993, -111.656333], zoom: 7,
            bounds: { latMin: 36.99, latMax: 42.00, lonMin: -114.05, lonMax: -109.04 }
        },
        '50': {
            fips: '50', name: 'Vermont', abbr: 'VT',
            dotName: 'VTrans', dotFullName: 'Vermont Agency of Transportation',
            center: [44.058954, -72.671017], zoom: 8,
            bounds: { latMin: 42.73, latMax: 45.02, lonMin: -73.44, lonMax: -71.47 }
        },
        '51': {
            fips: '51', name: 'Virginia', abbr: 'VA',
            dotName: 'VDOT', dotFullName: 'Virginia Department of Transportation',
            center: [37.522251, -78.668194], zoom: 7,
            bounds: { latMin: 36.54, latMax: 39.47, lonMin: -83.68, lonMax: -75.24 }
        },
        '53': {
            fips: '53', name: 'Washington', abbr: 'WA',
            dotName: 'WSDOT', dotFullName: 'Washington State Department of Transportation',
            center: [47.407324, -120.575800], zoom: 7,
            bounds: { latMin: 45.54, latMax: 49.00, lonMin: -124.85, lonMax: -116.92 }
        },
        '54': {
            fips: '54', name: 'West Virginia', abbr: 'WV',
            dotName: 'WVDOT', dotFullName: 'West Virginia Division of Highways',
            center: [38.647285, -80.618327], zoom: 7,
            bounds: { latMin: 37.20, latMax: 40.64, lonMin: -82.64, lonMax: -77.72 }
        },
        '55': {
            fips: '55', name: 'Wisconsin', abbr: 'WI',
            dotName: 'WisDOT', dotFullName: 'Wisconsin Department of Transportation',
            center: [44.630907, -89.709392], zoom: 7,
            bounds: { latMin: 42.49, latMax: 47.08, lonMin: -92.89, lonMax: -86.25 }
        },
        '56': {
            fips: '56', name: 'Wyoming', abbr: 'WY',
            dotName: 'WYDOT', dotFullName: 'Wyoming Department of Transportation',
            center: [42.989659, -107.544392], zoom: 7,
            bounds: { latMin: 40.99, latMax: 45.01, lonMin: -111.06, lonMax: -104.05 }
        }
    };

    // ─── Abbreviation → FIPS lookup ───
    const ABBR_TO_FIPS = {};
    for (const [fips, state] of Object.entries(STATES)) {
        ABBR_TO_FIPS[state.abbr] = fips;
    }

    // ─── Fetch counties from TIGERweb Census API ───
    async function fetchCountiesFromTIGERweb(stateFips) {
        const url = `https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Current/MapServer/86/query?where=STATE='${stateFips}'&outFields=STATE,COUNTY,NAME,CENTLAT,CENTLON,AREALAND&returnGeometry=false&f=json`;

        try {
            const response = await fetch(url);
            if (!response.ok) throw new Error(`TIGERweb HTTP ${response.status}`);
            const data = await response.json();

            if (!data.features || data.features.length === 0) {
                console.warn(`[FIPSDatabase] No counties returned from TIGERweb for state ${stateFips}`);
                return null;
            }

            const counties = {};
            data.features.forEach(f => {
                const attrs = f.attributes;
                const countyFips = attrs.COUNTY;
                const name = attrs.NAME;
                const id = name.toLowerCase()
                    .replace(/[^a-z0-9]+/g, '')  // Remove special chars
                    .replace(/county$/i, '');      // Remove "county" suffix

                counties[id || countyFips] = {
                    name: `${name} County`,
                    type: 'county',
                    fips: countyFips,
                    stateCountyFips: `${stateFips}${countyFips}`,
                    namePatterns: [name.toUpperCase(), name, `${name} County`],
                    mapCenter: [parseFloat(attrs.CENTLAT) || 0, parseFloat(attrs.CENTLON) || 0],
                    mapZoom: 10,
                    maintainsOwnRoads: true
                };
            });

            console.log(`[FIPSDatabase] Loaded ${Object.keys(counties).length} counties for state ${stateFips} from TIGERweb`);
            return counties;

        } catch (err) {
            console.error(`[FIPSDatabase] TIGERweb fetch failed for state ${stateFips}:`, err.message);
            return null;
        }
    }

    // ─── Public API ───
    return {
        /**
         * Get state info by FIPS code.
         * @param {string} stateFips - Two-digit state FIPS (e.g., '08')
         * @returns {Object|null} State info object
         */
        getState(stateFips) {
            const padded = String(stateFips).padStart(2, '0');
            return STATES[padded] || null;
        },

        /**
         * Get state info by abbreviation.
         * @param {string} abbr - Two-letter state abbreviation (e.g., 'CO')
         * @returns {Object|null} State info object
         */
        getStateByAbbr(abbr) {
            const fips = ABBR_TO_FIPS[abbr.toUpperCase()];
            return fips ? STATES[fips] : null;
        },

        /**
         * Get all states sorted alphabetically by name.
         * @returns {Object[]} Array of state objects
         */
        getAllStates() {
            return Object.values(STATES).sort((a, b) => a.name.localeCompare(b.name));
        },

        /**
         * Get FIPS code from abbreviation.
         * @param {string} abbr - Two-letter abbreviation
         * @returns {string|null} FIPS code
         */
        getFipsFromAbbr(abbr) {
            return ABBR_TO_FIPS[abbr.toUpperCase()] || null;
        },

        /**
         * Get counties for a state. Priority: cache > embedded USCountiesDB > TIGERweb API.
         * For states with custom config (CO, VA), merges with bundled jurisdiction data.
         * @param {string} stateFips - Two-digit state FIPS
         * @returns {Promise<Object>} County objects keyed by ID
         */
        async getCounties(stateFips) {
            const padded = String(stateFips).padStart(2, '0');

            // Return cached if available
            if (countyCache[padded]) {
                return countyCache[padded];
            }

            // Try embedded USCountiesDB first (no network required)
            if (typeof USCountiesDB !== 'undefined' && USCountiesDB.hasState(padded)) {
                const embedded = USCountiesDB.getCounties(padded);
                if (embedded && Object.keys(embedded).length > 0) {
                    countyCache[padded] = embedded;
                    console.log(`[FIPSDatabase] Loaded ${Object.keys(embedded).length} counties for state ${padded} from embedded database`);
                    return embedded;
                }
            }

            // Fallback: fetch from TIGERweb API
            const counties = await fetchCountiesFromTIGERweb(padded);
            if (counties) {
                countyCache[padded] = counties;
                return counties;
            }

            // Final fallback: return empty
            console.warn(`[FIPSDatabase] Could not load counties for state ${padded}`);
            return {};
        },

        /**
         * Pre-populate county cache (e.g., from bundled jurisdictions.json data).
         * @param {string} stateFips - Two-digit state FIPS
         * @param {Object} counties - County objects keyed by ID
         */
        cacheCounties(stateFips, counties) {
            const padded = String(stateFips).padStart(2, '0');
            countyCache[padded] = counties;
            console.log(`[FIPSDatabase] Cached ${Object.keys(counties).length} counties for state ${padded}`);
        },

        /**
         * Look up a single county by state and county FIPS.
         * @param {string} stateFips - Two-digit state FIPS
         * @param {string} countyFips - Three-digit county FIPS
         * @returns {Object|null} County info
         */
        lookupCounty(stateFips, countyFips) {
            const padded = String(stateFips).padStart(2, '0');
            const cached = countyCache[padded];
            if (!cached) return null;

            for (const county of Object.values(cached)) {
                if (county.fips === countyFips || county.stateCountyFips === `${padded}${countyFips}`) {
                    return county;
                }
            }
            return null;
        },

        /**
         * Check if counties are cached for a state (avoids async).
         * @param {string} stateFips
         * @returns {boolean}
         */
        hasCountiesLoaded(stateFips) {
            const padded = String(stateFips).padStart(2, '0');
            return !!countyCache[padded];
        },

        /**
         * Build a full geo config object for any state (used by StateAdapter).
         * This auto-generates the config that was previously hardcoded per state.
         * @param {string} stateFips - Two-digit FIPS
         * @param {Object} [counties] - Optional pre-loaded counties
         * @returns {Object} GeoConfig compatible with applyStateAdapterConfig()
         */
        buildGeoConfig(stateFips, counties) {
            const state = this.getState(stateFips);
            if (!state) return null;

            const jurisdictions = counties || countyCache[stateFips] || {};
            const defaultJurisdiction = Object.keys(jurisdictions)[0] || '';

            return {
                stateFips: state.fips,
                stateName: state.name,
                stateAbbr: state.abbr,
                coordinateBounds: {
                    latMin: state.bounds.latMin,
                    latMax: state.bounds.latMax,
                    lonMin: state.bounds.lonMin,
                    lonMax: state.bounds.lonMax
                },
                defaultJurisdiction: defaultJurisdiction,
                defaultMapCenter: state.center,
                defaultMapZoom: state.zoom,
                appSubtitle: `${state.name} Crash Analysis Tool`,
                districtLabel: 'Census Subdivisions',
                dotName: state.dotName,
                dotFullName: state.dotFullName,
                jurisdictions: jurisdictions
            };
        },

        /**
         * Get the number of states in the database.
         * @returns {number}
         */
        get stateCount() {
            return Object.keys(STATES).length;
        }
    };
})();

// Export for Node.js testing (ignored in browser)
if (typeof module !== 'undefined' && module.exports) {
    module.exports = FIPSDatabase;
}
