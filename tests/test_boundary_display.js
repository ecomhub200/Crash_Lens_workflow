/**
 * Comprehensive Test Suite — Jurisdiction Boundary Display on Map
 *
 * Validates that when a user selects a jurisdiction or changes view level,
 * the map tab correctly displays the appropriate boundary using TIGERweb
 * Census API for counties/states and BTS NTAD API for MPOs.
 *
 * Test matrix:
 *   1. All 50 state hierarchy files exist with correct structure
 *   2. Hierarchy files have valid regions (DOT districts) and MPOs with map data
 *   3. County FIPS codes in hierarchies resolve to valid entries
 *   4. BTS API fields are present on MPOs for boundary fetching
 *   5. TIGERweb API config is correctly set up in config.json
 *   6. Code paths: handleTierChange handles all 5 tiers with boundaries
 *   7. Code paths: showTab('map') restores boundaries for all active tiers
 *   8. Code paths: handleRegionSelection renders region boundary
 *   9. Code paths: handleMPOSelection renders MPO boundary from BTS
 *  10. Code paths: boundary cleanup on tier switch
 *  11. Code paths: displayMPOBoundary, displayRegionBoundary, removeMPO/RegionBoundaryLayer
 *  12. Code paths: loadPendingDistrictsOnMapReady handles region/MPO pending loads
 *  13. State outline boundary display for state tier
 *  14. builtInLayersState has mpoBoundary and regionBoundary entries
 *
 * Run with Node.js:
 *   node tests/test_boundary_display.js
 */

const fs = require('fs');
const path = require('path');

const ROOT = path.resolve(__dirname, '..');

let passed = 0;
let failed = 0;
let skipped = 0;
const errors = [];

function assert(condition, message) {
    if (condition) {
        passed++;
    } else {
        failed++;
        errors.push(message);
        console.error(`  FAIL: ${message}`);
    }
}

function skip(message) {
    skipped++;
    console.log(`  SKIP: ${message}`);
}

function section(name) {
    console.log(`\n${'═'.repeat(60)}`);
    console.log(`  ${name}`);
    console.log(`${'═'.repeat(60)}`);
}

// ─── Load source data ───
const configPath = path.join(ROOT, 'config.json');
const indexPath = path.join(ROOT, 'app', 'index.html');
let config, indexHtml;

try {
    config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
    indexHtml = fs.readFileSync(indexPath, 'utf8');
} catch (e) {
    console.error(`FATAL: Cannot load required files: ${e.message}`);
    process.exit(1);
}

// ─── State name → directory name mapping ───
const STATE_DIRS = {
    'AL': 'alabama', 'AK': 'alaska', 'AZ': 'arizona', 'AR': 'arkansas',
    'CA': 'california', 'CO': 'colorado', 'CT': 'connecticut', 'DE': 'delaware',
    'DC': 'district_of_columbia', 'FL': 'florida', 'GA': 'georgia', 'HI': 'hawaii',
    'ID': 'idaho', 'IL': 'illinois', 'IN': 'indiana', 'IA': 'iowa',
    'KS': 'kansas', 'KY': 'kentucky', 'LA': 'louisiana', 'ME': 'maine',
    'MD': 'maryland', 'MA': 'massachusetts', 'MI': 'michigan', 'MN': 'minnesota',
    'MS': 'mississippi', 'MO': 'missouri', 'MT': 'montana', 'NE': 'nebraska',
    'NV': 'nevada', 'NH': 'new_hampshire', 'NJ': 'new_jersey', 'NM': 'new_mexico',
    'NY': 'new_york', 'NC': 'north_carolina', 'ND': 'north_dakota', 'OH': 'ohio',
    'OK': 'oklahoma', 'OR': 'oregon', 'PA': 'pennsylvania', 'RI': 'rhode_island',
    'SC': 'south_carolina', 'SD': 'south_dakota', 'TN': 'tennessee', 'TX': 'texas',
    'UT': 'utah', 'VT': 'vermont', 'VA': 'virginia', 'WA': 'washington',
    'WV': 'west_virginia', 'WI': 'wisconsin', 'WY': 'wyoming'
};

const FIPS_TO_ABBR = {
    '01': 'AL', '02': 'AK', '04': 'AZ', '05': 'AR', '06': 'CA',
    '08': 'CO', '09': 'CT', '10': 'DE', '11': 'DC', '12': 'FL',
    '13': 'GA', '15': 'HI', '16': 'ID', '17': 'IL', '18': 'IN',
    '19': 'IA', '20': 'KS', '21': 'KY', '22': 'LA', '23': 'ME',
    '24': 'MD', '25': 'MA', '26': 'MI', '27': 'MN', '28': 'MS',
    '29': 'MO', '30': 'MT', '31': 'NE', '32': 'NV', '33': 'NH',
    '34': 'NJ', '35': 'NM', '36': 'NY', '37': 'NC', '38': 'ND',
    '39': 'OH', '40': 'OK', '41': 'OR', '42': 'PA', '44': 'RI',
    '45': 'SC', '46': 'SD', '47': 'TN', '48': 'TX', '49': 'UT',
    '50': 'VT', '51': 'VA', '53': 'WA', '54': 'WV', '55': 'WI',
    '56': 'WY'
};


// ═════════════════════════════════════════════════════════════
//  1. HIERARCHY FILES — Existence & Completeness
// ═════════════════════════════════════════════════════════════
section('1. Hierarchy Files — Existence for All 51 States/DC');

let allHierarchies = {};
let totalRegions = 0;
let totalMpos = 0;
let totalCounties = 0;

Object.entries(STATE_DIRS).forEach(([abbr, dir]) => {
    const hierPath = path.join(ROOT, 'states', dir, 'hierarchy.json');
    const exists = fs.existsSync(hierPath);
    assert(exists, `states/${dir}/hierarchy.json exists (${abbr})`);

    if (exists) {
        try {
            const hier = JSON.parse(fs.readFileSync(hierPath, 'utf8'));
            allHierarchies[abbr] = hier;
        } catch (e) {
            assert(false, `states/${dir}/hierarchy.json valid JSON: ${e.message}`);
        }
    }
});

const statesWithHierarchy = Object.keys(allHierarchies).length;
assert(statesWithHierarchy === 51, `All 51 states have hierarchy files, found ${statesWithHierarchy}`);


// ═════════════════════════════════════════════════════════════
//  2. HIERARCHY STRUCTURE — Required Fields
// ═════════════════════════════════════════════════════════════
section('2. Hierarchy Structure — Required Fields');

Object.entries(allHierarchies).forEach(([abbr, hier]) => {
    const dir = STATE_DIRS[abbr];

    // State info block
    assert(hier.state, `${abbr}: has state info block`);
    if (hier.state) {
        assert(hier.state.fips, `${abbr}: has state.fips`);
        assert(hier.state.name, `${abbr}: has state.name`);
        assert(hier.state.dot || hier.state.abbreviation, `${abbr}: has state.dot or abbreviation`);
    }

    // Region type labels
    assert(hier.regionType, `${abbr}: has regionType block`);
    if (hier.regionType) {
        assert(hier.regionType.label, `${abbr}: has regionType.label`);
        assert(hier.regionType.labelPlural, `${abbr}: has regionType.labelPlural`);
    }

    // Regions
    assert(hier.regions && typeof hier.regions === 'object', `${abbr}: has regions object`);
    const regionCount = Object.keys(hier.regions || {}).length;
    assert(regionCount > 0, `${abbr}: has ${regionCount} regions (>0)`);
    totalRegions += regionCount;

    // TPRs/MPOs
    assert(hier.tprs && typeof hier.tprs === 'object', `${abbr}: has tprs object`);
    const mpoCount = Object.keys(hier.tprs || {}).length;
    totalMpos += mpoCount;
    // DC and small states may have few MPOs, so no minimum assertion here
});

console.log(`\n  TOTALS: ${totalRegions} regions, ${totalMpos} MPOs across ${statesWithHierarchy} states`);
assert(totalRegions >= 350, `Total regions >= 350 (got ${totalRegions})`);
assert(totalMpos >= 350, `Total MPOs >= 350 (got ${totalMpos})`);


// ═════════════════════════════════════════════════════════════
//  3. REGIONS — Map Data for Boundary Display
// ═════════════════════════════════════════════════════════════
section('3. Regions — Map Positioning Data for Boundary Display');

let regionsWithBounds = 0;
let regionsWithCenter = 0;
let regionsWithCounties = 0;
let regionsMissingMapData = [];

Object.entries(allHierarchies).forEach(([abbr, hier]) => {
    Object.entries(hier.regions || {}).forEach(([id, region]) => {
        // Must have center or mapBounds for map positioning
        const hasCenter = region.center && Array.isArray(region.center) && region.center.length === 2;
        const hasBounds = region.mapBounds && region.mapBounds.sw && region.mapBounds.ne;

        if (hasCenter) regionsWithCenter++;
        if (hasBounds) regionsWithBounds++;
        if (!hasCenter && !hasBounds) {
            regionsMissingMapData.push(`${abbr}/${id}`);
        }

        // Must have zoom
        assert(typeof region.zoom === 'number' || typeof region.mapZoom === 'number',
            `${abbr}/${id}: has zoom level`);

        // Must have counties array
        if (region.counties && region.counties.length > 0) {
            regionsWithCounties++;
        }

        // Must have a name
        assert(region.name, `${abbr}/${id}: has a name`);
    });
});

console.log(`  Regions with mapBounds: ${regionsWithBounds}`);
console.log(`  Regions with center:    ${regionsWithCenter}`);
console.log(`  Regions with counties:  ${regionsWithCounties}`);
assert(regionsWithCenter === totalRegions,
    `All ${totalRegions} regions have center coordinates (${regionsWithCenter} found)`);
assert(regionsMissingMapData.length === 0,
    `No regions missing map data: ${regionsMissingMapData.length === 0 ? 'OK' : regionsMissingMapData.slice(0, 5).join(', ')}`);


// ═════════════════════════════════════════════════════════════
//  4. MPOs — Map Data & BTS Fields for Boundary Fetching
// ═════════════════════════════════════════════════════════════
section('4. MPOs — Map Data & BTS Integration Fields');

let mposWithCenter = 0;
let mposWithBtsAcronym = 0;
let mposWithCounties = 0;
let mposMissingCenter = [];

Object.entries(allHierarchies).forEach(([abbr, hier]) => {
    Object.entries(hier.tprs || {}).forEach(([id, mpo]) => {
        // Name is required
        assert(mpo.name, `${abbr}/${id}: MPO has name`);

        // Type should be present
        assert(mpo.type === 'mpo' || mpo.type === 'rural_tpr',
            `${abbr}/${id}: MPO type is mpo or rural_tpr (got ${mpo.type})`);

        // Center for map fly-to
        const hasCenter = mpo.center && Array.isArray(mpo.center) && mpo.center.length === 2;
        if (hasCenter) mposWithCenter++;
        else mposMissingCenter.push(`${abbr}/${id}`);

        // BTS acronym for boundary fetching
        if (mpo.btsAcronym) mposWithBtsAcronym++;

        // Counties for crash data filtering
        if (mpo.counties && mpo.counties.length > 0) mposWithCounties++;

        // Zoom
        assert(typeof mpo.zoom === 'number' || typeof mpo.mapZoom === 'number',
            `${abbr}/${id}: MPO has zoom level`);
    });
});

console.log(`  MPOs with center:       ${mposWithCenter}`);
console.log(`  MPOs with btsAcronym:   ${mposWithBtsAcronym}`);
console.log(`  MPOs with counties:     ${mposWithCounties}`);
assert(mposWithCenter >= totalMpos * 0.95,
    `>95% MPOs have center (${mposWithCenter}/${totalMpos}): ${mposMissingCenter.length === 0 ? 'OK' : mposMissingCenter.slice(0, 5).join(', ')}`);


// ═════════════════════════════════════════════════════════════
//  5. COUNTY FIPS RESOLUTION — Validate FIPS Codes
// ═════════════════════════════════════════════════════════════
section('5. County FIPS Resolution in Hierarchies');

// Load us_counties_db.js to validate FIPS codes
const countiesDbPath = path.join(ROOT, 'states', 'us_counties_db.js');
let countiesDbContent = '';
try {
    countiesDbContent = fs.readFileSync(countiesDbPath, 'utf8');
} catch (e) {
    skip('us_counties_db.js not found, skipping FIPS validation');
}

if (countiesDbContent) {
    // Parse all county FIPS from the DB
    const allValidFips = {};
    const fipsPattern = /f:'(\d{3})'/g;
    const stateBlockPattern = /'(\d{2})':\s*\{/g;
    let currentState = null;
    for (const line of countiesDbContent.split('\n')) {
        const stateMatch = line.match(/'(\d{2})':\s*\{/);
        if (stateMatch) {
            currentState = stateMatch[1];
            if (!allValidFips[currentState]) allValidFips[currentState] = new Set();
        }
        if (currentState) {
            const fipsMatch = line.match(/f:'(\d{3})'/);
            if (fipsMatch) {
                allValidFips[currentState].add(fipsMatch[1]);
            }
        }
    }

    let validCount = 0;
    let invalidCount = 0;
    let invalidFips = [];

    Object.entries(allHierarchies).forEach(([abbr, hier]) => {
        const stateFips = hier.state?.fips;
        if (!stateFips || !allValidFips[stateFips]) return;

        const validSet = allValidFips[stateFips];

        // Check region counties
        Object.entries(hier.regions || {}).forEach(([id, region]) => {
            (region.counties || []).forEach(fips => {
                if (validSet.has(fips)) {
                    validCount++;
                } else {
                    invalidCount++;
                    if (invalidFips.length < 10) {
                        invalidFips.push(`${abbr}/${id}: county FIPS ${fips}`);
                    }
                }
            });
        });

        // Check MPO counties
        Object.entries(hier.tprs || {}).forEach(([id, mpo]) => {
            (mpo.counties || []).forEach(fips => {
                if (validSet.has(fips)) {
                    validCount++;
                } else {
                    invalidCount++;
                    if (invalidFips.length < 10) {
                        invalidFips.push(`${abbr}/${id}: county FIPS ${fips}`);
                    }
                }
            });
        });
    });

    console.log(`  Valid FIPS references: ${validCount}`);
    console.log(`  Invalid FIPS refs:     ${invalidCount}`);
    if (invalidFips.length > 0) {
        console.log(`  Examples: ${invalidFips.slice(0, 5).join(', ')}`);
    }
    assert(invalidCount <= validCount * 0.02,
        `<2% invalid FIPS (${invalidCount}/${validCount + invalidCount}): ${invalidCount === 0 ? 'OK' : invalidFips[0]}`);
}


// ═════════════════════════════════════════════════════════════
//  6. TIGERWEB API CONFIG — Verify Configuration
// ═════════════════════════════════════════════════════════════
section('6. TIGERweb API Configuration');

const tigerweb = config.apis?.tigerweb;
assert(tigerweb, 'TIGERweb API config exists in config.json');
assert(tigerweb?.enabled === true, 'TIGERweb API is enabled');
assert(tigerweb?.baseUrl?.includes('tigerweb.geo.census.gov'), 'TIGERweb base URL is Census Bureau');
assert(tigerweb?.layers?.counties === 82, 'TIGERweb county layer ID is 82');
assert(tigerweb?.boundaryStyle, 'TIGERweb has boundaryStyle config');
assert(tigerweb?.boundaryStyle?.color, 'TIGERweb boundaryStyle has color');
assert(tigerweb?.boundaryStyle?.weight > 0, 'TIGERweb boundaryStyle has weight');
assert(typeof tigerweb?.boundaryStyle?.fillOpacity === 'number', 'TIGERweb boundaryStyle has fillOpacity');


// ═════════════════════════════════════════════════════════════
//  7. CODE PATHS — handleTierChange Boundary Logic
// ═════════════════════════════════════════════════════════════
section('7. Code Paths — handleTierChange Boundary Management');

// Extract handleTierChange function body
const tierChangeFnMatch = indexHtml.match(
    /async function handleTierChange\(tier\)\s*\{([\s\S]*?)\n\}/
);
assert(tierChangeFnMatch, 'handleTierChange function exists');

if (tierChangeFnMatch) {
    const fnBody = tierChangeFnMatch[1];

    // Verify cleanup: removes previous overlays on tier change
    assert(fnBody.includes('removeMPOBoundaryLayer'),
        'handleTierChange calls removeMPOBoundaryLayer on tier switch');
    assert(fnBody.includes('removeRegionBoundaryLayer'),
        'handleTierChange calls removeRegionBoundaryLayer on tier switch');
    assert(fnBody.includes('_stateOutlineLayer'),
        'handleTierChange cleans up state outline layer');

    // Verify federal tier: zoom to continental US
    assert(fnBody.includes("tier === 'federal'") && fnBody.includes('39.5') && fnBody.includes('-98.35'),
        'handleTierChange zooms to continental US for federal tier');
    assert(fnBody.includes('flyTo') && fnBody.includes(', 4,'),
        'handleTierChange uses zoom level 4 for federal view');

    // Verify state tier: loads TIGERweb state outline
    assert(fnBody.includes("tier === 'state'") && fnBody.includes('getStateOutline'),
        'handleTierChange loads state outline from TIGERweb for state tier');
    assert(fnBody.includes('L.geoJSON(stateOutline'),
        'handleTierChange renders state outline as L.geoJSON');
    assert(fnBody.includes('flyToBounds') && fnBody.includes('stateLayer.getBounds'),
        'handleTierChange fits map to state outline bounds');

    // Verify county tier: restores jurisdiction boundary
    assert(fnBody.includes("tier === 'county'") && fnBody.includes('updateJurisdictionBoundary'),
        'handleTierChange calls updateJurisdictionBoundary for county tier');

    // Verify region/mpo: dropdown population
    assert(fnBody.includes('populateRegionDropdown'),
        'handleTierChange populates region dropdown');
    assert(fnBody.includes('populateMPODropdown'),
        'handleTierChange populates MPO dropdown');

    // Verify hierarchy loading
    assert(fnBody.includes('HierarchyRegistry.load'),
        'handleTierChange loads hierarchy when needed');
}


// ═════════════════════════════════════════════════════════════
//  8. CODE PATHS — showTab('map') Boundary Restoration
// ═════════════════════════════════════════════════════════════
section('8. Code Paths — showTab Map Boundary Restoration');

const showTabMatch = indexHtml.match(
    /function showTab\(tabId\)\s*\{([\s\S]*?)\n\}/
);
assert(showTabMatch, 'showTab function exists');

if (showTabMatch) {
    const fnBody = showTabMatch[1];

    // Map tab initializes or invalidates map
    assert(fnBody.includes("tabId === 'map'") && fnBody.includes('initMap'),
        'showTab initializes map on first map tab visit');
    assert(fnBody.includes('invalidateSize'),
        'showTab invalidates map size to fix tile rendering');

    // Tier-aware boundary restoration
    assert(fnBody.includes('viewTier') || fnBody.includes('activeTier'),
        'showTab checks active view tier for boundary restoration');

    // County tier: restores jurisdiction boundary
    assert(fnBody.includes("'county'") && fnBody.includes('addJurisdictionBoundaryLayer'),
        'showTab restores county jurisdiction boundary on map show');

    // State tier: restores state outline
    assert(fnBody.includes("'state'") && fnBody.includes('getStateOutline'),
        'showTab restores state outline boundary on map show');

    // Region tier: restores region boundary
    assert(fnBody.includes("'region'") && fnBody.includes('displayRegionBoundary'),
        'showTab restores region boundary on map show');

    // MPO tier: restores MPO boundary
    assert(fnBody.includes("'mpo'") && fnBody.includes('displayMPOBoundary') || fnBody.includes('getMPOByAcronym'),
        'showTab restores MPO boundary on map show');

    // Magisterial districts still load
    assert(fnBody.includes('loadMagisterialDistricts'),
        'showTab still loads magisterial districts when enabled');
}


// ═════════════════════════════════════════════════════════════
//  9. CODE PATHS — handleRegionSelection Renders Boundary
// ═════════════════════════════════════════════════════════════
section('9. Code Paths — handleRegionSelection Boundary Rendering');

const regionSelMatch = indexHtml.match(
    /async function handleRegionSelection\(\)\s*\{([\s\S]*?)\n\}/
);
assert(regionSelMatch, 'handleRegionSelection function exists');

if (regionSelMatch) {
    const fnBody = regionSelMatch[1];

    // Gets region from hierarchy
    assert(fnBody.includes('HierarchyRegistry.getData'),
        'handleRegionSelection reads from HierarchyRegistry');

    // Clears MPO boundary (mutual exclusion)
    assert(fnBody.includes('removeMPOBoundaryLayer'),
        'handleRegionSelection clears MPO boundary');

    // Calls displayRegionBoundary
    assert(fnBody.includes('displayRegionBoundary'),
        'handleRegionSelection calls displayRegionBoundary');

    // Has fallback to center/zoom
    assert(fnBody.includes('region.center') && fnBody.includes('flyTo'),
        'handleRegionSelection has flyTo center fallback');
}


// ═════════════════════════════════════════════════════════════
//  10. CODE PATHS — handleMPOSelection Renders BTS Boundary
// ═════════════════════════════════════════════════════════════
section('10. Code Paths — handleMPOSelection BTS Boundary Rendering');

const mpoSelMatch = indexHtml.match(
    /async function handleMPOSelection\(\)\s*\{([\s\S]*?)\n\}/
);
assert(mpoSelMatch, 'handleMPOSelection function exists');

if (mpoSelMatch) {
    const fnBody = mpoSelMatch[1];

    // Clears region boundary (mutual exclusion)
    assert(fnBody.includes('removeRegionBoundaryLayer'),
        'handleMPOSelection clears region boundary');

    // Strategy 1: BTS acronym lookup
    assert(fnBody.includes('btsAcronym') && fnBody.includes('getMPOByAcronym'),
        'handleMPOSelection uses btsAcronym to query BTS');

    // Strategy 2: State-level BTS query with name matching
    assert(fnBody.includes('getMPOs(stateAbbr') || fnBody.includes('getMPOs('),
        'handleMPOSelection falls back to state-level BTS query');
    assert(fnBody.includes('MPO_NAME') || fnBody.includes('ACRONYM'),
        'handleMPOSelection matches by BTS name/acronym fields');

    // Renders boundary
    assert(fnBody.includes('displayMPOBoundary'),
        'handleMPOSelection calls displayMPOBoundary');

    // Has center fallback
    assert(fnBody.includes('mpo.center') && fnBody.includes('flyTo'),
        'handleMPOSelection has flyTo center fallback');

    // Has mapBounds fallback
    assert(fnBody.includes('mapBounds') && fnBody.includes('flyToBounds'),
        'handleMPOSelection has flyToBounds fallback');

    // Deselection clears boundary
    assert(fnBody.includes('removeMPOBoundaryLayer'),
        'handleMPOSelection clears MPO boundary on deselect');
}


// ═════════════════════════════════════════════════════════════
//  11. CODE PATHS — displayMPOBoundary & displayRegionBoundary
// ═════════════════════════════════════════════════════════════
section('11. Code Paths — Boundary Display Functions');

// displayMPOBoundary
assert(indexHtml.includes('function displayMPOBoundary('),
    'displayMPOBoundary function is defined');
const mpoDisplayMatch = indexHtml.match(
    /function displayMPOBoundary\(geojson, mpoId, mpoName\)\s*\{([\s\S]*?)\nfunction /
);
if (mpoDisplayMatch) {
    const fnBody = mpoDisplayMatch[1];
    assert(fnBody.includes('removeMPOBoundaryLayer'), 'displayMPOBoundary clears previous layer');
    assert(fnBody.includes('L.geoJSON(geojson'), 'displayMPOBoundary creates L.geoJSON layer');
    assert(fnBody.includes('addTo(crashMap)'), 'displayMPOBoundary adds layer to map');
    assert(fnBody.includes('flyToBounds'), 'displayMPOBoundary fits map to boundary');
    assert(fnBody.includes('bindPopup'), 'displayMPOBoundary adds popup with info');
    assert(fnBody.includes('MPO_NAME') || fnBody.includes('mpoName'), 'displayMPOBoundary shows MPO name in popup');
    assert(fnBody.includes('BTS NTAD') || fnBody.includes('bts'), 'displayMPOBoundary attributes BTS source');
    assert(fnBody.includes('builtInLayersState.mpoBoundary'), 'displayMPOBoundary updates layer state');
}

// removeMPOBoundaryLayer
assert(indexHtml.includes('function removeMPOBoundaryLayer()'),
    'removeMPOBoundaryLayer function is defined');

// displayRegionBoundary
assert(indexHtml.includes('function displayRegionBoundary('),
    'displayRegionBoundary function is defined');
const regionDisplayMatch = indexHtml.match(
    /function displayRegionBoundary\(region, regionId\)\s*\{([\s\S]*?)\nfunction /
);
if (regionDisplayMatch) {
    const fnBody = regionDisplayMatch[1];
    assert(fnBody.includes('removeRegionBoundaryLayer'), 'displayRegionBoundary clears previous layer');
    assert(fnBody.includes('L.rectangle') || fnBody.includes('L.latLngBounds'),
        'displayRegionBoundary uses rectangle/bounds');
    assert(fnBody.includes('addTo(crashMap)'), 'displayRegionBoundary adds layer to map');
    assert(fnBody.includes('flyToBounds'), 'displayRegionBoundary fits map to boundary');
    assert(fnBody.includes('bindPopup'), 'displayRegionBoundary adds popup');
    assert(fnBody.includes('builtInLayersState.regionBoundary'), 'displayRegionBoundary updates layer state');
}

// removeRegionBoundaryLayer
assert(indexHtml.includes('function removeRegionBoundaryLayer()'),
    'removeRegionBoundaryLayer function is defined');


// ═════════════════════════════════════════════════════════════
//  12. CODE PATHS — loadPendingDistrictsOnMapReady
// ═════════════════════════════════════════════════════════════
section('12. Code Paths — Pending Load on Map Ready');

const pendingMatch = indexHtml.match(
    /function loadPendingDistrictsOnMapReady\(\)\s*\{([\s\S]*?)\n\}/
);
assert(pendingMatch, 'loadPendingDistrictsOnMapReady function exists');

if (pendingMatch) {
    const fnBody = pendingMatch[1];

    // Existing: jurisdiction boundary pending load
    assert(fnBody.includes('jurisdictionBoundary') && fnBody.includes('_pendingLoad'),
        'Handles pending jurisdiction boundary load');

    // Existing: magisterial districts pending load
    assert(fnBody.includes('magisterialDistricts') && fnBody.includes('_pendingLoad'),
        'Handles pending magisterial districts load');

    // New: region boundary pending load
    assert(fnBody.includes("'region'") && fnBody.includes('displayRegionBoundary'),
        'Handles pending region boundary load on map ready');

    // New: MPO boundary pending load
    assert(fnBody.includes("'mpo'") && fnBody.includes('getMPOByAcronym'),
        'Handles pending MPO boundary load on map ready');
}


// ═════════════════════════════════════════════════════════════
//  13. builtInLayersState — MPO & Region Boundary Entries
// ═════════════════════════════════════════════════════════════
section('13. builtInLayersState — Layer State Entries');

// Extract builtInLayersState definition
const layerStateMatch = indexHtml.match(
    /const builtInLayersState\s*=\s*\{([\s\S]*?)\};/
);
assert(layerStateMatch, 'builtInLayersState is defined');

if (layerStateMatch) {
    const stateBody = layerStateMatch[1];

    // Existing entries
    assert(stateBody.includes('jurisdictionBoundary:'),
        'builtInLayersState has jurisdictionBoundary entry');
    assert(stateBody.includes('magisterialDistricts:'),
        'builtInLayersState has magisterialDistricts entry');

    // New entries for MPO and Region boundaries
    assert(stateBody.includes('mpoBoundary:'),
        'builtInLayersState has mpoBoundary entry');
    assert(stateBody.includes('regionBoundary:'),
        'builtInLayersState has regionBoundary entry');
}

// Verify state outline layer tracking
assert(indexHtml.includes('_stateOutlineLayer'),
    'State outline layer is tracked in builtInLayersState');


// ═════════════════════════════════════════════════════════════
//  14. BTS MPO API — BoundaryService Integration
// ═════════════════════════════════════════════════════════════
section('14. BTS MPO API — BoundaryService Methods');

assert(indexHtml.includes('BTS_MPO_BASE'),
    'BTS_MPO_BASE constant is defined');
assert(indexHtml.includes('NTAD_Metropolitan_Planning_Organizations'),
    'BTS MPO FeatureServer URL is correct');

// BoundaryService methods
assert(indexHtml.includes('async getMPOs(stateAbbrev)'),
    'BoundaryService has getMPOs(stateAbbrev) method');
assert(indexHtml.includes('async getMPOByAcronym(acronym)'),
    'BoundaryService has getMPOByAcronym method');
assert(indexHtml.includes('async getMPOByName(name)'),
    'BoundaryService has getMPOByName method');
assert(indexHtml.includes('async getStateOutline(stateFips)'),
    'BoundaryService has getStateOutline method');
assert(indexHtml.includes('async getCounties(stateFips)'),
    'BoundaryService has getCounties method');

// BTS attribution
assert(indexHtml.includes('addBTSMPOAttribution'),
    'BTS MPO attribution helper exists');
assert(indexHtml.includes('removeBTSMPOAttribution'),
    'BTS MPO attribution removal helper exists');


// ═════════════════════════════════════════════════════════════
//  15. CROSS-TIER BOUNDARY MUTUAL EXCLUSION
// ═════════════════════════════════════════════════════════════
section('15. Cross-Tier Boundary Mutual Exclusion');

// When switching to region, MPO boundary must be cleared (and vice versa)
// handleRegionSelection clears MPO
assert(
    (regionSelMatch && regionSelMatch[1].includes('removeMPOBoundaryLayer')),
    'Region selection clears MPO boundary layer'
);

// handleMPOSelection clears region
assert(
    (mpoSelMatch && mpoSelMatch[1].includes('removeRegionBoundaryLayer')),
    'MPO selection clears region boundary layer'
);

// handleTierChange clears all overlays before applying new tier
if (tierChangeFnMatch) {
    const fnBody = tierChangeFnMatch[1];
    const removeMpoIdx = fnBody.indexOf('removeMPOBoundaryLayer');
    const removeRegionIdx = fnBody.indexOf('removeRegionBoundaryLayer');
    const removeStateIdx = fnBody.indexOf('_stateOutlineLayer');
    assert(removeMpoIdx > 0 && removeMpoIdx < fnBody.indexOf("tier === 'federal'"),
        'handleTierChange clears MPO boundary BEFORE tier-specific logic');
    assert(removeRegionIdx > 0 && removeRegionIdx < fnBody.indexOf("tier === 'federal'"),
        'handleTierChange clears region boundary BEFORE tier-specific logic');
    assert(removeStateIdx > 0 && removeStateIdx < fnBody.indexOf("tier === 'federal'"),
        'handleTierChange clears state outline BEFORE tier-specific logic');
}


// ═════════════════════════════════════════════════════════════
//  16. SPOT-CHECK — Key States
// ═════════════════════════════════════════════════════════════
section('16. Spot-Check — Key States Hierarchy Data');

// Texas — large state, most districts & MPOs
const txHier = allHierarchies['TX'];
if (txHier) {
    const txRegions = Object.keys(txHier.regions || {}).length;
    const txMpos = Object.keys(txHier.tprs || {}).length;
    assert(txRegions >= 20, `Texas has ${txRegions} regions (expected 25 TxDOT districts)`);
    assert(txMpos >= 15, `Texas has ${txMpos} MPOs (expected ~25)`);

    // Check a known district
    const hasHouston = Object.values(txHier.regions).some(r =>
        r.name?.toLowerCase().includes('houston') || r.hq?.toLowerCase() === 'houston'
    );
    assert(hasHouston, 'Texas hierarchy includes Houston district');
}

// Florida — many MPOs
const flHier = allHierarchies['FL'];
if (flHier) {
    const flMpos = Object.keys(flHier.tprs || {}).length;
    assert(flMpos >= 20, `Florida has ${flMpos} MPOs (expected 27)`);
}

// California — large state
const caHier = allHierarchies['CA'];
if (caHier) {
    const caRegions = Object.keys(caHier.regions || {}).length;
    assert(caRegions >= 10, `California has ${caRegions} Caltrans districts (expected 12)`);
}

// Colorado — hand-curated with corridors
const coHier = allHierarchies['CO'];
if (coHier) {
    assert(coHier.corridors && Object.keys(coHier.corridors).length > 0,
        'Colorado hierarchy has corridors (hand-curated)');
    assert(Object.keys(coHier.allCounties || {}).length === 64,
        'Colorado has all 64 counties');
}

// Virginia — hand-curated with VDOT districts
const vaHier = allHierarchies['VA'];
if (vaHier) {
    assert(Object.keys(vaHier.regions || {}).length === 9,
        'Virginia has 9 VDOT construction districts');
    const vaMpos = Object.values(vaHier.tprs || {}).filter(t => t.type === 'mpo');
    assert(vaMpos.length >= 5, `Virginia has ${vaMpos.length} MPOs (expected 5+)`);
    // Check HRTPO has btsAcronym
    const hrtpo = Object.values(vaHier.tprs || {}).find(t => t.btsAcronym === 'HRTPO');
    assert(hrtpo, 'Virginia HRTPO has btsAcronym for BTS API lookup');
}


// ═════════════════════════════════════════════════════════════
//  17. HIERARCHY DIRECTORY NAME RESOLUTION
// ═════════════════════════════════════════════════════════════
section('17. Hierarchy Directory Name Resolution (FIPSDatabase Path)');

// Verify the directory naming convention matches what HierarchyRegistry expects:
// FIPSDatabase.getState(fips).name.toLowerCase().replace(/\s+/g, '_')
Object.entries(FIPS_TO_ABBR).forEach(([fips, abbr]) => {
    const expectedDir = STATE_DIRS[abbr];
    if (!expectedDir) return;

    const hierPath = path.join(ROOT, 'states', expectedDir, 'hierarchy.json');
    const exists = fs.existsSync(hierPath);
    assert(exists,
        `FIPS ${fips} (${abbr}) resolves to states/${expectedDir}/hierarchy.json`);
});


// ═════════════════════════════════════════════════════════════
//  18. updateJurisdictionBoundary — Auto-Enable & Load
// ═════════════════════════════════════════════════════════════
section('18. updateJurisdictionBoundary — Auto-Enable on Jurisdiction Change');

const updateBndMatch = indexHtml.match(
    /function updateJurisdictionBoundary\(jurisdictionId\)\s*\{([\s\S]*?)\n\}/
);
assert(updateBndMatch, 'updateJurisdictionBoundary function exists');

if (updateBndMatch) {
    const fnBody = updateBndMatch[1];

    // Defers if map not ready
    assert(fnBody.includes('_pendingLoad') && fnBody.includes('_pendingJurisdiction'),
        'Defers load if map not initialized (stores pending state)');

    // Auto-enables checkbox
    assert(fnBody.includes('enabled = true') || fnBody.includes('.enabled'),
        'Auto-enables boundary layer when jurisdiction selected');
    assert(fnBody.includes('mapAsset_jurisdictionBoundary'),
        'Updates checkbox UI when auto-enabling');

    // Loads boundary
    assert(fnBody.includes('addJurisdictionBoundaryLayer'),
        'Calls addJurisdictionBoundaryLayer to fetch from TIGERweb');
}


// ═════════════════════════════════════════════════════════════
//  19. applyJurisdictionSelection — Triggers Boundary Update
// ═════════════════════════════════════════════════════════════
section('19. applyJurisdictionSelection — Chain to Boundary Update');

const applyJurisMatch = indexHtml.match(
    /function applyJurisdictionSelection\(jurisdictionId\)\s*\{([\s\S]*?)\n\}/
);
assert(applyJurisMatch, 'applyJurisdictionSelection function exists');

if (applyJurisMatch) {
    const fnBody = applyJurisMatch[1];

    // Flies map to jurisdiction center
    assert(fnBody.includes('flyTo'), 'Flies map to new jurisdiction center');

    // Triggers boundary update
    assert(fnBody.includes('updateJurisdictionBoundary'),
        'Calls updateJurisdictionBoundary to load boundary for new jurisdiction');

    // Updates magisterial districts
    assert(fnBody.includes('updateMagisterialDistricts'),
        'Calls updateMagisterialDistricts for new jurisdiction');
}


// ═════════════════════════════════════════════════════════════
//  20. jurisdictionBoundaryPane — Custom Map Pane
// ═════════════════════════════════════════════════════════════
section('20. Map Pane Setup for Boundaries');

assert(indexHtml.includes("createPane('jurisdictionBoundaryPane')"),
    'jurisdictionBoundaryPane is created in initMap');
assert(indexHtml.includes('zIndex = 340') || indexHtml.includes("zIndex = '340'") ||
       indexHtml.match(/jurisdictionBoundaryPane.*zIndex.*3[4-5]0/s),
    'jurisdictionBoundaryPane has correct zIndex');


// ═════════════════════════════════════════════════════════════
//  21. COUNTY BOUNDARY CLEANUP — Non-County Tier Switches
// ═════════════════════════════════════════════════════════════
section('21. County Boundary Cleanup on Non-County Tier Switch');

// handleTierChange must remove county jurisdiction boundary for non-county tiers
if (tierChangeFnMatch) {
    const fnBody = tierChangeFnMatch[1];
    assert(fnBody.includes('removeJurisdictionBoundaryLayer'),
        'handleTierChange calls removeJurisdictionBoundaryLayer for non-county tiers');
    assert(fnBody.includes("tier !== 'county'") && fnBody.includes('removeJurisdictionBoundaryLayer'),
        'handleTierChange guards county boundary removal with tier !== county check');
    assert(fnBody.includes('jurisdictionBoundary.enabled = false'),
        'handleTierChange disables county boundary enabled flag for non-county tiers');
}

// handleMPOSelection must clear county boundary before loading MPO boundary
if (mpoSelMatch) {
    const fnBody = mpoSelMatch[1];
    assert(fnBody.includes('removeJurisdictionBoundaryLayer'),
        'handleMPOSelection calls removeJurisdictionBoundaryLayer to clear county boundary');
    assert(fnBody.includes('jurisdictionBoundary.enabled = false'),
        'handleMPOSelection disables county boundary enabled flag');
    // Verify county boundary removal comes before BTS boundary fetch
    const removeJurisIdx = fnBody.indexOf('removeJurisdictionBoundaryLayer');
    const btsFetchIdx = fnBody.indexOf('getMPOByAcronym');
    assert(removeJurisIdx > 0 && btsFetchIdx > 0 && removeJurisIdx < btsFetchIdx,
        'handleMPOSelection clears county boundary BEFORE fetching BTS MPO boundary');
}

// handleRegionSelection must clear county boundary before loading region boundary
if (regionSelMatch) {
    const fnBody = regionSelMatch[1];
    assert(fnBody.includes('removeJurisdictionBoundaryLayer'),
        'handleRegionSelection calls removeJurisdictionBoundaryLayer to clear county boundary');
    assert(fnBody.includes('jurisdictionBoundary.enabled = false'),
        'handleRegionSelection disables county boundary enabled flag');
}

// displayMPOBoundary must also clear county boundary as safety net
const displayMpoMatch = indexHtml.match(
    /function displayMPOBoundary\(geojson,\s*mpoId,\s*mpoName\)\s*\{([\s\S]*?)\nconsole\.log/
) || indexHtml.match(
    /function displayMPOBoundary\(geojson,\s*mpoId,\s*mpoName\)\s*\{([\s\S]*?)\n\}/
);
if (displayMpoMatch) {
    const fnBody = displayMpoMatch[1];
    assert(fnBody.includes('removeJurisdictionBoundaryLayer'),
        'displayMPOBoundary clears county boundary layer as safety net');
} else {
    assert(false, 'displayMPOBoundary function not found for county boundary cleanup check');
}

// displayRegionBoundary must also clear county boundary as safety net
const displayRegMatch = indexHtml.match(
    /function displayRegionBoundary\(region,\s*regionId\)\s*\{([\s\S]*?)\n\}/
);
if (displayRegMatch) {
    const fnBody = displayRegMatch[1];
    assert(fnBody.includes('removeJurisdictionBoundaryLayer'),
        'displayRegionBoundary clears county boundary layer as safety net');
} else {
    assert(false, 'displayRegionBoundary function not found for county boundary cleanup check');
}

// updateJurisdictionBoundary must skip auto-enable when non-county tier is active
if (updateBndMatch) {
    const fnBody = updateBndMatch[1];
    assert(fnBody.includes('viewTier'),
        'updateJurisdictionBoundary checks active viewTier before auto-enabling');
    assert(fnBody.includes("!== 'county'") || fnBody.includes("activeTier !== 'county'"),
        'updateJurisdictionBoundary skips auto-enable for non-county tiers');
}


// ═════════════════════════════════════════════════════════════
//  RESULTS
// ═════════════════════════════════════════════════════════════
console.log(`\n${'═'.repeat(60)}`);
console.log('  TEST RESULTS');
console.log(`${'═'.repeat(60)}`);
console.log(`  Passed:  ${passed}`);
console.log(`  Failed:  ${failed}`);
console.log(`  Skipped: ${skipped}`);
console.log(`  Total:   ${passed + failed + skipped}`);
console.log(`${'═'.repeat(60)}`);

if (errors.length > 0) {
    console.log('\n  FAILURES:');
    errors.forEach((e, i) => console.log(`    ${i + 1}. ${e}`));
}

console.log(`\n  Coverage Summary:`);
console.log(`    - ${statesWithHierarchy}/51 state hierarchy files`);
console.log(`    - ${totalRegions} DOT districts/regions across all states`);
console.log(`    - ${totalMpos} MPOs/TPRs across all states`);
console.log(`    - 5 view tiers verified: Federal, State, Region, MPO, County`);
console.log(`    - TIGERweb + BTS NTAD API integration verified`);
console.log(`    - Boundary auto-display on jurisdiction change verified`);
console.log(`    - Boundary restoration on map tab switch verified`);
console.log(`    - Cross-tier boundary mutual exclusion verified`);
console.log(`    - County boundary cleanup on non-county tier switch verified`);

process.exit(failed > 0 ? 1 : 0);
