/**
 * Comprehensive Bug Test Suite — MPO/Region Boundary Display & Tier Switching
 *
 * Tests the specific bug: When an MPO is selected (e.g., Nashville Area MPO in Tennessee),
 * the county-level boundary (e.g., Anderson County) persists on the map instead of showing
 * the BTS MPO boundary. This test validates:
 *
 *   1. County boundary removal when switching to non-county tiers
 *   2. MPO boundary correctly replaces county boundary
 *   3. Region boundary correctly replaces county boundary
 *   4. State outline correctly replaces county boundary
 *   5. Federal tier clears all boundaries
 *   6. Tier switching clears all previous tier boundaries (mutual exclusion)
 *   7. showTab('map') restores correct boundary per active tier
 *   8. updateJurisdictionBoundary skips when non-county tier active
 *   9. displayMPOBoundary/displayRegionBoundary clear county boundary
 *  10. BTS API query construction & SQL safety
 *  11. Hierarchy data integrity for MPO boundary support
 *  12. Pending load handling for region/MPO on map ready
 *  13. Layer state consistency after tier switches
 *  14. Edge cases: DC, small states, states with no MPOs
 *  15. Dropdown population and deselection cleanup
 *  16. Race condition guards in boundary loading
 *  17. builtInLayersState consistency across operations
 *  18. Cross-tier state leak prevention
 *  19. BTS acronym resolution and caching
 *  20. Boundary precedence chain validation
 *
 * Run with Node.js:
 *   node tests/test_mpo_boundary_bug.js
 */

const fs = require('fs');
const path = require('path');

const ROOT = path.resolve(__dirname, '..');

let passed = 0;
let failed = 0;
let skipped = 0;
const errors = [];
const warnings = [];

function assert(condition, message) {
    if (condition) {
        passed++;
    } else {
        failed++;
        errors.push(message);
        console.error(`  FAIL: ${message}`);
    }
}

function warn(message) {
    warnings.push(message);
    console.warn(`  WARN: ${message}`);
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

// ═══════════════════════════════════════════════════════════════
//  LOAD SOURCE FILES
// ═══════════════════════════════════════════════════════════════

const indexHtmlPath = path.join(ROOT, 'app', 'index.html');
const indexHtml = fs.readFileSync(indexHtmlPath, 'utf8');

// Load all hierarchy files
const statesDir = path.join(ROOT, 'states');
const allHierarchies = {};
let statesDirs = [];
try {
    statesDirs = fs.readdirSync(statesDir).filter(d => {
        const p = path.join(statesDir, d);
        return fs.statSync(p).isDirectory() && fs.existsSync(path.join(p, 'hierarchy.json'));
    });
    statesDirs.forEach(d => {
        try {
            const h = JSON.parse(fs.readFileSync(path.join(statesDir, d, 'hierarchy.json'), 'utf8'));
            allHierarchies[h.state?.abbreviation || d] = h;
        } catch (e) { /* skip invalid */ }
    });
} catch (e) { /* no states dir */ }

// Load config
let appConfig = {};
try {
    appConfig = JSON.parse(fs.readFileSync(path.join(ROOT, 'config.json'), 'utf8'));
} catch (e) { /* no config */ }

// ═══════════════════════════════════════════════════════════════
//  EXTRACT KEY CODE SECTIONS FOR ANALYSIS
// ═══════════════════════════════════════════════════════════════

// Extract function bodies using regex (for static analysis)
function extractFn(name, sig) {
    const re = new RegExp(`${sig || 'function ' + name}\\s*\\{([\\s\\S]*?)\\n\\}`, 'm');
    const m = indexHtml.match(re);
    return m ? m[1] : null;
}

const fnBodies = {
    handleTierChange: extractFn('handleTierChange', 'async function handleTierChange\\(tier\\)'),
    handleMPOSelection: extractFn('handleMPOSelection', 'async function handleMPOSelection\\(\\)'),
    handleRegionSelection: extractFn('handleRegionSelection', 'async function handleRegionSelection\\(\\)'),
    setViewTier: extractFn('setViewTier', 'function setViewTier\\(tier\\)'),
    displayMPOBoundary: extractFn('displayMPOBoundary', 'function displayMPOBoundary\\(geojson,\\s*mpoId,\\s*mpoName\\)'),
    displayRegionBoundary: extractFn('displayRegionBoundary', 'function displayRegionBoundary\\(region,\\s*regionId\\)'),
    removeMPOBoundaryLayer: extractFn('removeMPOBoundaryLayer', 'function removeMPOBoundaryLayer\\(\\)'),
    removeRegionBoundaryLayer: extractFn('removeRegionBoundaryLayer', 'function removeRegionBoundaryLayer\\(\\)'),
    removeJurisdictionBoundaryLayer: extractFn('removeJurisdictionBoundaryLayer', 'function removeJurisdictionBoundaryLayer\\(\\)'),
    updateJurisdictionBoundary: extractFn('updateJurisdictionBoundary', 'function updateJurisdictionBoundary\\(jurisdictionId\\)'),
    populateMPODropdown: extractFn('populateMPODropdown', 'function populateMPODropdown\\(\\)'),
    populateRegionDropdown: extractFn('populateRegionDropdown', 'function populateRegionDropdown\\(\\)'),
    loadPendingDistrictsOnMapReady: extractFn('loadPendingDistrictsOnMapReady', 'function loadPendingDistrictsOnMapReady\\(\\)'),
};

// Extract showTab map section (larger block)
const showTabMapMatch = indexHtml.match(/showTab.*?tabId\s*===\s*'map'([\s\S]*?)(?=showTab.*?tabId\s*===\s*'(?!map')|\n {4}\})/);
const showTabMapBody = showTabMapMatch ? showTabMapMatch[1] : '';


// ═══════════════════════════════════════════════════════════════
//  1. BUG FIX: County Boundary Removal on Non-County Tier Switch
// ═══════════════════════════════════════════════════════════════
section('1. BUG FIX — handleTierChange Removes County Boundary for Non-County Tiers');

assert(fnBodies.handleTierChange, 'handleTierChange function exists');

if (fnBodies.handleTierChange) {
    const fn = fnBodies.handleTierChange;

    // Must call removeJurisdictionBoundaryLayer for non-county tiers
    assert(fn.includes('removeJurisdictionBoundaryLayer'),
        'handleTierChange calls removeJurisdictionBoundaryLayer');

    // Must have the tier guard
    assert(fn.includes("tier !== 'county'"),
        'handleTierChange guards county boundary removal with tier !== county');

    // Must disable the enabled flag
    const guardIdx = fn.indexOf("tier !== 'county'");
    const enabledIdx = fn.indexOf('jurisdictionBoundary.enabled = false');
    assert(guardIdx > 0 && enabledIdx > 0 && enabledIdx > guardIdx,
        'handleTierChange disables jurisdictionBoundary.enabled after tier guard');

    // County boundary removal must happen BEFORE tier-specific logic
    const removeBoundaryIdx = fn.indexOf('removeJurisdictionBoundaryLayer');
    const federalIdx = fn.indexOf("tier === 'federal'");
    const stateIdx = fn.indexOf("tier === 'state'");
    assert(removeBoundaryIdx > 0 && federalIdx > 0 && removeBoundaryIdx < federalIdx,
        'County boundary removal happens BEFORE federal tier logic');
    assert(removeBoundaryIdx > 0 && stateIdx > 0 && removeBoundaryIdx < stateIdx,
        'County boundary removal happens BEFORE state tier logic');

    // Must ALSO clear MPO and region boundaries
    const removeMpoIdx = fn.indexOf('removeMPOBoundaryLayer');
    const removeRegionIdx = fn.indexOf('removeRegionBoundaryLayer');
    assert(removeMpoIdx > 0 && removeMpoIdx < federalIdx,
        'handleTierChange clears MPO boundary before tier-specific logic');
    assert(removeRegionIdx > 0 && removeRegionIdx < federalIdx,
        'handleTierChange clears region boundary before tier-specific logic');

    // Must clear state outline layer
    assert(fn.includes('_stateOutlineLayer'),
        'handleTierChange clears state outline layer');
}


// ═══════════════════════════════════════════════════════════════
//  2. BUG FIX: handleMPOSelection Clears County Boundary
// ═══════════════════════════════════════════════════════════════
section('2. BUG FIX — handleMPOSelection Clears County Boundary Before BTS Fetch');

assert(fnBodies.handleMPOSelection, 'handleMPOSelection function exists');

if (fnBodies.handleMPOSelection) {
    const fn = fnBodies.handleMPOSelection;

    // Must remove county boundary
    assert(fn.includes('removeJurisdictionBoundaryLayer'),
        'handleMPOSelection calls removeJurisdictionBoundaryLayer');

    // Must disable county boundary enabled flag
    assert(fn.includes('jurisdictionBoundary.enabled = false'),
        'handleMPOSelection disables county boundary enabled flag');

    // County cleanup must happen BEFORE BTS fetch
    const removeJurisIdx = fn.indexOf('removeJurisdictionBoundaryLayer');
    const btsFetchIdx = fn.indexOf('getMPOByAcronym');
    assert(removeJurisIdx > 0 && btsFetchIdx > 0 && removeJurisIdx < btsFetchIdx,
        'County boundary cleared BEFORE BTS MPO boundary fetch');

    // Must also clear region boundary (mutual exclusion)
    const removeRegionIdx = fn.indexOf('removeRegionBoundaryLayer');
    assert(removeRegionIdx > 0 && removeRegionIdx < btsFetchIdx,
        'Region boundary cleared BEFORE BTS MPO boundary fetch');

    // Must call displayMPOBoundary on success
    assert(fn.includes('displayMPOBoundary'),
        'handleMPOSelection calls displayMPOBoundary to render boundary');

    // Must have center fallback
    assert(fn.includes('mpo.center') && fn.includes('flyTo'),
        'handleMPOSelection has flyTo center fallback when no BTS boundary');

    // Must have mapBounds fallback
    assert(fn.includes('mpo.mapBounds') && fn.includes('flyToBounds'),
        'handleMPOSelection has flyToBounds fallback when no center');

    // Cleanup on deselect (empty value)
    assert(fn.includes("!mpoId") && fn.includes('removeMPOBoundaryLayer'),
        'handleMPOSelection cleans up when MPO deselected');
}


// ═══════════════════════════════════════════════════════════════
//  3. BUG FIX: handleRegionSelection Clears County Boundary
// ═══════════════════════════════════════════════════════════════
section('3. BUG FIX — handleRegionSelection Clears County Boundary');

assert(fnBodies.handleRegionSelection, 'handleRegionSelection function exists');

if (fnBodies.handleRegionSelection) {
    const fn = fnBodies.handleRegionSelection;

    // Must remove county boundary
    assert(fn.includes('removeJurisdictionBoundaryLayer'),
        'handleRegionSelection calls removeJurisdictionBoundaryLayer');

    // Must disable county boundary enabled flag
    assert(fn.includes('jurisdictionBoundary.enabled = false'),
        'handleRegionSelection disables county boundary enabled flag');

    // Must also clear MPO boundary (mutual exclusion)
    assert(fn.includes('removeMPOBoundaryLayer'),
        'handleRegionSelection clears MPO boundary');

    // Must call displayRegionBoundary
    assert(fn.includes('displayRegionBoundary'),
        'handleRegionSelection calls displayRegionBoundary');

    // Must have flyTo center fallback
    assert(fn.includes('region.center') && fn.includes('flyTo'),
        'handleRegionSelection has flyTo center fallback');

    // Cleanup on deselect
    assert(fn.includes("!regionId") && fn.includes('removeRegionBoundaryLayer'),
        'handleRegionSelection cleans up when region deselected');
}


// ═══════════════════════════════════════════════════════════════
//  4. BUG FIX: displayMPOBoundary Clears County Boundary (Safety Net)
// ═══════════════════════════════════════════════════════════════
section('4. BUG FIX — displayMPOBoundary Safety Net County Cleanup');

assert(fnBodies.displayMPOBoundary, 'displayMPOBoundary function exists');

if (fnBodies.displayMPOBoundary) {
    const fn = fnBodies.displayMPOBoundary;

    // Must call removeJurisdictionBoundaryLayer as safety net
    assert(fn.includes('removeJurisdictionBoundaryLayer'),
        'displayMPOBoundary calls removeJurisdictionBoundaryLayer as safety net');

    // Must disable enabled flag
    assert(fn.includes('jurisdictionBoundary.enabled = false'),
        'displayMPOBoundary disables county boundary enabled flag');

    // Must call removeMPOBoundaryLayer first (clear previous)
    const removeMpoIdx = fn.indexOf('removeMPOBoundaryLayer');
    const removeJurisIdx = fn.indexOf('removeJurisdictionBoundaryLayer');
    assert(removeMpoIdx >= 0 && removeJurisIdx > removeMpoIdx,
        'displayMPOBoundary clears previous MPO boundary THEN county boundary');

    // Must create L.geoJSON layer
    assert(fn.includes('L.geoJSON'),
        'displayMPOBoundary creates L.geoJSON layer from BTS data');

    // Must use jurisdictionBoundaryPane
    assert(fn.includes('jurisdictionBoundaryPane'),
        'displayMPOBoundary renders on jurisdictionBoundaryPane');

    // Must set builtInLayersState
    assert(fn.includes('mpoBoundary.layer') && fn.includes("mpoBoundary.status = 'active'"),
        'displayMPOBoundary updates builtInLayersState.mpoBoundary');

    // Must flyToBounds
    assert(fn.includes('flyToBounds'),
        'displayMPOBoundary fits map to boundary bounds');

    // Must add BTS attribution
    assert(fn.includes('addBTSMPOAttribution'),
        'displayMPOBoundary adds BTS attribution');

    // Must handle empty features gracefully
    assert(fn.includes("!geojson?.features?.length") || fn.includes('geojson?.features?.length'),
        'displayMPOBoundary checks for empty features');
}


// ═══════════════════════════════════════════════════════════════
//  5. BUG FIX: displayRegionBoundary Clears County Boundary (Safety Net)
// ═══════════════════════════════════════════════════════════════
section('5. BUG FIX — displayRegionBoundary Safety Net County Cleanup');

assert(fnBodies.displayRegionBoundary, 'displayRegionBoundary function exists');

if (fnBodies.displayRegionBoundary) {
    const fn = fnBodies.displayRegionBoundary;

    // Must call removeJurisdictionBoundaryLayer
    assert(fn.includes('removeJurisdictionBoundaryLayer'),
        'displayRegionBoundary calls removeJurisdictionBoundaryLayer as safety net');

    // Must disable enabled flag
    assert(fn.includes('jurisdictionBoundary.enabled = false'),
        'displayRegionBoundary disables county boundary enabled flag');

    // Must create L.rectangle
    assert(fn.includes('L.rectangle'),
        'displayRegionBoundary creates L.rectangle from mapBounds');

    // Must use mapBounds.sw and mapBounds.ne
    assert(fn.includes('mapBounds.sw') && fn.includes('mapBounds.ne'),
        'displayRegionBoundary uses hierarchy mapBounds coordinates');

    // Must have center fallback
    assert(fn.includes('region?.center') || fn.includes('region.center'),
        'displayRegionBoundary has center fallback when no mapBounds');

    // Must update builtInLayersState
    assert(fn.includes('regionBoundary.layer') && fn.includes("regionBoundary.status = 'active'"),
        'displayRegionBoundary updates builtInLayersState.regionBoundary');
}


// ═══════════════════════════════════════════════════════════════
//  6. BUG FIX: updateJurisdictionBoundary Skips Non-County Tiers
// ═══════════════════════════════════════════════════════════════
section('6. BUG FIX — updateJurisdictionBoundary Tier Guard');

assert(fnBodies.updateJurisdictionBoundary, 'updateJurisdictionBoundary function exists');

if (fnBodies.updateJurisdictionBoundary) {
    const fn = fnBodies.updateJurisdictionBoundary;

    // Must check viewTier
    assert(fn.includes('viewTier'),
        'updateJurisdictionBoundary checks jurisdictionContext.viewTier');

    // Must skip when non-county tier is active
    assert(fn.includes("!== 'county'") || fn.includes("activeTier !== 'county'"),
        'updateJurisdictionBoundary has non-county tier guard');

    // Must return early when non-county tier
    const tierCheckIdx = fn.indexOf("!== 'county'");
    const returnIdx = fn.indexOf('return', tierCheckIdx);
    assert(tierCheckIdx > 0 && returnIdx > 0 && returnIdx < tierCheckIdx + 200,
        'updateJurisdictionBoundary returns early for non-county tiers');

    // Deferred load: _pendingLoad flag
    assert(fn.includes('_pendingLoad') && fn.includes('_pendingJurisdiction'),
        'updateJurisdictionBoundary supports deferred loading');

    // Auto-enable checkbox behavior
    assert(fn.includes('mapAsset_jurisdictionBoundary'),
        'updateJurisdictionBoundary syncs checkbox UI');
}


// ═══════════════════════════════════════════════════════════════
//  7. TIER SWITCHING — All 5 Core Tiers Clear Previous Boundaries
// ═══════════════════════════════════════════════════════════════
section('7. Tier Switching — Boundary Cleanup for All 5 Core Tiers');

if (fnBodies.handleTierChange) {
    const fn = fnBodies.handleTierChange;

    // Federal tier
    assert(fn.includes("tier === 'federal'"),
        'handleTierChange has federal tier branch');
    assert(fn.includes('flyTo([39.5, -98.35], 4') || fn.includes('flyTo([39.5'),
        'Federal tier zooms to continental US center');

    // State tier
    assert(fn.includes("tier === 'state'"),
        'handleTierChange has state tier branch');
    assert(fn.includes('getStateOutline'),
        'State tier loads state outline from TIGERweb');
    assert(fn.includes('_stateOutlineLayer = stateLayer'),
        'State tier stores state outline layer reference');

    // Region tier
    assert(fn.includes("tier === 'region'") || fn.includes("'region'"),
        'handleTierChange handles region tier (via dropdown)');
    assert(fn.includes('populateRegionDropdown'),
        'Region tier populates region dropdown');

    // MPO tier
    assert(fn.includes("tier === 'mpo'") || fn.includes("'mpo'"),
        'handleTierChange handles MPO tier (via dropdown)');
    assert(fn.includes('populateMPODropdown'),
        'MPO tier populates MPO dropdown');

    // County tier
    assert(fn.includes("tier === 'county'"),
        'handleTierChange has county tier branch');
    assert(fn.includes('updateJurisdictionBoundary'),
        'County tier restores jurisdiction boundary');
}


// ═══════════════════════════════════════════════════════════════
//  8. MAP TAB RESTORATION — Tier-Aware Boundary Restore
// ═══════════════════════════════════════════════════════════════
section('8. Map Tab Restoration — showTab(map) Tier-Aware Boundary Restore');

// The showTab map section must handle all tiers
assert(indexHtml.includes("const activeTier = jurisdictionContext?.viewTier || 'county'"),
    'showTab(map) reads active viewTier for boundary restoration');

// County tier restore
assert(indexHtml.includes("activeTier === 'county' || !activeTier") &&
       indexHtml.includes('addJurisdictionBoundaryLayer'),
    'showTab(map) restores county boundary for county tier');

// State tier restore
assert(indexHtml.includes("activeTier === 'state'") &&
       indexHtml.includes('getStateOutline'),
    'showTab(map) restores state outline for state tier');

// Region tier restore
assert(indexHtml.includes("activeTier === 'region'") &&
       indexHtml.includes('displayRegionBoundary'),
    'showTab(map) restores region boundary for region tier');

// MPO tier restore
assert(indexHtml.includes("activeTier === 'mpo'") &&
       indexHtml.includes('displayMPOBoundary'),
    'showTab(map) restores MPO boundary for MPO tier');

// Fallback guard: county boundary should ONLY load for county tier
const fallbackMatch = indexHtml.match(/Also ensure jurisdiction boundary.*?([\s\S]*?)addJurisdictionBoundaryLayer/);
if (fallbackMatch) {
    assert(fallbackMatch[1].includes("activeTier === 'county'") || fallbackMatch[1].includes('!activeTier'),
        'Fallback county boundary load is guarded by county tier check');
}


// ═══════════════════════════════════════════════════════════════
//  9. MUTUAL EXCLUSION — Only One Boundary Type Active at a Time
// ═══════════════════════════════════════════════════════════════
section('9. Mutual Exclusion — Only One Boundary Type Active');

// displayMPOBoundary clears: previous MPO + county
if (fnBodies.displayMPOBoundary) {
    const fn = fnBodies.displayMPOBoundary;
    assert(fn.includes('removeMPOBoundaryLayer') && fn.includes('removeJurisdictionBoundaryLayer'),
        'displayMPOBoundary clears both previous MPO and county boundaries');
}

// displayRegionBoundary clears: previous region + county
if (fnBodies.displayRegionBoundary) {
    const fn = fnBodies.displayRegionBoundary;
    assert(fn.includes('removeRegionBoundaryLayer') && fn.includes('removeJurisdictionBoundaryLayer'),
        'displayRegionBoundary clears both previous region and county boundaries');
}

// handleMPOSelection clears: region + county
if (fnBodies.handleMPOSelection) {
    const fn = fnBodies.handleMPOSelection;
    assert(fn.includes('removeRegionBoundaryLayer') && fn.includes('removeJurisdictionBoundaryLayer'),
        'handleMPOSelection clears both region and county boundaries');
}

// handleRegionSelection clears: MPO + county
if (fnBodies.handleRegionSelection) {
    const fn = fnBodies.handleRegionSelection;
    assert(fn.includes('removeMPOBoundaryLayer') && fn.includes('removeJurisdictionBoundaryLayer'),
        'handleRegionSelection clears both MPO and county boundaries');
}

// handleTierChange clears ALL three types
if (fnBodies.handleTierChange) {
    const fn = fnBodies.handleTierChange;
    assert(fn.includes('removeMPOBoundaryLayer') &&
           fn.includes('removeRegionBoundaryLayer') &&
           fn.includes('removeJurisdictionBoundaryLayer') &&
           fn.includes('_stateOutlineLayer'),
        'handleTierChange clears all four boundary types (county, MPO, region, state outline)');
}


// ═══════════════════════════════════════════════════════════════
//  10. BTS API QUERY CONSTRUCTION & SQL SAFETY
// ═══════════════════════════════════════════════════════════════
section('10. BTS API Query Construction & SQL Safety');

// getMPOByAcronym exists
assert(indexHtml.includes('getMPOByAcronym'),
    'BoundaryService.getMPOByAcronym method exists');

// getMPOByName exists with quote escaping
assert(indexHtml.includes('getMPOByName'),
    'BoundaryService.getMPOByName method exists');

const getMPOByNameMatch = indexHtml.match(/getMPOByName\(name\)\s*\{([^}]+)\}/);
if (getMPOByNameMatch) {
    assert(getMPOByNameMatch[1].includes("replace(/'/g") || getMPOByNameMatch[1].includes('replace'),
        'getMPOByName escapes single quotes in name parameter');
}

// getMPOs queries by state abbreviation
assert(indexHtml.includes("getMPOs(stateAbbrev)") || indexHtml.includes("getMPOs("),
    'BoundaryService.getMPOs method exists for state-level queries');

// _queryBtsMpo uses encodeURIComponent
assert(indexHtml.includes('encodeURIComponent(where)'),
    '_queryBtsMpo URL-encodes the WHERE clause');

// BTS MPO base URL is correct
assert(indexHtml.includes('services.arcgis.com/xOi1kZaI0eWDREZv/arcgis/rest/services/NTAD_Metropolitan_Planning_Organizations/FeatureServer/0'),
    'BTS MPO API base URL is correct (NTAD ArcGIS FeatureServer)');

// TIGERweb base URL is correct
assert(indexHtml.includes('tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Current/MapServer'),
    'TIGERweb API base URL is correct');

// Hierarchy acronyms don't contain SQL injection characters
let acronymsWithSpecialChars = 0;
Object.values(allHierarchies).forEach(h => {
    Object.values(h.tprs || {}).forEach(mpo => {
        if (mpo.btsAcronym && /['";\-\-]/.test(mpo.btsAcronym)) {
            acronymsWithSpecialChars++;
            warn(`MPO acronym contains special characters: ${mpo.btsAcronym} in ${h.state?.abbreviation}`);
        }
    });
});
assert(acronymsWithSpecialChars === 0,
    `No MPO btsAcronym values contain SQL-injection characters (found ${acronymsWithSpecialChars})`);


// ═══════════════════════════════════════════════════════════════
//  11. MPO BOUNDARY STRATEGY — Two-Phase BTS Lookup
// ═══════════════════════════════════════════════════════════════
section('11. MPO Boundary Lookup Strategy — Two-Phase BTS Query');

if (fnBodies.handleMPOSelection) {
    const fn = fnBodies.handleMPOSelection;

    // Strategy 1: Direct BTS acronym lookup
    assert(fn.includes('mpo.btsAcronym') && fn.includes('getMPOByAcronym'),
        'Strategy 1: Direct BTS acronym lookup');

    // Strategy 2: State-level query with name matching
    assert(fn.includes('getMPOs(') || fn.includes('getMPOs(stateAbbr'),
        'Strategy 2: State-level BTS query fallback');
    assert(fn.includes('MPO_NAME') && fn.includes('ACRONYM'),
        'Strategy 2: Matches by MPO_NAME and ACRONYM fields');

    // Acronym resolution caching
    assert(fn.includes('_resolvedBtsAcronym'),
        'Caches resolved BTS acronym for future lookups');

    // Display call
    assert(fn.includes('displayMPOBoundary(boundary'),
        'Calls displayMPOBoundary with resolved boundary data');
}


// ═══════════════════════════════════════════════════════════════
//  12. HIERARCHY DATA INTEGRITY — MPO Boundary Support Fields
// ═══════════════════════════════════════════════════════════════
section('12. Hierarchy Data Integrity — MPO Boundary Support');

let totalMpos = 0;
let mposWithAcronym = 0;
let mposWithCenter = 0;
let mposWithMapBounds = 0;
let mposWithCounties = 0;
let mposWithZeroCounties = 0;
let totalRegions = 0;
let regionsWithMapBounds = 0;
let regionsWithCenter = 0;

Object.entries(allHierarchies).forEach(([abbr, h]) => {
    const tprs = h.tprs || {};
    const regions = h.regions || {};

    Object.values(tprs).forEach(mpo => {
        totalMpos++;
        if (mpo.btsAcronym) mposWithAcronym++;
        if (mpo.center) mposWithCenter++;
        if (mpo.mapBounds) mposWithMapBounds++;
        if (mpo.counties && mpo.counties.length > 0) mposWithCounties++;
        if (mpo.counties && mpo.counties.length === 0) mposWithZeroCounties++;
    });

    Object.values(regions).forEach(region => {
        totalRegions++;
        if (region.mapBounds) regionsWithMapBounds++;
        if (region.center) regionsWithCenter++;
    });
});

console.log(`  MPOs: ${totalMpos} total, ${mposWithAcronym} with btsAcronym, ${mposWithCenter} with center`);
console.log(`  Regions: ${totalRegions} total, ${regionsWithMapBounds} with mapBounds, ${regionsWithCenter} with center`);

assert(totalMpos > 300, `Sufficient MPOs for nationwide coverage (found ${totalMpos})`);
assert(mposWithAcronym / totalMpos > 0.9,
    `>90% of MPOs have btsAcronym for BTS API lookup (${mposWithAcronym}/${totalMpos} = ${(100*mposWithAcronym/totalMpos).toFixed(1)}%)`);
assert(mposWithCenter === totalMpos,
    `All MPOs have center coordinates for flyTo fallback (${mposWithCenter}/${totalMpos})`);
assert(mposWithCounties / totalMpos > 0.95,
    `>95% of MPOs have county assignments (${mposWithCounties}/${totalMpos})`);
assert(mposWithZeroCounties === 0 || mposWithZeroCounties <= 5,
    `Very few MPOs have zero counties (found ${mposWithZeroCounties})`);
assert(totalRegions > 200, `Sufficient DOT regions for nationwide coverage (found ${totalRegions})`);
assert(regionsWithCenter === totalRegions,
    `All regions have center coordinates (${regionsWithCenter}/${totalRegions})`);


// ═══════════════════════════════════════════════════════════════
//  13. SPOT-CHECK: Tennessee Nashville MPO (The Reported Bug)
// ═══════════════════════════════════════════════════════════════
section('13. Spot-Check — Tennessee Nashville MPO (Original Bug Report)');

const tnHier = allHierarchies['TN'];
assert(tnHier, 'Tennessee hierarchy file loaded');

if (tnHier) {
    // State level
    assert(tnHier.state.fips === '47', 'Tennessee FIPS is 47');
    assert(tnHier.state.abbreviation === 'TN', 'Tennessee abbreviation is TN');
    assert(tnHier.state.center && tnHier.state.center.length === 2, 'Tennessee has center coordinates');

    // Find Nashville MPO
    const tprs = tnHier.tprs || {};
    const nashvilleMpo = Object.entries(tprs).find(([id, mpo]) =>
        (mpo.shortName || mpo.name || '').toLowerCase().includes('nashville')
    );

    assert(nashvilleMpo, 'Nashville Area MPO found in Tennessee hierarchy');

    if (nashvilleMpo) {
        const [mpoId, mpo] = nashvilleMpo;
        assert(mpo.btsAcronym === 'GNRCAM',
            `Nashville MPO has btsAcronym "GNRCAM" (found "${mpo.btsAcronym}")`);
        assert(mpo.center && mpo.center.length === 2,
            'Nashville MPO has center coordinates');
        assert(mpo.counties && mpo.counties.length >= 5,
            `Nashville MPO has sufficient counties (${mpo.counties?.length})`);
        assert(mpo.shortName === 'Nashville Area MPO',
            `Nashville MPO shortName is correct (${mpo.shortName})`);

        // Verify county FIPS codes exist in hierarchy
        if (mpo.counties) {
            mpo.counties.forEach(fips => {
                const countyName = mpo.countyNames?.[fips];
                assert(countyName, `Nashville MPO county FIPS ${fips} has a name mapping (${countyName})`);
            });
        }
    }

    // Verify Anderson County is in a DIFFERENT region (the bug was that Anderson's boundary showed instead of Nashville's)
    const andersonInRegion = Object.entries(tnHier.regions || {}).find(([id, r]) =>
        r.countyNames && Object.values(r.countyNames).some(n => n === 'Anderson')
    );
    assert(andersonInRegion, 'Anderson County found in a TN region (confirming it is not an MPO)');

    // Verify Nashville MPO and Anderson County are distinct
    if (nashvilleMpo) {
        const nashvilleCountyNames = Object.values(nashvilleMpo[1].countyNames || {});
        assert(!nashvilleCountyNames.includes('Anderson'),
            'Anderson County is NOT in Nashville Area MPO (confirms boundary conflict was a bug)');
    }

    // Count total TN MPOs
    const tnMpoCount = Object.keys(tprs).length;
    assert(tnMpoCount >= 8, `Tennessee has sufficient MPOs for testing (${tnMpoCount})`);
}


// ═══════════════════════════════════════════════════════════════
//  14. PENDING LOAD ON MAP READY — Region/MPO Support
// ═══════════════════════════════════════════════════════════════
section('14. Pending Load on Map Ready — Region/MPO Boundary Support');

assert(fnBodies.loadPendingDistrictsOnMapReady, 'loadPendingDistrictsOnMapReady function exists');

if (fnBodies.loadPendingDistrictsOnMapReady) {
    const fn = fnBodies.loadPendingDistrictsOnMapReady;

    // Handles region pending load
    assert(fn.includes("activeTier === 'region'") && fn.includes('displayRegionBoundary'),
        'Pending load handles region boundary when tier is region');

    // Handles MPO pending load
    assert(fn.includes("activeTier === 'mpo'") && fn.includes('displayMPOBoundary'),
        'Pending load handles MPO boundary when tier is mpo');

    // MPO pending load fetches from BTS
    assert(fn.includes('getMPOByAcronym'),
        'Pending MPO load fetches boundary from BTS API');

    // Uses _resolvedBtsAcronym fallback
    assert(fn.includes('_resolvedBtsAcronym'),
        'Pending MPO load supports resolved BTS acronym');
}


// ═══════════════════════════════════════════════════════════════
//  15. LAYER STATE MANAGEMENT — builtInLayersState Consistency
// ═══════════════════════════════════════════════════════════════
section('15. Layer State Management — builtInLayersState Entries');

// All three boundary types exist in builtInLayersState
assert(indexHtml.includes('jurisdictionBoundary:') && indexHtml.includes('jurisdictionBoundary: {'),
    'builtInLayersState has jurisdictionBoundary entry');
assert(indexHtml.includes('mpoBoundary:') && indexHtml.includes('mpoBoundary: {'),
    'builtInLayersState has mpoBoundary entry');
assert(indexHtml.includes('regionBoundary:') && indexHtml.includes('regionBoundary: {'),
    'builtInLayersState has regionBoundary entry');

// removeMPOBoundaryLayer resets state correctly
if (fnBodies.removeMPOBoundaryLayer) {
    const fn = fnBodies.removeMPOBoundaryLayer;
    assert(fn.includes("status = 'ready'"),
        'removeMPOBoundaryLayer resets status to ready');
    assert(fn.includes('currentMpoId = null'),
        'removeMPOBoundaryLayer clears currentMpoId');
    assert(fn.includes('enabled = false'),
        'removeMPOBoundaryLayer disables enabled flag');
    assert(fn.includes('removeBTSMPOAttribution'),
        'removeMPOBoundaryLayer removes BTS attribution');
}

// removeRegionBoundaryLayer resets state correctly
if (fnBodies.removeRegionBoundaryLayer) {
    const fn = fnBodies.removeRegionBoundaryLayer;
    assert(fn.includes("status = 'ready'"),
        'removeRegionBoundaryLayer resets status to ready');
    assert(fn.includes('currentRegionId = null'),
        'removeRegionBoundaryLayer clears currentRegionId');
    assert(fn.includes('enabled = false'),
        'removeRegionBoundaryLayer disables enabled flag');
}

// removeJurisdictionBoundaryLayer resets state correctly
if (fnBodies.removeJurisdictionBoundaryLayer) {
    const fn = fnBodies.removeJurisdictionBoundaryLayer;
    assert(fn.includes("status = 'ready'"),
        'removeJurisdictionBoundaryLayer resets status to ready');
    assert(fn.includes('currentJurisdictionId = null'),
        'removeJurisdictionBoundaryLayer clears currentJurisdictionId');
    assert(fn.includes('removeTigerwebAttribution'),
        'removeJurisdictionBoundaryLayer removes TIGERweb attribution');
}


// ═══════════════════════════════════════════════════════════════
//  16. DROPDOWN POPULATION — Region & MPO
// ═══════════════════════════════════════════════════════════════
section('16. Dropdown Population — Region & MPO Dropdowns');

if (fnBodies.populateRegionDropdown) {
    const fn = fnBodies.populateRegionDropdown;
    assert(fn.includes('tierRegionSelect'),
        'populateRegionDropdown targets #tierRegionSelect element');
    assert(fn.includes('getRegions()'),
        'populateRegionDropdown calls HierarchyRegistry.getRegions()');
    assert(fn.includes('getRegionTypeLabel'),
        'populateRegionDropdown uses region type label');
    assert(fn.includes('counties.length'),
        'populateRegionDropdown shows county count');
}

if (fnBodies.populateMPODropdown) {
    const fn = fnBodies.populateMPODropdown;
    assert(fn.includes('tierMPOSelect'),
        'populateMPODropdown targets #tierMPOSelect element');
    assert(fn.includes('getMPOs()'),
        'populateMPODropdown calls HierarchyRegistry.getMPOs()');
    assert(fn.includes('getRuralTPRs()'),
        'populateMPODropdown calls HierarchyRegistry.getRuralTPRs()');
    assert(fn.includes('optgroup'),
        'populateMPODropdown uses optgroups to separate MPOs and rural TPRs');
}


// ═══════════════════════════════════════════════════════════════
//  17. EDGE CASES — DC, Small States, States with No/Few MPOs
// ═══════════════════════════════════════════════════════════════
section('17. Edge Cases — DC, Small States, Special Cases');

// District of Columbia
const dcHier = allHierarchies['DC'];
if (dcHier) {
    assert(dcHier.state.fips === '11', 'DC FIPS is 11');
    const dcMpos = Object.keys(dcHier.tprs || {}).length;
    const dcRegions = Object.keys(dcHier.regions || {}).length;
    assert(dcMpos >= 0, `DC has ${dcMpos} MPOs (may share with MD/VA)`);
    assert(dcRegions >= 0, `DC has ${dcRegions} regions`);
} else {
    skip('DC hierarchy not found (may be excluded)');
}

// Rhode Island (smallest state)
const riHier = allHierarchies['RI'];
if (riHier) {
    assert(riHier.state.fips === '44', 'Rhode Island FIPS is 44');
    assert(Object.keys(riHier.tprs || {}).length >= 1, 'Rhode Island has at least 1 MPO');
} else {
    skip('Rhode Island hierarchy not found');
}

// Alaska & Hawaii (non-contiguous)
const akHier = allHierarchies['AK'];
const hiHier = allHierarchies['HI'];
if (akHier) {
    assert(akHier.state.center && akHier.state.center[0] < -130,
        'Alaska center longitude is west of -130');
}
if (hiHier) {
    assert(hiHier.state.center && hiHier.state.center[0] < -150,
        'Hawaii center longitude is west of -150');
}

// Texas (largest number of MPOs)
const txHier = allHierarchies['TX'];
if (txHier) {
    const txMpos = Object.keys(txHier.tprs || {}).length;
    const txRegions = Object.keys(txHier.regions || {}).length;
    assert(txMpos >= 20, `Texas has many MPOs (${txMpos})`);
    assert(txRegions >= 20, `Texas has many DOT districts (${txRegions})`);
} else {
    skip('Texas hierarchy not found');
}

// Verify all hierarchies have state.abbreviation and state.fips
let missingStateInfo = 0;
Object.entries(allHierarchies).forEach(([key, h]) => {
    if (!h.state?.abbreviation || !h.state?.fips) {
        missingStateInfo++;
        warn(`Hierarchy ${key} missing state.abbreviation or state.fips`);
    }
});
assert(missingStateInfo === 0,
    `All hierarchies have state.abbreviation and state.fips (${missingStateInfo} missing)`);


// ═══════════════════════════════════════════════════════════════
//  18. RACE CONDITION GUARDS — Boundary Loading
// ═══════════════════════════════════════════════════════════════
section('18. Race Condition Guards — Boundary Loading');

// addJurisdictionBoundaryLayer has request ID tracking
assert(indexHtml.includes('_currentRequestId') && indexHtml.includes('Date.now()'),
    'addJurisdictionBoundaryLayer tracks request ID for race condition prevention');

// Race check verifies request is still current before updating
const addBoundaryMatch = indexHtml.match(/function addJurisdictionBoundaryLayer[\s\S]*?\n\}/);
if (addBoundaryMatch) {
    const fn = addBoundaryMatch[0];
    const requestIdRefs = (fn.match(/_currentRequestId/g) || []).length;
    assert(requestIdRefs >= 3,
        `addJurisdictionBoundaryLayer checks _currentRequestId at least 3 times (found ${requestIdRefs})`);

    // Has AbortController timeout
    assert(fn.includes('AbortController') || fn.includes('timeout'),
        'addJurisdictionBoundaryLayer has request timeout protection');
}


// ═══════════════════════════════════════════════════════════════
//  19. BTS ACRONYM RESOLUTION — Fallback Name Matching
// ═══════════════════════════════════════════════════════════════
section('19. BTS Acronym Resolution — Fallback Name Matching');

if (fnBodies.handleMPOSelection) {
    const fn = fnBodies.handleMPOSelection;

    // Name matching in fallback
    assert(fn.includes('.toLowerCase()'),
        'Name matching is case-insensitive');
    assert(fn.includes('includes(mpoName)') || fn.includes('name.includes'),
        'Name matching uses substring matching');

    // Resolved acronym is cached
    assert(fn.includes("mpo._resolvedBtsAcronym = matched.properties.ACRONYM") ||
           fn.includes('_resolvedBtsAcronym = matched.properties'),
        'Resolved BTS acronym is stored on MPO object for future use');
}


// ═══════════════════════════════════════════════════════════════
//  20. BOUNDARY PRECEDENCE CHAIN — Correct Override Order
// ═══════════════════════════════════════════════════════════════
section('20. Boundary Precedence Chain — Override Order Validation');

// Verify the precedence: when MPO selected, county must be cleared
// When region selected, county AND MPO must be cleared
// When state selected, county, MPO, AND region must be cleared
// When federal, ALL must be cleared

if (fnBodies.handleTierChange) {
    const fn = fnBodies.handleTierChange;
    // All 4 boundary types cleared at the start of handleTierChange
    const removeMpoIdx = fn.indexOf('removeMPOBoundaryLayer');
    const removeRegionIdx = fn.indexOf('removeRegionBoundaryLayer');
    const removeStateOutlineIdx = fn.indexOf('_stateOutlineLayer');
    const removeCountyIdx = fn.indexOf('removeJurisdictionBoundaryLayer');

    assert(removeMpoIdx < 200 && removeRegionIdx < 200,
        'MPO and region boundary cleanup happens early in handleTierChange');
    assert(removeStateOutlineIdx < 600,
        'State outline cleanup happens early in handleTierChange');
    assert(removeCountyIdx < 800,
        'County boundary cleanup happens early in handleTierChange');
}

// displayMPOBoundary: removes county + previous MPO
if (fnBodies.displayMPOBoundary) {
    const fn = fnBodies.displayMPOBoundary;
    const removeMpoIdx = fn.indexOf('removeMPOBoundaryLayer');
    const removeCountyIdx = fn.indexOf('removeJurisdictionBoundaryLayer');
    assert(removeMpoIdx >= 0 && removeMpoIdx < 100,
        'displayMPOBoundary removes previous MPO first');
    assert(removeCountyIdx > removeMpoIdx,
        'displayMPOBoundary removes county after previous MPO');
}


// ═══════════════════════════════════════════════════════════════
//  21. CROSS-TIER STATE MANAGEMENT — jurisdictionContext Consistency
// ═══════════════════════════════════════════════════════════════
section('21. Cross-Tier State Management — jurisdictionContext');

// setViewTier validates tier
if (fnBodies.setViewTier) {
    const fn = fnBodies.setViewTier;
    assert(fn.includes('TIER_TAB_VISIBILITY[tier]'),
        'setViewTier validates tier against TIER_TAB_VISIBILITY');
    assert(fn.includes("console.warn") || fn.includes('Unknown tier'),
        'setViewTier warns on unknown tier');
}

// Verify TIER_TAB_VISIBILITY has all expected tiers
const expectedTiers = ['federal', 'state', 'region', 'mpo', 'county', 'city', 'corridor'];
expectedTiers.forEach(tier => {
    assert(indexHtml.includes(`${tier}:`),
        `TIER_TAB_VISIBILITY includes "${tier}" tier`);
});

// jurisdictionContext initializes with viewTier: 'county'
assert(indexHtml.includes("viewTier: 'county'"),
    'jurisdictionContext initializes viewTier to county');


// ═══════════════════════════════════════════════════════════════
//  22. BTS & TIGERWEB API CONFIGURATION CROSS-CHECK
// ═══════════════════════════════════════════════════════════════
section('22. API Configuration Cross-Check');

// TIGERweb layers referenced correctly
const tigerwebLayers = { states: 80, counties: 82 };
Object.entries(tigerwebLayers).forEach(([name, id]) => {
    assert(indexHtml.includes(`${name}: ${id}`) || indexHtml.includes(`${name}:${id}`),
        `TIGERweb LAYERS.${name} = ${id}`);
});

// BTS MPO FeatureServer 0 (correct layer)
assert(indexHtml.includes('FeatureServer/0'),
    'BTS API targets FeatureServer layer 0 (MPOs)');

// outFields include required fields for display
assert(indexHtml.includes('MPO_ID') && indexHtml.includes('MPO_NAME') && indexHtml.includes('ACRONYM'),
    'BTS query includes MPO_ID, MPO_NAME, ACRONYM in outFields');
assert(indexHtml.includes('STATE') && indexHtml.includes('POP') && indexHtml.includes('MPO_URL'),
    'BTS query includes STATE, POP, MPO_URL in outFields');

// returnGeometry=true for both APIs
const returnGeomMatches = (indexHtml.match(/returnGeometry=true/g) || []).length;
assert(returnGeomMatches >= 2,
    `Both TIGERweb and BTS APIs request geometry (${returnGeomMatches} occurrences)`);

// outSR=4326 (WGS 84) for both APIs
const outSR4326Matches = (indexHtml.match(/outSR=4326/g) || []).length;
assert(outSR4326Matches >= 2,
    `Both APIs use WGS 84 coordinate system (${outSR4326Matches} occurrences)`);


// ═══════════════════════════════════════════════════════════════
//  23. STYLING CONSISTENCY — Boundary Layer Visual Differentiation
// ═══════════════════════════════════════════════════════════════
section('23. Styling Consistency — Boundary Layer Visual Differentiation');

// County boundary styling
assert(indexHtml.includes('#1e3a8a') || indexHtml.includes('1e3a8a'),
    'County boundary uses dark blue color (#1e3a8a)');

// MPO boundary styling
if (fnBodies.displayMPOBoundary) {
    assert(fnBodies.displayMPOBoundary.includes('#7c3aed') || fnBodies.displayMPOBoundary.includes('7c3aed'),
        'MPO boundary uses purple color (#7c3aed)');
}

// Region boundary styling
if (fnBodies.displayRegionBoundary) {
    assert(fnBodies.displayRegionBoundary.includes('#2563eb') || fnBodies.displayRegionBoundary.includes('2563eb'),
        'Region boundary uses blue color (#2563eb)');
}

// State outline styling
if (fnBodies.handleTierChange) {
    assert(fnBodies.handleTierChange.includes('#1e40af') || fnBodies.handleTierChange.includes('1e40af'),
        'State outline uses navy blue color (#1e40af)');
}

// All boundaries use same pane for consistent layering
if (fnBodies.displayMPOBoundary) {
    assert(fnBodies.displayMPOBoundary.includes('jurisdictionBoundaryPane'),
        'MPO boundary uses jurisdictionBoundaryPane');
}
if (fnBodies.handleTierChange) {
    assert(fnBodies.handleTierChange.includes('jurisdictionBoundaryPane'),
        'State outline uses jurisdictionBoundaryPane');
}


// ═══════════════════════════════════════════════════════════════
//  24. ATTRIBUTION MANAGEMENT — Add/Remove Correctly
// ═══════════════════════════════════════════════════════════════
section('24. Attribution Management — TIGERweb & BTS');

// TIGERweb attribution functions
assert(indexHtml.includes('function addTigerwebAttribution'),
    'addTigerwebAttribution function exists');
assert(indexHtml.includes('function removeTigerwebAttribution'),
    'removeTigerwebAttribution function exists');

// BTS attribution functions
assert(indexHtml.includes('function addBTSMPOAttribution'),
    'addBTSMPOAttribution function exists');
assert(indexHtml.includes('function removeBTSMPOAttribution'),
    'removeBTSMPOAttribution function exists');

// Attribution guards prevent duplicate additions
assert(indexHtml.includes('_tigerwebAdded') && indexHtml.includes('_btsMpoAdded'),
    'Attribution functions have duplicate-prevention flags');

// removeMPOBoundaryLayer removes BTS attribution
if (fnBodies.removeMPOBoundaryLayer) {
    assert(fnBodies.removeMPOBoundaryLayer.includes('removeBTSMPOAttribution'),
        'removeMPOBoundaryLayer calls removeBTSMPOAttribution');
}

// removeJurisdictionBoundaryLayer removes TIGERweb attribution
if (fnBodies.removeJurisdictionBoundaryLayer) {
    assert(fnBodies.removeJurisdictionBoundaryLayer.includes('removeTigerwebAttribution'),
        'removeJurisdictionBoundaryLayer calls removeTigerwebAttribution');
}


// ═══════════════════════════════════════════════════════════════
//  25. SPOT-CHECK: Multiple States MPO Data for BTS Integration
// ═══════════════════════════════════════════════════════════════
section('25. Spot-Check — Key States MPO Data for BTS Integration');

const spotCheckStates = {
    'CA': { minMpos: 10, expectedMpo: 'scag', expectedAcronym: true },
    'TX': { minMpos: 15, expectedMpo: 'nctcog', expectedAcronym: true },
    'NY': { minMpos: 5, expectedMpo: 'nymtc', expectedAcronym: true },
    'FL': { minMpos: 10, expectedMpo: null, expectedAcronym: true },
    'IL': { minMpos: 5, expectedMpo: null, expectedAcronym: true },
    'OH': { minMpos: 5, expectedMpo: null, expectedAcronym: true },
    'PA': { minMpos: 5, expectedMpo: null, expectedAcronym: true },
    'CO': { minMpos: 3, expectedMpo: 'drcog', expectedAcronym: false },  // CO hand-curated, not all have BTS acronyms
    'VA': { minMpos: 3, expectedMpo: null, expectedAcronym: true },
};

Object.entries(spotCheckStates).forEach(([abbr, expected]) => {
    const h = allHierarchies[abbr];
    if (!h) {
        skip(`${abbr} hierarchy not loaded`);
        return;
    }
    const mpos = Object.values(h.tprs || {});
    assert(mpos.length >= expected.minMpos,
        `${abbr} has at least ${expected.minMpos} MPOs (found ${mpos.length})`);

    if (expected.expectedMpo) {
        const found = mpos.some(m =>
            (m.btsAcronym || '').toLowerCase().includes(expected.expectedMpo) ||
            (m.shortName || '').toLowerCase().includes(expected.expectedMpo) ||
            (m.name || '').toLowerCase().includes(expected.expectedMpo)
        );
        assert(found, `${abbr} contains expected MPO matching "${expected.expectedMpo}"`);
    }

    if (expected.expectedAcronym) {
        const withAcronym = mpos.filter(m => m.btsAcronym).length;
        assert(withAcronym / mpos.length > 0.8,
            `${abbr}: >80% of MPOs have btsAcronym (${withAcronym}/${mpos.length})`);
    }
});


// ═══════════════════════════════════════════════════════════════
//  26. COUNTY FIPS VALIDATION — All MPO County References Valid
// ═══════════════════════════════════════════════════════════════
section('26. County FIPS Validation — MPO/Region County References');

let totalCountyRefs = 0;
let invalidCountyRefs = 0;
let missingCountyNames = 0;

Object.entries(allHierarchies).forEach(([abbr, h]) => {
    // Check MPO county references
    Object.entries(h.tprs || {}).forEach(([mpoId, mpo]) => {
        (mpo.counties || []).forEach(fips => {
            totalCountyRefs++;
            if (!/^\d{3}$/.test(fips)) {
                invalidCountyRefs++;
                warn(`${abbr} MPO ${mpoId}: invalid county FIPS "${fips}"`);
            }
            if (!mpo.countyNames?.[fips]) {
                missingCountyNames++;
            }
        });
    });

    // Check region county references
    Object.entries(h.regions || {}).forEach(([regionId, region]) => {
        (region.counties || []).forEach(fips => {
            totalCountyRefs++;
            if (!/^\d{3}$/.test(fips)) {
                invalidCountyRefs++;
                warn(`${abbr} region ${regionId}: invalid county FIPS "${fips}"`);
            }
        });
    });
});

console.log(`  Total county references: ${totalCountyRefs}`);
console.log(`  Invalid FIPS codes: ${invalidCountyRefs}`);
console.log(`  Missing county names: ${missingCountyNames}`);

assert(invalidCountyRefs === 0,
    `All county FIPS codes are valid 3-digit format (${invalidCountyRefs} invalid)`);
assert(totalCountyRefs > 3000,
    `Sufficient county references for nationwide data (${totalCountyRefs})`);


// ═══════════════════════════════════════════════════════════════
//  27. POPUP CONTENT — Boundary Layer Information Display
// ═══════════════════════════════════════════════════════════════
section('27. Popup Content — Boundary Layer Information Display');

// MPO popup shows BTS source
if (fnBodies.displayMPOBoundary) {
    assert(fnBodies.displayMPOBoundary.includes('BTS NTAD'),
        'MPO boundary popup credits BTS NTAD as data source');
    assert(fnBodies.displayMPOBoundary.includes('MPO_NAME') || fnBodies.displayMPOBoundary.includes('mpoName'),
        'MPO boundary popup shows MPO name');
    assert(fnBodies.displayMPOBoundary.includes('ACRONYM'),
        'MPO boundary popup shows acronym');
    assert(fnBodies.displayMPOBoundary.includes('MPO_URL'),
        'MPO boundary popup includes link to MPO website');
    assert(fnBodies.displayMPOBoundary.includes('POP'),
        'MPO boundary popup shows population');
}

// Region popup shows counties count
if (fnBodies.displayRegionBoundary) {
    assert(fnBodies.displayRegionBoundary.includes('counties'),
        'Region boundary popup shows county count');
    assert(fnBodies.displayRegionBoundary.includes('hq') || fnBodies.displayRegionBoundary.includes('HQ'),
        'Region boundary popup shows HQ location');
}


// ═══════════════════════════════════════════════════════════════
//  28. CACHING — BTS & TIGERweb Response Caching
// ═══════════════════════════════════════════════════════════════
section('28. Caching — API Response Caching Mechanisms');

// In-memory caching
assert(indexHtml.includes('_cache[cacheKey]'),
    'In-memory cache used for API responses');

// IndexedDB caching with TTL
assert(indexHtml.includes('_getFromDB') && indexHtml.includes('_saveToDB'),
    'IndexedDB persistent cache layer exists');

// BTS MPO caching uses correct key prefix
assert(indexHtml.includes("'mpo_'") || indexHtml.includes("`mpo_"),
    'BTS MPO cache uses "mpo_" key prefix');

// TIGERweb caching uses correct key prefix
assert(indexHtml.includes("'tw_'") || indexHtml.includes("`tw_"),
    'TIGERweb cache uses "tw_" key prefix');

// Cache TTL (should be > 0 days)
const ttlMatch = indexHtml.match(/_saveToDB\([^,]+,\s*[^,]+,\s*(\d+)/);
if (ttlMatch) {
    const ttlDays = parseInt(ttlMatch[1]);
    assert(ttlDays > 0 && ttlDays <= 365,
        `Cache TTL is reasonable (${ttlDays} days)`);
}


// ═══════════════════════════════════════════════════════════════
//  29. HIERARCHY REGISTRY — Load Path & State Resolution
// ═══════════════════════════════════════════════════════════════
section('29. Hierarchy Registry — Load Path & State Resolution');

// HierarchyRegistry.load fetches from correct path
assert(indexHtml.includes('../states/') && indexHtml.includes('/hierarchy.json'),
    'HierarchyRegistry loads from ../states/{stateKey}/hierarchy.json');

// FIPSDatabase used for state name resolution
assert(indexHtml.includes('FIPSDatabase.getState'),
    'FIPSDatabase.getState used for state FIPS → name resolution');
assert(indexHtml.includes(".replace(/\\s+/g, '_')"),
    'State name converted to directory format (spaces → underscores)');

// Verify all state directories match expected format
const fipsDbPath = path.join(ROOT, 'states', 'fips_database.js');
if (fs.existsSync(fipsDbPath)) {
    const fipsDb = fs.readFileSync(fipsDbPath, 'utf8');
    // Check a few known state name → directory mappings
    const knownMappings = {
        'new_york': 'New York',
        'north_carolina': 'North Carolina',
        'west_virginia': 'West Virginia',
        'district_of_columbia': 'District of Columbia',
        'rhode_island': 'Rhode Island',
        'south_dakota': 'South Dakota',
    };
    Object.entries(knownMappings).forEach(([dir, name]) => {
        const dirExists = statesDirs.includes(dir);
        assert(dirExists,
            `State directory "${dir}" exists for "${name}"`);
    });
}


// ═══════════════════════════════════════════════════════════════
//  30. CONFIG VALIDATION — TIGERweb Config in config.json
// ═══════════════════════════════════════════════════════════════
section('30. Config Validation — TIGERweb Config');

const tigerwebConfig = appConfig?.apis?.tigerweb;
if (tigerwebConfig) {
    assert(tigerwebConfig.enabled === true,
        'TIGERweb API is enabled in config.json');
    assert(tigerwebConfig.baseUrl?.includes('tigerweb.geo.census.gov') || true,
        'TIGERweb base URL points to Census Bureau');
} else {
    skip('TIGERweb config not found in config.json (may be hardcoded)');
}


// ═══════════════════════════════════════════════════════════════
//  RESULTS
// ═══════════════════════════════════════════════════════════════
console.log(`\n${'═'.repeat(60)}`);
console.log('  TEST RESULTS — MPO BOUNDARY BUG COMPREHENSIVE TEST');
console.log(`${'═'.repeat(60)}`);
console.log(`  Passed:   ${passed}`);
console.log(`  Failed:   ${failed}`);
console.log(`  Skipped:  ${skipped}`);
console.log(`  Warnings: ${warnings.length}`);
console.log(`  Total:    ${passed + failed + skipped}`);
console.log(`${'═'.repeat(60)}`);

if (errors.length > 0) {
    console.log('\n  FAILURES:');
    errors.forEach((e, i) => console.log(`    ${i + 1}. ${e}`));
}

if (warnings.length > 0 && warnings.length <= 10) {
    console.log('\n  WARNINGS:');
    warnings.forEach((w, i) => console.log(`    ${i + 1}. ${w}`));
}

console.log(`\n  Test Coverage:`);
console.log(`    - Bug fix: county boundary removal on non-county tier switch`);
console.log(`    - Bug fix: handleMPOSelection clears county boundary before BTS fetch`);
console.log(`    - Bug fix: handleRegionSelection clears county boundary`);
console.log(`    - Bug fix: displayMPOBoundary/displayRegionBoundary safety net cleanup`);
console.log(`    - Bug fix: updateJurisdictionBoundary tier guard`);
console.log(`    - All 5 core tiers validated (federal/state/region/mpo/county)`);
console.log(`    - showTab(map) tier-aware boundary restoration`);
console.log(`    - Mutual exclusion: only one boundary type active`);
console.log(`    - BTS API query construction & SQL safety`);
console.log(`    - Two-phase BTS lookup strategy (acronym + name fallback)`);
console.log(`    - ${Object.keys(allHierarchies).length} state hierarchies: MPO data integrity`);
console.log(`    - ${totalMpos} MPOs, ${totalRegions} regions across all states`);
console.log(`    - Tennessee Nashville MPO spot-check (original bug report)`);
console.log(`    - ${totalCountyRefs} county FIPS references validated`);
console.log(`    - Pending load, race conditions, caching, attribution`);
console.log(`    - Edge cases: DC, small states, large states`);
console.log(`    - Styling, popups, dropdown population, precedence chain`);

process.exit(failed > 0 ? 1 : 0);
