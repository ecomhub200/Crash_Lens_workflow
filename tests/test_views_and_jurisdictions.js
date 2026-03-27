/**
 * Comprehensive Test Suite — View Levels & Jurisdiction Files
 *
 * Validates:
 *  1. All jurisdiction files exist and have correct structure
 *  2. config.json has all required jurisdiction entries
 *  3. State hierarchy files are complete and consistent
 *  4. View tier switching works for all levels (Federal/State/Region/MPO/County)
 *  5. Dropdown population logic filters correctly by state
 *  6. Cross-state jurisdiction isolation
 *  7. Default state/jurisdiction configuration
 *
 * Run with Node.js:
 *   node tests/test_views_and_jurisdictions.js
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
    console.log(`\n=== ${name} ===`);
}

// ─────────────────────────────────────────────
// 1. CONFIG.JSON STRUCTURE
// ─────────────────────────────────────────────
section('1. config.json — Structure & Integrity');

const configPath = path.join(ROOT, 'config.json');
let config;
try {
    config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
    assert(true, 'config.json is valid JSON');
} catch (e) {
    assert(false, `config.json parse error: ${e.message}`);
    process.exit(1);
}

assert(config.defaultState === 'colorado', `defaultState should be "colorado", got "${config.defaultState}"`);
assert(config.states && config.states.colorado, 'states.colorado exists');
assert(config.states && config.states.virginia, 'states.virginia exists');
assert(config.states.colorado.fips === '08', 'Colorado FIPS is 08');
assert(config.states.colorado.abbreviation === 'CO', 'Colorado abbreviation is CO');
assert(config.states.colorado.defaultJurisdiction === 'douglas', 'Colorado default jurisdiction is douglas');
assert(config.states.colorado.dataDir === 'CDOT', 'Colorado dataDir is CDOT');
assert(config.states.virginia.fips === '51', 'Virginia FIPS is 51');
assert(config.states.virginia.abbreviation === 'VA', 'Virginia abbreviation is VA');

// ─────────────────────────────────────────────
// 2. JURISDICTION ENTRIES — Colorado
// ─────────────────────────────────────────────
section('2. Colorado Jurisdictions in config.json');

const allJuris = config.jurisdictions;
assert(allJuris && typeof allJuris === 'object', 'jurisdictions object exists');

// Colorado entries
const coEntries = Object.entries(allJuris).filter(([id, j]) => j.state === 'CO');
assert(coEntries.length === 64, `Should have 64 CO jurisdictions, found ${coEntries.length}`);

// Douglas must exist (primary deployment target)
assert(allJuris.douglas, 'Douglas County entry exists');
assert(allJuris.douglas.fips === '035', 'Douglas County FIPS is 035');
assert(allJuris.douglas.state === 'CO', 'Douglas County tagged with state: CO');
assert(allJuris.douglas.mapCenter && allJuris.douglas.mapCenter.length === 2, 'Douglas County has mapCenter');
assert(allJuris.douglas.maintainsOwnRoads === true, 'Douglas County maintainsOwnRoads');

// Key Colorado counties
const keyCoCounties = ['co_denver', 'co_el_paso', 'co_boulder', 'co_larimer', 'co_arapahoe', 'co_jefferson', 'co_adams', 'co_weld', 'co_pueblo', 'co_mesa'];
keyCoCounties.forEach(key => {
    assert(allJuris[key], `Key CO county "${key}" exists in config`);
    if (allJuris[key]) {
        assert(allJuris[key].state === 'CO', `${key} has state: CO`);
        assert(allJuris[key].fips, `${key} has FIPS code`);
        assert(allJuris[key].namePatterns && allJuris[key].namePatterns.length >= 2, `${key} has namePatterns`);
    }
});

// Check no FIPS collisions within Colorado
const coFipsMap = {};
let fipsCollision = false;
coEntries.forEach(([id, j]) => {
    if (coFipsMap[j.fips]) {
        fipsCollision = true;
        console.error(`  FIPS collision: ${j.fips} used by both "${id}" and "${coFipsMap[j.fips]}"`);
    }
    coFipsMap[j.fips] = id;
});
assert(!fipsCollision, 'No FIPS code collisions within Colorado');

// ─────────────────────────────────────────────
// 3. JURISDICTION ENTRIES — Virginia
// ─────────────────────────────────────────────
section('3. Virginia Jurisdictions in config.json');

const vaEntries = Object.entries(allJuris).filter(([id, j]) => {
    if (id.startsWith('_')) return false;
    return !j.state; // Legacy VA entries have no state tag
});
assert(vaEntries.length >= 130, `Should have 130+ VA jurisdictions, found ${vaEntries.length}`);

// Virginia has independent cities
const vaCities = vaEntries.filter(([, j]) => j.type === 'city');
assert(vaCities.length >= 30, `VA should have 30+ independent cities, found ${vaCities.length}`);

// ─────────────────────────────────────────────
// 4. STATE HIERARCHY FILES
// ─────────────────────────────────────────────
section('4. State Hierarchy Files');

['colorado', 'virginia'].forEach(state => {
    const hierPath = path.join(ROOT, 'states', state, 'hierarchy.json');
    const exists = fs.existsSync(hierPath);
    assert(exists, `states/${state}/hierarchy.json exists`);
    if (!exists) return;

    const hier = JSON.parse(fs.readFileSync(hierPath, 'utf8'));
    assert(hier.state, `${state} hierarchy has state info`);
    assert(hier.regions && Object.keys(hier.regions).length > 0, `${state} hierarchy has regions`);
    assert(hier.allCounties && Object.keys(hier.allCounties).length > 0, `${state} hierarchy has allCounties`);

    if (state === 'colorado') {
        assert(Object.keys(hier.allCounties).length === 64, `Colorado has 64 counties in hierarchy`);
        assert(Object.keys(hier.regions).length === 5, `Colorado has 5 CDOT regions`);
        assert(hier.tprs && Object.keys(hier.tprs).length > 0, `Colorado has TPRs/MPOs`);
        assert(hier.corridors && Object.keys(hier.corridors).length > 0, `Colorado has corridors`);

        // Verify Douglas County is in a region
        const douglasInRegion = Object.values(hier.regions).some(r => r.counties && r.counties.includes('035'));
        assert(douglasInRegion, 'Douglas County (035) is assigned to a CDOT region');
    }

    if (state === 'virginia') {
        assert(Object.keys(hier.regions).length === 9, `Virginia has 9 VDOT districts`);
    }
});

// ─────────────────────────────────────────────
// 5. BOUNDARY CONFIG FILES
// ─────────────────────────────────────────────
section('5. Boundary Configuration Files');

['colorado', 'virginia'].forEach(state => {
    const bndPath = path.join(ROOT, 'states', state, 'boundaries.json');
    const exists = fs.existsSync(bndPath);
    assert(exists, `states/${state}/boundaries.json exists`);
    if (!exists) return;

    const bnd = JSON.parse(fs.readFileSync(bndPath, 'utf8'));
    assert(bnd.dotDistricts, `${state} boundaries has dotDistricts config`);
    assert(bnd.dotDistricts.endpoint, `${state} boundaries has ArcGIS endpoint`);
    assert(bnd.mpo, `${state} boundaries has MPO config`);
    assert(bnd.counties, `${state} boundaries has counties config`);
});

// ─────────────────────────────────────────────
// 6. STATE CONFIG FILES
// ─────────────────────────────────────────────
section('6. State-Specific Config Files');

['colorado', 'virginia'].forEach(state => {
    const cfgPath = path.join(ROOT, 'states', state, 'config.json');
    const exists = fs.existsSync(cfgPath);
    assert(exists, `states/${state}/config.json exists`);
    if (!exists) return;

    const cfg = JSON.parse(fs.readFileSync(cfgPath, 'utf8'));
    assert(cfg.state, `${state} config has state info`);
    assert(cfg.columnMapping, `${state} config has columnMapping`);
    assert(cfg.roadSystems, `${state} config has roadSystems`);

    if (state === 'colorado') {
        assert(cfg.state.abbreviation === 'CO', 'CO config abbreviation matches');
        assert(cfg.jurisdictions && cfg.jurisdictions.douglas, 'CO config has douglas jurisdiction');
        assert(cfg.columnMapping.ROUTE === 'Rd_Number', 'CO route column is Rd_Number');
        assert(cfg.derivedFields && cfg.derivedFields.SEVERITY, 'CO has derived severity field');
    }

    // Virginia config may not have jurisdictions (they're in the main config.json)
    if (state === 'virginia' && !cfg.jurisdictions) {
        skip(`${state} config does not have jurisdictions (stored in main config.json)`);
    }
});

// ─────────────────────────────────────────────
// 7. VIEW TIER CONFIGURATION
// ─────────────────────────────────────────────
section('7. View Tier Tab Visibility Matrix');

// Parse the TIER_TAB_VISIBILITY from index.html
const indexHtml = fs.readFileSync(path.join(ROOT, 'app', 'index.html'), 'utf8');

const tierVisMatch = indexHtml.match(/const TIER_TAB_VISIBILITY\s*=\s*\{[\s\S]*?\};/);
assert(tierVisMatch, 'TIER_TAB_VISIBILITY is defined in index.html');

if (tierVisMatch) {
    // Eval in safe context
    const tierVis = eval(`(function(){ ${tierVisMatch[0]} return TIER_TAB_VISIBILITY; })()`);
    const expectedTiers = ['federal', 'state', 'region', 'mpo', 'county', 'city', 'corridor'];
    expectedTiers.forEach(tier => {
        assert(tierVis[tier], `TIER_TAB_VISIBILITY has "${tier}" tier`);
        if (tierVis[tier]) {
            assert(typeof tierVis[tier].dashboard === 'number', `${tier} tier has dashboard visibility`);
            assert(typeof tierVis[tier].map === 'number', `${tier} tier has map visibility`);
        }
    });

    // County tier should have all tabs visible
    assert(tierVis.county.dashboard === 1, 'County tier: dashboard visible');
    assert(tierVis.county.deepDive === 1, 'County tier: deepDive visible');
    assert(tierVis.county.crashPrediction === 1, 'County tier: crashPrediction visible');

    // Federal tier should hide detailed tabs
    assert(tierVis.federal.deepDive === 0, 'Federal tier: deepDive hidden');
    assert(tierVis.federal.crashPrediction === 0, 'Federal tier: crashPrediction hidden');
}

// ─────────────────────────────────────────────
// 8. TIER EXTENSION & JURISDICTION CONTEXT
// ─────────────────────────────────────────────
section('8. _TIER_EXTENSIONS & jurisdictionContext');

const tierExtMatch = indexHtml.match(/const _TIER_EXTENSIONS\s*=\s*\{[\s\S]*?\};/);
assert(tierExtMatch, '_TIER_EXTENSIONS is defined');

if (tierExtMatch) {
    const tierExt = eval(`(function(){ ${tierExtMatch[0]} return _TIER_EXTENSIONS; })()`);
    assert(tierExt.viewTier === 'county', 'Default viewTier is county');
    assert(tierExt.tierState === null, 'Default tierState is null');
    assert(tierExt.tierRegion === null, 'Default tierRegion is null');
    assert(tierExt.tierMpo === null, 'Default tierMpo is null');
    assert(tierExt.tierRoadType === 'all_roads', 'Default tierRoadType is all_roads');
    assert(tierExt.hierarchyLoaded === false, 'Default hierarchyLoaded is false');
    assert(tierExt.boundariesLoaded === false, 'Default boundariesLoaded is false');
}

// Check jurisdictionContext default state
assert(indexHtml.includes("stateCode: 'CO'"), 'jurisdictionContext defaults to CO');
assert(indexHtml.includes("stateFips: '08'"), 'jurisdictionContext defaults to FIPS 08');
assert(indexHtml.includes("stateName: 'Colorado'"), 'jurisdictionContext defaults to Colorado');
assert(indexHtml.includes('..._TIER_EXTENSIONS'), 'jurisdictionContext spreads _TIER_EXTENSIONS');

// ─────────────────────────────────────────────
// 9. KEY FUNCTIONS EXIST
// ─────────────────────────────────────────────
section('9. Key Functions Exist in index.html');

const requiredFunctions = [
    'setViewTier',
    'updateTabVisibilityForTier',
    'updateTierSelectorUI',
    'handleTierChange',
    'populateRegionDropdown',
    'populateMPODropdown',
    'handleRegionSelection',
    'handleMPOSelection',
    'populateStateDropdown',
    'populateJurisdictionDropdown',
    'handleStateSelection',
    'applyDynamicStateConfig',
    'loadAppConfig',
    'getMinimalFallbackConfig',
    '_fipsToAbbr',
    'loadSavedSelections',
    'getActiveJurisdictionId',
    'buildJurisdictionContextFromSelection',
    'restoreJurisdictionContext'
];

requiredFunctions.forEach(fn => {
    const pattern = new RegExp(`function\\s+${fn}\\s*\\(`);
    assert(pattern.test(indexHtml), `Function "${fn}" exists`);
});

// ─────────────────────────────────────────────
// 10. MODULE IIFEs EXIST
// ─────────────────────────────────────────────
section('10. IIFE Module Definitions');

const requiredModules = [
    'HierarchyRegistry',
    'BoundaryService',
    'SpatialClipService',
    'AggregateLoader'
];

requiredModules.forEach(mod => {
    const pattern = new RegExp(`const ${mod}\\s*=\\s*\\(\\(\\)\\s*=>\\s*\\{`);
    assert(pattern.test(indexHtml), `Module "${mod}" is defined as IIFE`);
});

// ─────────────────────────────────────────────
// 11. NO DUPLICATE FUNCTION NAMES
// ─────────────────────────────────────────────
section('11. No Duplicate Function Names');

const funcDecls = indexHtml.match(/function\s+(\w+)\s*\(/g) || [];
const funcNames = funcDecls.map(d => d.match(/function\s+(\w+)/)[1]);
const funcCounts = {};
funcNames.forEach(name => { funcCounts[name] = (funcCounts[name] || 0) + 1; });

const duplicates = Object.entries(funcCounts).filter(([, count]) => count > 1);
// Filter to only NEW duplicates introduced by jurisdiction expansion
const newFunctions = ['setViewTier', 'updateTabVisibilityForTier', 'updateTierSelectorUI',
    'handleTierChange', 'populateRegionDropdown', 'populateMPODropdown',
    'handleRegionSelection', 'handleMPOSelection', '_fipsToAbbr'];
const newDuplicates = duplicates.filter(([name]) => newFunctions.includes(name));
if (duplicates.length > 0) {
    duplicates.forEach(([name, count]) => {
        const isNew = newFunctions.includes(name);
        console.log(`  ${isNew ? 'FAIL' : 'WARN'}: Function "${name}" declared ${count} times${isNew ? '' : ' (pre-existing)'}`);
    });
}
assert(newDuplicates.length === 0, `No NEW duplicate function declarations from jurisdiction expansion (found ${newDuplicates.length})`);

// ─────────────────────────────────────────────
// 12. DOM ELEMENT IDs IN HTML
// ─────────────────────────────────────────────
section('12. Required DOM Element IDs');

const requiredElements = [
    'stateSelect',
    'jurisdictionSelect',
    'tierSelector',
    'tierRegionSelect',
    'tierMPOSelect',
    'tierRegionRow',
    'tierMPORow',
    'tierScopeIndicator',
    'tierScopeText',
    'stateStatusText'
];

requiredElements.forEach(id => {
    const pattern = new RegExp(`id=["']${id}["']`);
    assert(pattern.test(indexHtml), `DOM element #${id} exists`);
});

// ─────────────────────────────────────────────
// 13. HIERARCHY ↔ CONFIG CONSISTENCY
// ─────────────────────────────────────────────
section('13. Hierarchy ↔ Config Consistency');

const coHier = JSON.parse(fs.readFileSync(path.join(ROOT, 'states', 'colorado', 'hierarchy.json'), 'utf8'));

// Every county in hierarchy should exist in config
const configCoKeys = new Set(
    Object.entries(allJuris)
        .filter(([, j]) => j.state === 'CO')
        .map(([, j]) => j.fips)
);

let missingFromConfig = 0;
Object.entries(coHier.allCounties).forEach(([fips, name]) => {
    if (!configCoKeys.has(fips)) {
        missingFromConfig++;
        console.error(`  County FIPS ${fips} (${name}) in hierarchy but missing from config.json`);
    }
});
assert(missingFromConfig === 0, `All hierarchy counties present in config.json (${missingFromConfig} missing)`);

// Every county in config should exist in hierarchy
const hierFips = new Set(Object.keys(coHier.allCounties));
let missingFromHier = 0;
coEntries.forEach(([id, j]) => {
    if (!hierFips.has(j.fips)) {
        missingFromHier++;
        console.error(`  Config entry "${id}" (FIPS ${j.fips}) missing from hierarchy`);
    }
});
assert(missingFromHier === 0, `All config counties present in hierarchy (${missingFromHier} missing)`);

// ─────────────────────────────────────────────
// 14. REGION → COUNTY MEMBERSHIP
// ─────────────────────────────────────────────
section('14. Region → County Membership Coverage');

// Every Colorado county should be in at least one region
const countiesInAnyRegion = new Set();
Object.values(coHier.regions).forEach(region => {
    (region.counties || []).forEach(fips => countiesInAnyRegion.add(fips));
});

let orphanCounties = 0;
Object.keys(coHier.allCounties).forEach(fips => {
    if (!countiesInAnyRegion.has(fips)) {
        orphanCounties++;
        console.error(`  County ${fips} (${coHier.allCounties[fips]}) not assigned to any region`);
    }
});
assert(orphanCounties === 0, `All 64 counties assigned to a region (${orphanCounties} orphans)`);

// ─────────────────────────────────────────────
// 15. DROPDOWN FILTERING LOGIC
// ─────────────────────────────────────────────
section('15. Jurisdiction Dropdown State Filtering');

// Simulate the filtering logic from populateJurisdictionDropdown
function simulateFilter(activeStateAbbr) {
    const filtered = [];
    for (const [id, data] of Object.entries(allJuris)) {
        if (id.startsWith('_')) continue;
        const entryState = data.state || null;
        if (entryState) {
            if (entryState !== activeStateAbbr) continue;
        } else {
            if (activeStateAbbr !== 'VA') continue;
        }
        filtered.push({ id, ...data });
    }
    return filtered;
}

const coFiltered = simulateFilter('CO');
assert(coFiltered.length === 64, `CO filter returns 64 counties, got ${coFiltered.length}`);
assert(coFiltered.some(j => j.id === 'douglas'), 'CO filter includes douglas');
assert(!coFiltered.some(j => j.id === 'henrico'), 'CO filter excludes henrico (VA)');
assert(!coFiltered.some(j => j.id === 'accomack'), 'CO filter excludes accomack (VA)');

const vaFiltered = simulateFilter('VA');
assert(vaFiltered.length >= 130, `VA filter returns 130+ jurisdictions, got ${vaFiltered.length}`);
assert(vaFiltered.some(j => j.id === 'henrico'), 'VA filter includes henrico');
assert(!vaFiltered.some(j => j.id === 'douglas'), 'VA filter excludes douglas (CO)');
assert(!vaFiltered.some(j => j.id === 'co_denver'), 'VA filter excludes co_denver');

// Filtering for a state with no entries should return empty
const txFiltered = simulateFilter('TX');
assert(txFiltered.length === 0, `TX filter returns 0 (no TX jurisdictions), got ${txFiltered.length}`);

// ─────────────────────────────────────────────
// 16. DATA DIRECTORY STRUCTURE
// ─────────────────────────────────────────────
section('16. Data Directory Structure');

assert(fs.existsSync(path.join(ROOT, 'data')), 'data/ directory exists');
assert(fs.existsSync(path.join(ROOT, 'data', 'CDOT')), 'data/CDOT/ directory exists');
assert(fs.existsSync(path.join(ROOT, 'states')), 'states/ directory exists');
assert(fs.existsSync(path.join(ROOT, 'states', 'colorado')), 'states/colorado/ directory exists');
assert(fs.existsSync(path.join(ROOT, 'states', 'virginia')), 'states/virginia/ directory exists');
assert(fs.existsSync(path.join(ROOT, 'states', 'us_counties_db.js')), 'states/us_counties_db.js exists');
assert(fs.existsSync(path.join(ROOT, 'states', 'fips_database.js')), 'states/fips_database.js exists');
assert(fs.existsSync(path.join(ROOT, 'states', 'state_adapter.js')), 'states/state_adapter.js exists');
assert(fs.existsSync(path.join(ROOT, 'scripts', 'generate_aggregates.py')), 'scripts/generate_aggregates.py exists');

// ─────────────────────────────────────────────
// 17. FALLBACK CONFIG DEFAULTS
// ─────────────────────────────────────────────
section('17. Fallback Configuration Defaults');

// Check getMinimalFallbackConfig has Colorado defaults
const fallbackMatch = indexHtml.match(/function getMinimalFallbackConfig\(\)\s*\{[\s\S]*?\n\}/);
assert(fallbackMatch, 'getMinimalFallbackConfig function exists');
if (fallbackMatch) {
    const fallbackCode = fallbackMatch[0];
    assert(fallbackCode.includes('"colorado"'), 'Fallback config includes colorado default state');
    assert(fallbackCode.includes('"douglas"'), 'Fallback config includes douglas jurisdiction');
    assert(fallbackCode.includes('defaultState: "colorado"'), 'Fallback defaultState is colorado');
}

// MAP_CENTER default
assert(indexHtml.includes('MAP_CENTER = [39.33, -104.93]'), 'MAP_CENTER defaults to Douglas County CO');

// _fipsToAbbr function exists with CO mapping
assert(indexHtml.includes("'08': 'CO'"), '_fipsToAbbr has Colorado FIPS mapping');

// ─────────────────────────────────────────────
// 18. ERROR HANDLING IN ASYNC HANDLERS
// ─────────────────────────────────────────────
section('18. Error Handling in Async Tier Handlers');

['handleTierChange', 'handleRegionSelection', 'handleMPOSelection'].forEach(fn => {
    const fnMatch = indexHtml.match(new RegExp(`async function ${fn}\\([^)]*\\)\\s*\\{[\\s\\S]*?\\n\\}`));
    if (fnMatch) {
        assert(fnMatch[0].includes('try {'), `${fn} has try block`);
        assert(fnMatch[0].includes('catch'), `${fn} has catch block`);
        assert(fnMatch[0].includes('console.error'), `${fn} logs errors`);
    } else {
        assert(false, `${fn} function body found for error handling check`);
    }
});

// ─────────────────────────────────────────────
// 19. NO HARDCODED VIRGINIA-ONLY DEFAULTS
// ─────────────────────────────────────────────
section('19. Colorado-Ready Defaults (No Hardcoded Virginia-Only Fallbacks)');

// Check critical fallback values are not Virginia-only
assert(!indexHtml.includes("select.value = '51'; // Default Virginia"), 'No hardcoded Virginia FIPS in state selector');
assert(indexHtml.includes("configDefaultFips"), 'State selector uses dynamic configDefaultFips');

// Check MAP_CENTER is not Henrico
assert(!indexHtml.includes('MAP_CENTER = [37.55, -77.45]'), 'MAP_CENTER not hardcoded to Henrico');

// ─────────────────────────────────────────────
// 20. PYTHON AGGREGATE SCRIPT
// ─────────────────────────────────────────────
section('20. Python Aggregate Generator Script');

const pyPath = path.join(ROOT, 'scripts', 'generate_aggregates.py');
if (fs.existsSync(pyPath)) {
    const pyContent = fs.readFileSync(pyPath, 'utf8');
    assert(pyContent.includes('def compute_county_stats'), 'Script has compute_county_stats');
    assert(pyContent.includes('def generate_statewide_aggregates'), 'Script has generate_statewide_aggregates');
    assert(pyContent.includes('def generate_group_aggregates'), 'Script has generate_group_aggregates');
    assert(pyContent.includes('--state'), 'Script supports --state argument');
    assert(pyContent.includes('argparse'), 'Script uses argparse');
} else {
    skip('generate_aggregates.py not found');
}

// ─────────────────────────────────────────────
// RESULTS SUMMARY
// ─────────────────────────────────────────────
console.log('\n' + '='.repeat(50));
console.log(`RESULTS: ${passed} passed, ${failed} failed, ${skipped} skipped`);
console.log('='.repeat(50));

if (errors.length > 0) {
    console.log('\nFailed tests:');
    errors.forEach((e, i) => console.log(`  ${i + 1}. ${e}`));
}

process.exit(failed > 0 ? 1 : 0);
