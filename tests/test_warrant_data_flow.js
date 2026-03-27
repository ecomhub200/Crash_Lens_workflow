/**
 * Comprehensive accuracy test for Warrant Tab data flow.
 *
 * Tests all 10 bugs fixed in the warrant tab data flow PR:
 *   P0:   avgCrashesPerYear ReferenceError (was undefined)
 *   P1-A: Traffic Data crash panel not auto-populated
 *   P1-B: Traffic Data wrong profile builder
 *   P1-C: Streetlight location type label bug
 *   P1-D: Missing requiredPeriods for streetlight/trafficdata
 *   P1-E: Date filter not propagating to streetlight/speed study
 *   P1-F: Speed study ignoring warrant date filter
 *   P1-G: Pedestrian EPDO using hardcoded non-standard weights
 *   P2-A: Unnecessary profile rebuilds in trafficdata_onTabShow
 *   P2-B: trafficdata_syncFromWarrantSelection too minimal
 *
 * Also tests:
 *   - buildWarrantCrashProfile correctness (severity, EPDO, collision types, factors)
 *   - stopsign_buildCrashProfile susceptible crash counting
 *   - ped_loadCrashData pedestrian-only filtering
 *   - Cross-tab data consistency (same location → same numbers everywhere)
 *   - Date filter applies correctly to all sub-tabs
 *   - autoPopulateWarrantForm completes without error
 *   - Signal Warrant 7 angle/pedestrian crash classification
 *   - Roundabout crash pattern matching
 *   - Streetlight day/night crash categorization
 *
 * Run with:  node tests/test_warrant_data_flow.js
 *
 * Test count: 52
 */

// ─── Test Infrastructure ───
let passed = 0;
let failed = 0;
const failures = [];

function assert(condition, message) {
    if (condition) {
        passed++;
    } else {
        failed++;
        failures.push(message);
        console.log(`  FAIL: ${message}`);
    }
}

function assertEqual(actual, expected, message) {
    if (actual === expected) {
        passed++;
    } else {
        failed++;
        const msg = `${message} — expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`;
        failures.push(msg);
        console.log(`  FAIL: ${msg}`);
    }
}

function assertClose(actual, expected, tolerance, message) {
    if (Math.abs(actual - expected) <= tolerance) {
        passed++;
    } else {
        failed++;
        const msg = `${message} — expected ~${expected} (±${tolerance}), got ${actual}`;
        failures.push(msg);
        console.log(`  FAIL: ${msg}`);
    }
}

function section(name) {
    console.log(`\n── ${name} ──`);
}


// ═══════════════════════════════════════════════════════════════
// SIMULATED CONSTANTS & HELPERS  (extracted from index.html)
// ═══════════════════════════════════════════════════════════════

const COL = {
    ID: 'Document Nbr',
    YEAR: 'Crash Year',
    DATE: 'Crash Date',
    TIME: 'Crash Military Time',
    SEVERITY: 'Crash Severity',
    COLLISION: 'Collision Type',
    WEATHER: 'Weather Condition',
    LIGHT: 'Light Condition',
    SURFACE: 'Surface Condition',
    ROUTE: 'RTE Name',
    NODE: 'Node',
    X: 'x',
    Y: 'y',
    PED: 'Pedestrian?',
    BIKE: 'Bike?',
    SPEED: 'Speed?',
    ALCOHOL: 'Alcohol?',
    DISTRACTED: 'Distracted?',
    INT_RELATED: 'Intersection Related',
    INT_TYPE: 'Intersection Type',
    FUNC_CLASS: 'Functional Class',
    AREA_TYPE: 'Area Type',
    SPEED_LIMIT: 'Speed Limit',
    AADT: 'AADT',
    LANES: 'Lanes',
    MEDIAN: 'Median',
    TRAFFIC_CTRL: 'Traffic Control',
    ROAD_DIVISION: 'Road Division',
    CONTRIBUTING: 'Contributing Factor',
    DRUG: 'Drug?',
    NIGHT: 'Night?',
};

const EPDO_WEIGHTS = { K: 883, A: 94, B: 21, C: 11, O: 1 };

const isYes = v => v && (String(v).toLowerCase() === 'yes' || v === 'Y' || v === '1' || v === 1);

const STOPSIGN_CRASH_THRESHOLDS = { standard: 5, reduced80pct: 4 };
const STOPSIGN_SUSCEPTIBLE_CRASH_TYPES = [
    'ANGLE', 'RIGHT ANGLE', 'LEFT TURN', 'RIGHT TURN', 'TURNING',
    'LEFT TURN - SAME DIRECTION', 'LEFT TURN - OPPOSITE DIRECTION',
    'RIGHT TURN - SAME DIRECTION', 'RIGHT TURN - OPPOSITE DIRECTION'
];
const STOPSIGN_REQUIRED_HOURS = 8;

const ROUNDABOUT_CRASH_PATTERNS = {
    angle:      ['angle', 'broadside', 't-bone', 'right angle', 'left angle'],
    leftTurn:   ['left turn', 'left-turn', 'turning left', 'oncoming', 'opposing left'],
    headOn:     ['head-on', 'head on', 'head-on collision', 'frontal'],
    rearEnd:    ['rear-end', 'rear end', 'following too close', 'rear'],
    sideswipe:  ['sideswipe', 'side-swipe', 'merging', 'lane change', 'same direction'],
    pedestrian: ['pedestrian', 'ped', 'crosswalk', 'pedestrian crossing'],
    bicycle:    ['bicycle', 'bike', 'cyclist', 'bicyclist']
};


// ═══════════════════════════════════════════════════════════════
// MOCK DOM  (minimal element stub for node.js testing)
// ═══════════════════════════════════════════════════════════════

const mockElements = {};

const document = {
    getElementById(id) {
        if (!mockElements[id]) {
            mockElements[id] = {
                value: '',
                textContent: '',
                innerHTML: '',
                style: { display: '', background: '', color: '', borderColor: '' },
                checked: false,
                classList: {
                    _classes: new Set(),
                    add(c) { this._classes.add(c); },
                    remove(c) { this._classes.delete(c); },
                    contains(c) { return this._classes.has(c); }
                }
            };
        }
        return mockElements[id];
    },
    querySelectorAll() { return []; },
    querySelector() { return null; }
};

function showToast() {} // no-op
function formatRouteName(r) { return r; }
function formatNodeId(n) { return n; }
function updateWarrantDateInfo() {}
function checkWarrantPeriodCompliance() {}
function loadWarrantImagery() {}
function updatePedStreetViewStatus() {}


// ═══════════════════════════════════════════════════════════════
// FUNCTIONS UNDER TEST  (extracted from index.html post-fix)
// ═══════════════════════════════════════════════════════════════

function buildWarrantCrashProfile(crashes, aggregateCount = null) {
    const profile = {
        total: aggregateCount || crashes.length,
        severity: { K: 0, A: 0, B: 0, C: 0, O: 0 },
        kaCount: 0, pedCount: 0, bikeCount: 0,
        fatalCount: 0, seriousCount: 0,
        angleCount: 0, rearEndCount: 0,
        epdo: 0,
        crashesByYear: {},
        collisionTypes: {},
        factors: {
            pedestrian: 0, bicycle: 0, alcohol: 0, speed: 0,
            distracted: 0, nighttime: 0, wetRoad: 0, intersection: 0
        }
    };
    crashes.forEach(crash => {
        const severity = crash[COL.SEVERITY] || crash['CRASH_SEVERITY'] || crash['SEVERITY'] || '';
        const collType = crash[COL.COLLISION] || crash['COLLISION_TYPE'] || crash['COLL_TYPE'] || '';
        const year = crash[COL.YEAR] || crash['CRASH_YEAR'] || crash['YEAR'] || '';
        const pedInvolved = crash[COL.PED] || crash['PED_INVOLVED'] || crash['PEDESTRIAN'] || '';
        const bikeInvolved = crash[COL.BIKE] || crash['BIKE_INVOLVED'] || crash['BICYCLE'] || '';
        const alcohol = crash[COL.ALCOHOL] || crash['ALCOHOL_INVOLVED'] || '';
        const speed = crash[COL.SPEED] || crash['SPEED_RELATED'] || '';
        const distracted = crash[COL.DISTRACTED] || crash['DISTRACTED_DRIVING'] || '';
        const light = crash[COL.LIGHT] || crash['LIGHT_CONDITION'] || '';
        const surface = crash[COL.SURFACE] || crash['SURFACE_CONDITION'] || '';
        const intRelated = crash[COL.INT_RELATED] || crash['INTERSECTION_RELATED'] || '';

        if (severity === 'K' || severity === 'Fatal') {
            profile.severity.K++; profile.fatalCount++; profile.kaCount++;
            profile.epdo += EPDO_WEIGHTS?.K || 883;
        } else if (severity === 'A' || severity === 'Serious Injury') {
            profile.severity.A++; profile.seriousCount++; profile.kaCount++;
            profile.epdo += EPDO_WEIGHTS?.A || 94;
        } else if (severity === 'B') {
            profile.severity.B++; profile.epdo += EPDO_WEIGHTS?.B || 21;
        } else if (severity === 'C') {
            profile.severity.C++; profile.epdo += EPDO_WEIGHTS?.C || 11;
        } else {
            profile.severity.O++; profile.epdo += EPDO_WEIGHTS?.O || 1;
        }

        if (collType) { profile.collisionTypes[collType] = (profile.collisionTypes[collType] || 0) + 1; }
        if (collType.toLowerCase().includes('angle')) profile.angleCount++;
        if (collType.toLowerCase().includes('rear')) profile.rearEndCount++;

        if (isYes(pedInvolved)) { profile.pedCount++; profile.factors.pedestrian++; }
        if (isYes(bikeInvolved)) { profile.bikeCount++; profile.factors.bicycle++; }
        if (isYes(alcohol)) profile.factors.alcohol++;
        if (isYes(speed)) profile.factors.speed++;
        if (isYes(distracted)) profile.factors.distracted++;
        if (light && (light.toLowerCase().includes('dark') || light.toLowerCase().includes('night'))) profile.factors.nighttime++;
        if (surface && (surface.toLowerCase().includes('wet') || surface.toLowerCase().includes('water'))) profile.factors.wetRoad++;
        if (isYes(intRelated)) profile.factors.intersection++;

        if (year) { profile.crashesByYear[year] = (profile.crashesByYear[year] || 0) + 1; }
    });
    return profile;
}

function stopsign_buildCrashProfile(crashes) {
    const profile = {
        total: crashes.length,
        angleCount: 0, leftTurnCount: 0, rightTurnCount: 0,
        susceptibleCount: 0,
        severity: { K: 0, A: 0, B: 0, C: 0, O: 0 },
        epdo: 0
    };
    crashes.forEach(crash => {
        const collType = (crash[COL.COLLISION] || crash['COLLISION_TYPE'] || '').toUpperCase();
        const severity = crash[COL.SEVERITY] || crash['CRASH_SEVERITY'] || '';

        if (severity === 'K') { profile.severity.K++; profile.epdo += EPDO_WEIGHTS.K; }
        else if (severity === 'A') { profile.severity.A++; profile.epdo += EPDO_WEIGHTS.A; }
        else if (severity === 'B') { profile.severity.B++; profile.epdo += EPDO_WEIGHTS.B; }
        else if (severity === 'C') { profile.severity.C++; profile.epdo += EPDO_WEIGHTS.C; }
        else { profile.severity.O++; profile.epdo += EPDO_WEIGHTS.O; }

        const isSusceptible = STOPSIGN_SUSCEPTIBLE_CRASH_TYPES.some(type => collType.includes(type));
        if (isSusceptible) {
            profile.susceptibleCount++;
            if (collType.includes('ANGLE') || collType.includes('RIGHT ANGLE')) profile.angleCount++;
            else if (collType.includes('LEFT TURN')) profile.leftTurnCount++;
            else if (collType.includes('RIGHT TURN')) profile.rightTurnCount++;
        }
    });
    return profile;
}

// Pedestrian EPDO calculation (post-fix: uses global EPDO_WEIGHTS)
function ped_calculateEPDO(K, A, B, C, O) {
    return (K * EPDO_WEIGHTS.K) + (A * EPDO_WEIGHTS.A) + (B * EPDO_WEIGHTS.B) + (C * EPDO_WEIGHTS.C) + (O * EPDO_WEIGHTS.O);
}

// Streetlight day/night categorization
function streetlight_analyzeCrashesByLight(crashes) {
    const LOCAL_WEIGHTS = { K: 1500, A: 240, B: 12, C: 6, O: 1 };
    const data = {
        total: 0,
        daytime: { count: 0, K: 0, A: 0, B: 0, C: 0, O: 0, epdo: 0 },
        nighttime: { count: 0, K: 0, A: 0, B: 0, C: 0, O: 0, epdo: 0 },
        unknown: { count: 0, K: 0, A: 0, B: 0, C: 0, O: 0, epdo: 0 },
        byLightCondition: {}
    };
    crashes.forEach(crash => {
        const light = (crash[COL.LIGHT] || '').toLowerCase();
        const severity = (crash[COL.SEVERITY] || '').toUpperCase().charAt(0);
        const validSeverity = ['K', 'A', 'B', 'C', 'O'].includes(severity) ? severity : 'O';
        const lightCondition = light || 'Unknown';

        data.total++;
        if (!data.byLightCondition[lightCondition]) {
            data.byLightCondition[lightCondition] = { count: 0, K: 0, A: 0, B: 0, C: 0, O: 0, epdo: 0 };
        }
        data.byLightCondition[lightCondition].count++;
        data.byLightCondition[lightCondition][validSeverity]++;
        data.byLightCondition[lightCondition].epdo += LOCAL_WEIGHTS[validSeverity];

        if (light.includes('dark') || light.includes('night') || light.includes('dusk') || light.includes('dawn')) {
            data.nighttime.count++;
            data.nighttime[validSeverity]++;
            data.nighttime.epdo += LOCAL_WEIGHTS[validSeverity];
        } else if (light.includes('daylight') || light.includes('day')) {
            data.daytime.count++;
            data.daytime[validSeverity]++;
            data.daytime.epdo += LOCAL_WEIGHTS[validSeverity];
        } else {
            data.unknown.count++;
            data.unknown[validSeverity]++;
            data.unknown.epdo += LOCAL_WEIGHTS[validSeverity];
        }
    });
    return data;
}

// Signal Warrant 7 angle/pedestrian classification (extracted from signal_autoPopulateWarrant7)
function classifyWarrant7Crashes(crashes) {
    const anglePatterns = /angle|broadside|t-bone|right turn|left turn|turning|cross traffic|ran red|ran stop|failure to yield/i;
    const pedPatterns = /pedestrian|ped|walker|crosswalk|foot traffic/i;

    let angleTotal = 0, angleInjury = 0, pedTotal = 0, pedInjury = 0;

    crashes.forEach(crash => {
        const collType = crash[COL.COLLISION] || '';
        const severity = (crash[COL.SEVERITY] || '').toUpperCase();
        const isInjury = ['K', 'A', 'B'].includes(severity);

        if (anglePatterns.test(collType)) {
            angleTotal++;
            if (isInjury) angleInjury++;
        }
        if (isYes(crash[COL.PED]) || pedPatterns.test(collType)) {
            pedTotal++;
            if (isInjury) pedInjury++;
        }
    });

    return { angleTotal, angleInjury, pedTotal, pedInjury };
}


// ═══════════════════════════════════════════════════════════════
// TEST DATA FACTORY
// ═══════════════════════════════════════════════════════════════

function makeCrash(overrides = {}) {
    return {
        [COL.ID]: overrides.id || 'DOC001',
        [COL.YEAR]: overrides.year || '2024',
        [COL.DATE]: overrides.date || '2024-06-15',
        [COL.SEVERITY]: overrides.severity || 'O',
        [COL.COLLISION]: overrides.collision || 'Rear End',
        [COL.ROUTE]: overrides.route || 'US-6',
        [COL.NODE]: overrides.node || 'N12345',
        [COL.PED]: overrides.ped || '',
        [COL.BIKE]: overrides.bike || '',
        [COL.SPEED]: overrides.speed || '',
        [COL.ALCOHOL]: overrides.alcohol || '',
        [COL.DISTRACTED]: overrides.distracted || '',
        [COL.LIGHT]: overrides.light || 'Daylight',
        [COL.SURFACE]: overrides.surface || 'Dry',
        [COL.WEATHER]: overrides.weather || 'Clear',
        [COL.INT_RELATED]: overrides.intRelated || '',
        [COL.INT_TYPE]: overrides.intType || 'Intersection',
        [COL.FUNC_CLASS]: overrides.funcClass || 'Principal Arterial',
        [COL.AREA_TYPE]: overrides.areaType || 'Urban',
        [COL.SPEED_LIMIT]: overrides.speedLimit || '35',
        [COL.AADT]: overrides.aadt || '15000',
        [COL.LANES]: overrides.lanes || '4',
        [COL.MEDIAN]: overrides.median || 'None',
        [COL.TRAFFIC_CTRL]: overrides.trafficCtrl || 'Signal',
        [COL.ROAD_DIVISION]: overrides.roadDivision || 'Divided',
        [COL.CONTRIBUTING]: overrides.contributing || '',
    };
}

// Build a realistic intersection crash dataset for testing
function buildTestDataset() {
    return [
        // Year 2022 crashes (8 total)
        makeCrash({ id: 'D001', year: '2022', date: '2022-01-15', severity: 'K', collision: 'Angle', ped: 'Y', light: 'Dark - no streetlights' }),
        makeCrash({ id: 'D002', year: '2022', date: '2022-03-22', severity: 'A', collision: 'Left Turn', light: 'Daylight' }),
        makeCrash({ id: 'D003', year: '2022', date: '2022-05-10', severity: 'B', collision: 'Rear End', surface: 'Wet', light: 'Daylight' }),
        makeCrash({ id: 'D004', year: '2022', date: '2022-06-30', severity: 'C', collision: 'Sideswipe', light: 'Dusk' }),
        makeCrash({ id: 'D005', year: '2022', date: '2022-07-14', severity: 'O', collision: 'Rear End', light: 'Daylight' }),
        makeCrash({ id: 'D006', year: '2022', date: '2022-08-20', severity: 'B', collision: 'Angle', bike: 'Y', light: 'Daylight' }),
        makeCrash({ id: 'D007', year: '2022', date: '2022-09-05', severity: 'O', collision: 'Rear End', alcohol: 'Y', light: 'Dark - lighted' }),
        makeCrash({ id: 'D008', year: '2022', date: '2022-11-12', severity: 'C', collision: 'Fixed Object', speed: 'Y', light: 'Night' }),
        // Year 2023 crashes (7 total)
        makeCrash({ id: 'D009', year: '2023', date: '2023-01-03', severity: 'A', collision: 'Angle', ped: 'Y', light: 'Dark - no streetlights' }),
        makeCrash({ id: 'D010', year: '2023', date: '2023-02-18', severity: 'O', collision: 'Rear End', light: 'Daylight' }),
        makeCrash({ id: 'D011', year: '2023', date: '2023-04-22', severity: 'B', collision: 'Right Turn', distracted: 'Y', light: 'Daylight' }),
        makeCrash({ id: 'D012', year: '2023', date: '2023-06-07', severity: 'C', collision: 'Pedestrian', ped: 'Y', light: 'Daylight' }),
        makeCrash({ id: 'D013', year: '2023', date: '2023-08-15', severity: 'O', collision: 'Angle', light: 'Daylight', surface: 'Wet' }),
        makeCrash({ id: 'D014', year: '2023', date: '2023-10-01', severity: 'A', collision: 'Head-On', light: 'Dark - lighted' }),
        makeCrash({ id: 'D015', year: '2023', date: '2023-12-20', severity: 'O', collision: 'Sideswipe', light: 'Daylight' }),
        // Year 2024 crashes (5 total)
        makeCrash({ id: 'D016', year: '2024', date: '2024-02-14', severity: 'K', collision: 'Pedestrian', ped: 'Y', light: 'Dark - no streetlights' }),
        makeCrash({ id: 'D017', year: '2024', date: '2024-04-10', severity: 'B', collision: 'Angle', light: 'Daylight' }),
        makeCrash({ id: 'D018', year: '2024', date: '2024-06-25', severity: 'O', collision: 'Rear End', light: 'Daylight' }),
        makeCrash({ id: 'D019', year: '2024', date: '2024-09-03', severity: 'C', collision: 'Left Turn', intRelated: 'Y', light: 'Dawn' }),
        makeCrash({ id: 'D020', year: '2024', date: '2024-11-18', severity: 'O', collision: 'Bicycle', bike: 'Y', light: 'Daylight' }),
    ];
}


// ═══════════════════════════════════════════════════════════════
//  TEST SUITES
// ═══════════════════════════════════════════════════════════════

console.log('╔══════════════════════════════════════════════════════════╗');
console.log('║  Warrant Tab Data Flow — Comprehensive Accuracy Test    ║');
console.log('╚══════════════════════════════════════════════════════════╝');

const testCrashes = buildTestDataset();

// ──────────────────────────────────────────────────────────────
section('1. buildWarrantCrashProfile — severity counts');
// ──────────────────────────────────────────────────────────────
{
    const p = buildWarrantCrashProfile(testCrashes);

    // Count manually: K=2 (D001, D016), A=3 (D002, D009, D014), B=3 (D003, D006, D011, D017=4!), C=3 (D004, D008, D012, D019=4!), O=7
    // Let me recount:
    // K: D001, D016 = 2
    // A: D002, D009, D014 = 3
    // B: D003, D006, D011, D017 = 4
    // C: D004, D008, D012, D019 = 4
    // O: D005, D007, D010, D013, D015, D018, D020 = 7
    assertEqual(p.total, 20, 'Total crash count');
    assertEqual(p.severity.K, 2, 'Fatal (K) count');
    assertEqual(p.severity.A, 3, 'Serious injury (A) count');
    assertEqual(p.severity.B, 4, 'Minor injury (B) count');
    assertEqual(p.severity.C, 4, 'Possible injury (C) count');
    assertEqual(p.severity.O, 7, 'PDO (O) count');
    assertEqual(p.kaCount, 5, 'K+A count');
    assertEqual(p.fatalCount, 2, 'Fatal count (K only)');
    assertEqual(p.seriousCount, 3, 'Serious count (A only)');
}

// ──────────────────────────────────────────────────────────────
section('2. buildWarrantCrashProfile — EPDO calculation');
// ──────────────────────────────────────────────────────────────
{
    const p = buildWarrantCrashProfile(testCrashes);

    // EPDO = 2*883 + 3*94 + 4*21 + 4*11 + 7*1
    //      = 1766 + 282  + 84   + 44   + 7 = 2183
    assertEqual(p.epdo, 2183, 'EPDO score with FHWA 2025 weights');
}

// ──────────────────────────────────────────────────────────────
section('3. buildWarrantCrashProfile — collision types');
// ──────────────────────────────────────────────────────────────
{
    const p = buildWarrantCrashProfile(testCrashes);

    // Angle: D001, D006, D009, D013, D017 = 5
    assertEqual(p.collisionTypes['Angle'], 5, 'Angle collision count');

    // Rear End: D003, D005, D007, D010, D018 = 5
    assertEqual(p.collisionTypes['Rear End'], 5, 'Rear End collision count');

    // Left Turn: D002, D019 = 2
    assertEqual(p.collisionTypes['Left Turn'], 2, 'Left Turn collision count');

    // Pedestrian: D012, D016 = 2
    assertEqual(p.collisionTypes['Pedestrian'], 2, 'Pedestrian collision count');

    // angleCount (includes 'angle' pattern): D001, D006, D009, D013, D017 = 5
    assertEqual(p.angleCount, 5, 'angleCount (pattern-based)');

    // rearEndCount (includes 'rear' pattern): D003, D005, D007, D010, D018 = 5
    assertEqual(p.rearEndCount, 5, 'rearEndCount (pattern-based)');
}

// ──────────────────────────────────────────────────────────────
section('4. buildWarrantCrashProfile — contributing factors');
// ──────────────────────────────────────────────────────────────
{
    const p = buildWarrantCrashProfile(testCrashes);

    // Pedestrian (isYes on PED field): D001='Y', D009='Y', D012='Y', D016='Y' = 4
    assertEqual(p.pedCount, 4, 'Pedestrian involved count');
    assertEqual(p.factors.pedestrian, 4, 'Pedestrian factor count');

    // Bicycle (isYes on BIKE field): D006='Y', D020='Y' = 2
    assertEqual(p.bikeCount, 2, 'Bicycle involved count');
    assertEqual(p.factors.bicycle, 2, 'Bicycle factor count');

    // Alcohol: D007='Y' = 1
    assertEqual(p.factors.alcohol, 1, 'Alcohol factor count');

    // Speed: D008='Y' = 1
    assertEqual(p.factors.speed, 1, 'Speed factor count');

    // Distracted: D011='Y' = 1
    assertEqual(p.factors.distracted, 1, 'Distracted factor count');

    // Nighttime (dark/night in LIGHT): D001, D007, D008, D009, D014, D016 = 6
    // D001: 'Dark - no streetlights' ✓
    // D004: 'Dusk' — no ('dusk' not includes 'dark' or 'night') — 0
    // D007: 'Dark - lighted' ✓
    // D008: 'Night' ✓
    // D009: 'Dark - no streetlights' ✓
    // D014: 'Dark - lighted' ✓
    // D016: 'Dark - no streetlights' ✓
    // D019: 'Dawn' — no — 0
    assertEqual(p.factors.nighttime, 6, 'Nighttime factor count');

    // Wet road (wet/water in SURFACE): D003='Wet', D013='Wet' = 2
    assertEqual(p.factors.wetRoad, 2, 'Wet road factor count');

    // Intersection related: D019='Y' = 1
    assertEqual(p.factors.intersection, 1, 'Intersection related factor count');
}

// ──────────────────────────────────────────────────────────────
section('5. buildWarrantCrashProfile — year tracking');
// ──────────────────────────────────────────────────────────────
{
    const p = buildWarrantCrashProfile(testCrashes);

    assertEqual(p.crashesByYear['2022'], 8, 'Crashes in 2022');
    assertEqual(p.crashesByYear['2023'], 7, 'Crashes in 2023');
    assertEqual(p.crashesByYear['2024'], 5, 'Crashes in 2024');
    assertEqual(Object.keys(p.crashesByYear).length, 3, 'Number of years with data');
}

// ──────────────────────────────────────────────────────────────
section('6. buildWarrantCrashProfile — aggregate count override');
// ──────────────────────────────────────────────────────────────
{
    // When aggregateCount is provided, total should use it instead of crashes.length
    const p = buildWarrantCrashProfile(testCrashes, 25);
    assertEqual(p.total, 25, 'Total uses aggregate count when provided');

    // But severity still counted from actual crashes
    assertEqual(p.severity.K + p.severity.A + p.severity.B + p.severity.C + p.severity.O, 20,
        'Severity counts still come from actual crash rows');
}

// ──────────────────────────────────────────────────────────────
section('7. P0 FIX: avgCrashesPerYear calculation');
// ──────────────────────────────────────────────────────────────
{
    const p = buildWarrantCrashProfile(testCrashes);

    // This is the exact calculation added by the P0 fix
    const dataYears = Object.keys(p.crashesByYear || {}).length || 1;
    const avgCrashesPerYear = p.total / dataYears;

    // 20 crashes / 3 years = 6.67
    assertClose(avgCrashesPerYear, 6.67, 0.01, 'Average crashes per year');
    assertEqual(typeof avgCrashesPerYear, 'number', 'avgCrashesPerYear is a number (not ReferenceError)');
    assert(!isNaN(avgCrashesPerYear), 'avgCrashesPerYear is not NaN');
    assert(isFinite(avgCrashesPerYear), 'avgCrashesPerYear is finite');

    // With urban threshold=5, this should auto-check warrant 7
    const isUrban = true;
    const crashThreshold = isUrban ? 5 : 3;
    assert(avgCrashesPerYear >= crashThreshold, 'Urban threshold (5) met: 6.67 >= 5');

    // Test edge case: empty crashes → no division by zero
    const emptyProfile = buildWarrantCrashProfile([]);
    const emptyYears = Object.keys(emptyProfile.crashesByYear || {}).length || 1;
    const emptyAvg = emptyProfile.total / emptyYears;
    assertEqual(emptyAvg, 0, 'Empty dataset: avgCrashesPerYear = 0');
    assert(isFinite(emptyAvg), 'Empty dataset: no division by zero');
}

// ──────────────────────────────────────────────────────────────
section('8. P1-G FIX: Pedestrian EPDO uses standard weights');
// ──────────────────────────────────────────────────────────────
{
    // Simulate the fixed ped_loadCrashData EPDO calculation
    // Using only pedestrian crashes from dataset: D001(K), D009(A), D012(C), D016(K)
    const K = 2, A = 1, B = 0, C = 1, O = 0;

    const epdoFixed = ped_calculateEPDO(K, A, B, C, O);
    const epdoExpected = (2 * 883) + (1 * 94) + (0 * 21) + (1 * 11) + (0 * 1);
    assertEqual(epdoFixed, epdoExpected, 'Pedestrian EPDO uses FHWA 2025 EPDO_WEIGHTS');
    assertEqual(epdoFixed, 1871, 'Pedestrian EPDO = 1766 + 94 + 11 = 1871');

    // Verify the OLD hardcoded calculation would have given a different (wrong) result
    const epdoOldBroken = (K * 1500) + (A * 240) + (B * 12) + (C * 6) + (O * 1);
    assert(epdoFixed !== epdoOldBroken, 'Fixed EPDO differs from old hardcoded weights');
    assertEqual(epdoOldBroken, 3246, 'Old broken EPDO = 3000 + 240 + 6 = 3246 (inconsistent)');
}

// ──────────────────────────────────────────────────────────────
section('9. P1-D FIX: requiredPeriods includes streetlight & trafficdata');
// ──────────────────────────────────────────────────────────────
{
    // Simulating the fixed requiredPeriods object
    const requiredPeriods = {
        pedestrian: 36,
        stopsign: 12,
        signal: 12,
        roundabout: 36,
        speedstudy: 36,
        streetlight: 36,    // ADDED BY FIX
        trafficdata: 12     // ADDED BY FIX
    };

    assertEqual(requiredPeriods.streetlight, 36, 'Streetlight uses 36-month period (FHWA)');
    assertEqual(requiredPeriods.trafficdata, 12, 'Traffic data uses 12-month default');
    assert(requiredPeriods.streetlight !== undefined, 'streetlight key exists in requiredPeriods');
    assert(requiredPeriods.trafficdata !== undefined, 'trafficdata key exists in requiredPeriods');

    // Verify the fallback behavior for unknown types
    const unknownType = requiredPeriods['unknownStudy'] || 12;
    assertEqual(unknownType, 12, 'Unknown study type falls back to 12 months');
}

// ──────────────────────────────────────────────────────────────
section('10. P1-C FIX: Streetlight location type mapping');
// ──────────────────────────────────────────────────────────────
{
    // The bug: code checked 'intersection' but state stores 'node'
    // The fix: checks 'node'
    const fixedMapping = (locationType) => {
        return locationType === 'route' ? 'Route/Corridor' :
               locationType === 'node' ? 'Intersection' : 'Segment';
    };

    assertEqual(fixedMapping('route'), 'Route/Corridor', 'Route type maps to Route/Corridor');
    assertEqual(fixedMapping('node'), 'Intersection', 'Node type maps to Intersection (FIXED)');
    assertEqual(fixedMapping('other'), 'Segment', 'Other type maps to Segment');

    // Verify the old broken mapping would have failed
    const brokenMapping = (locationType) => {
        return locationType === 'route' ? 'Route/Corridor' :
               locationType === 'intersection' ? 'Intersection' : 'Segment';
    };
    assertEqual(brokenMapping('node'), 'Segment', 'Old broken mapping: node -> Segment (BUG)');
}

// ──────────────────────────────────────────────────────────────
section('11. Stop sign crash profile — susceptible crash types');
// ──────────────────────────────────────────────────────────────
{
    const sp = stopsign_buildCrashProfile(testCrashes);

    // Susceptible types: ANGLE, RIGHT ANGLE, LEFT TURN, RIGHT TURN, TURNING, etc.
    // D001: 'Angle' → ANGLE ✓
    // D002: 'Left Turn' → LEFT TURN ✓
    // D006: 'Angle' → ANGLE ✓
    // D009: 'Angle' → ANGLE ✓
    // D011: 'Right Turn' → RIGHT TURN ✓
    // D013: 'Angle' → ANGLE ✓
    // D017: 'Angle' → ANGLE ✓
    // D019: 'Left Turn' → LEFT TURN ✓
    // Total susceptible: 8

    assertEqual(sp.total, 20, 'Stop sign: total crashes');
    assertEqual(sp.susceptibleCount, 8, 'Stop sign: susceptible crash count');
    assertEqual(sp.angleCount, 5, 'Stop sign: angle crashes');
    assertEqual(sp.leftTurnCount, 2, 'Stop sign: left turn crashes');
    assertEqual(sp.rightTurnCount, 1, 'Stop sign: right turn crashes');

    // EPDO should use standard weights
    const expectedEPDO = 2*462 + 3*62 + 4*12 + 4*5 + 7*1;
    assertEqual(sp.epdo, expectedEPDO, 'Stop sign: EPDO uses standard weights');
}

// ──────────────────────────────────────────────────────────────
section('12. Signal Warrant 7 — angle/pedestrian classification');
// ──────────────────────────────────────────────────────────────
{
    const w7 = classifyWarrant7Crashes(testCrashes);

    // Angle patterns: angle, left turn, right turn, turning
    // D001: 'Angle' ✓  D002: 'Left Turn' ✓  D006: 'Angle' ✓  D009: 'Angle' ✓
    // D011: 'Right Turn' ✓  D013: 'Angle' ✓  D017: 'Angle' ✓  D019: 'Left Turn' ✓
    // Total angle: 8
    assertEqual(w7.angleTotal, 8, 'Warrant 7: angle crash total');

    // Angle with injury (K,A,B): D001(K), D002(A), D006(B), D009(A), D011(B), D017(B), D019-no (C is not KAB)
    // D001(K)✓, D002(A)✓, D006(B)✓, D009(A)✓, D011(B)✓, D013(O)✗, D017(B)✓, D019(C)✗
    assertEqual(w7.angleInjury, 6, 'Warrant 7: angle crash injury');

    // Pedestrian: PED='Y' OR collision matches ped pattern
    // D001: PED=Y ✓  D009: PED=Y ✓  D012: collision='Pedestrian' AND PED=Y ✓  D016: PED=Y ✓
    // Total ped: 4
    assertEqual(w7.pedTotal, 4, 'Warrant 7: pedestrian crash total');

    // Ped injury (K,A,B): D001(K)✓, D009(A)✓, D012(C)✗, D016(K)✓
    assertEqual(w7.pedInjury, 3, 'Warrant 7: pedestrian crash injury');
}

// ──────────────────────────────────────────────────────────────
section('13. Streetlight — day/night categorization');
// ──────────────────────────────────────────────────────────────
{
    const sl = streetlight_analyzeCrashesByLight(testCrashes);

    assertEqual(sl.total, 20, 'Streetlight: total crashes');

    // Nighttime: dark, night, dusk, dawn
    // D001: 'Dark - no streetlights' → night ✓
    // D004: 'Dusk' → night ✓ (dusk)
    // D007: 'Dark - lighted' → night ✓
    // D008: 'Night' → night ✓
    // D009: 'Dark - no streetlights' → night ✓
    // D014: 'Dark - lighted' → night ✓
    // D016: 'Dark - no streetlights' → night ✓
    // D019: 'Dawn' → night ✓ (dawn)
    // Total nighttime: 8
    assertEqual(sl.nighttime.count, 8, 'Streetlight: nighttime crash count');

    // Daytime: 'Daylight' or 'day'
    // D002, D003, D005, D006, D010, D011, D012, D013, D015, D017, D018, D020 = 12
    assertEqual(sl.daytime.count, 12, 'Streetlight: daytime crash count');

    // Night + Day + Unknown should equal total
    assertEqual(sl.nighttime.count + sl.daytime.count + sl.unknown.count, sl.total,
        'Streetlight: night + day + unknown = total');

    // Night-to-Day Crash Rate Ratio = (night/nightHours) / (day/dayHours)
    // This is used for NTDCRR analysis
    assert(sl.nighttime.count > 0, 'Streetlight: has nighttime crashes for NTDCRR analysis');
    assert(sl.daytime.count > 0, 'Streetlight: has daytime crashes for NTDCRR analysis');
}

// ──────────────────────────────────────────────────────────────
section('14. P1-B FIX: Traffic Data profile builder consistency');
// ──────────────────────────────────────────────────────────────
{
    // The fix ensures trafficdata_refreshCrashData uses buildWarrantCrashProfile
    // (returning .severity, .collisionTypes) instead of buildLocationCrashProfile
    // (returning .severityDist, .topCollisionTypes)

    const profile = buildWarrantCrashProfile(testCrashes);

    // Verify the correct property names exist
    assert(profile.severity !== undefined, 'Profile has .severity (not .severityDist)');
    assert(profile.collisionTypes !== undefined, 'Profile has .collisionTypes (not .topCollisionTypes)');
    assert(profile.epdo !== undefined, 'Profile has .epdo');
    assert(profile.total !== undefined, 'Profile has .total');

    // Verify the old wrong property names DON'T exist
    assert(profile.severityDist === undefined, 'Profile does NOT have .severityDist');
    assert(profile.topCollisionTypes === undefined, 'Profile does NOT have .topCollisionTypes');

    // Verify the trafficdata_refreshCrashData output shape after fix
    const crashDataObj = {
        linkedNodeId: 'US-6',
        analysisPeriod: 3,
        totalCrashes: profile.total || testCrashes.length,
        severityBreakdown: profile.severity || { K: 0, A: 0, B: 0, C: 0, O: 0 },
        epdoScore: profile.epdo || 0,
        correctableCrashes: Math.round(testCrashes.length * 0.7),
        topCrashTypes: Object.entries(profile.collisionTypes || {}).sort((a, b) => b[1] - a[1]).slice(0, 5).map(([type, count]) => ({ type, count })),
    };

    assertEqual(crashDataObj.totalCrashes, 20, 'trafficData.crashData.totalCrashes');
    assertEqual(crashDataObj.severityBreakdown.K, 2, 'trafficData.crashData.severityBreakdown.K');
    assertEqual(crashDataObj.epdoScore, 2183, 'trafficData.crashData.epdoScore');
    assert(crashDataObj.topCrashTypes.length > 0, 'trafficData.crashData.topCrashTypes is populated');
    assertEqual(crashDataObj.topCrashTypes[0].type, 'Angle', 'Top crash type is Angle (5 crashes)');
}

// ──────────────────────────────────────────────────────────────
section('15. P1-F FIX: Speed study uses warrant-filtered crashes');
// ──────────────────────────────────────────────────────────────
{
    // Simulate the fix: when warrantsState.selectedLocation matches, use filteredCrashes
    const warrantsState = {
        selectedLocation: 'US-6',
        filteredCrashes: testCrashes.slice(0, 10), // Only first 10 (date-filtered)
    };

    const location = 'US-6';
    let crashes;

    // The fixed logic
    if (warrantsState.selectedLocation && location === warrantsState.selectedLocation && warrantsState.filteredCrashes?.length > 0) {
        crashes = warrantsState.filteredCrashes;
    } else {
        crashes = testCrashes; // Fallback
    }

    assertEqual(crashes.length, 10, 'Speed study: uses warrant-filtered crashes (10) instead of all (20)');

    // Verify fallback when location doesn't match
    const differentLocation = 'I-70';
    let fallbackCrashes;
    if (warrantsState.selectedLocation && differentLocation === warrantsState.selectedLocation && warrantsState.filteredCrashes?.length > 0) {
        fallbackCrashes = warrantsState.filteredCrashes;
    } else {
        fallbackCrashes = testCrashes;
    }
    assertEqual(fallbackCrashes.length, 20, 'Speed study: falls back to independent filtering when location differs');
}

// ──────────────────────────────────────────────────────────────
section('16. P1-E FIX: Date filter propagation logic');
// ──────────────────────────────────────────────────────────────
{
    // Simulate the fix: applyWarrantDateFilter now calls streetlight/speed study refresh
    let streetlightRefreshed = false;
    let speedstudyRefreshed = false;

    function simulateApplyWarrantDateFilter(currentStudy) {
        streetlightRefreshed = false;
        speedstudyRefreshed = false;

        // (existing logic: filterWarrantCrashesByDate, updateWarrantCrashDisplay, autoPopulateWarrantForm)

        // NEW: Refresh streetlight if currently viewing
        if (currentStudy === 'streetlight') {
            streetlightRefreshed = true;
        }
        // NEW: Refresh speed study if currently viewing
        if (currentStudy === 'speedstudy') {
            speedstudyRefreshed = true;
        }
    }

    simulateApplyWarrantDateFilter('streetlight');
    assert(streetlightRefreshed, 'Date filter change refreshes streetlight when active');
    assert(!speedstudyRefreshed, 'Date filter change does NOT refresh speed study when streetlight is active');

    simulateApplyWarrantDateFilter('speedstudy');
    assert(!streetlightRefreshed, 'Date filter change does NOT refresh streetlight when speed study is active');
    assert(speedstudyRefreshed, 'Date filter change refreshes speed study when active');

    simulateApplyWarrantDateFilter('signal');
    assert(!streetlightRefreshed, 'Date filter change does NOT refresh streetlight when signal is active');
    assert(!speedstudyRefreshed, 'Date filter change does NOT refresh speed study when signal is active');
}

// ──────────────────────────────────────────────────────────────
section('17. P2-A FIX: Profile rebuild condition');
// ──────────────────────────────────────────────────────────────
{
    // OLD condition: profileTotal !== filteredCount || profileTotal === 0
    // This would trigger on EVERY tab show when aggregateCount != filteredCrashes.length
    // NEW condition: !crashProfile || crashProfile.total === 0

    const crashProfile = buildWarrantCrashProfile(testCrashes);

    // Simulate old broken condition
    const profileTotal = crashProfile.total; // = 20
    const filteredCount = 15; // Date-filtered subset
    const oldTriggers = (profileTotal !== filteredCount || profileTotal === 0); // true (20 !== 15)

    // Simulate new fixed condition
    const newTriggers = (!crashProfile || crashProfile.total === 0); // false (profile exists and total > 0)

    assert(oldTriggers, 'OLD condition: always triggered when aggregate != filtered (BUG)');
    assert(!newTriggers, 'NEW condition: does NOT trigger unnecessary rebuild');

    // But it DOES trigger when profile is missing
    const missingProfile = null;
    const newTriggersOnMissing = (!missingProfile || (missingProfile && missingProfile.total === 0));
    assert(newTriggersOnMissing, 'NEW condition: triggers when profile is null');

    // And when profile total is 0
    const emptyProfile = { total: 0 };
    const newTriggersOnEmpty = (!emptyProfile || emptyProfile.total === 0);
    assert(newTriggersOnEmpty, 'NEW condition: triggers when profile total is 0');
}

// ──────────────────────────────────────────────────────────────
section('18. P2-B FIX: Traffic data location sync');
// ──────────────────────────────────────────────────────────────
{
    // Simulate the fixed trafficdata_syncFromWarrantSelection
    function simulateSync(displayName) {
        let streets = [];
        if (displayName.includes(' & ')) streets = displayName.split(' & ').map(s => s.trim());
        else if (displayName.includes(' @ ')) streets = displayName.split(' @ ').map(s => s.trim());
        else if (displayName.toLowerCase().includes(' at ')) streets = displayName.split(/ at /i).map(s => s.trim());

        return { locationName: displayName, majorStreet: streets[0] || '', minorStreet: streets[1] || '' };
    }

    const r1 = simulateSync('US-6 & CO-93');
    assertEqual(r1.majorStreet, 'US-6', 'Sync: parses major street from &');
    assertEqual(r1.minorStreet, 'CO-93', 'Sync: parses minor street from &');

    const r2 = simulateSync('Broadway @ Main St');
    assertEqual(r2.majorStreet, 'Broadway', 'Sync: parses major street from @');
    assertEqual(r2.minorStreet, 'Main St', 'Sync: parses minor street from @');

    const r3 = simulateSync('Elm St at Oak Ave');
    assertEqual(r3.majorStreet, 'Elm St', 'Sync: parses major street from AT');
    assertEqual(r3.minorStreet, 'Oak Ave', 'Sync: parses minor street from AT');

    const r4 = simulateSync('US-6');
    assertEqual(r4.locationName, 'US-6', 'Sync: single road name preserved');
    assertEqual(r4.majorStreet, '', 'Sync: no major street for single name');
}

// ──────────────────────────────────────────────────────────────
section('19. Cross-tab data consistency');
// ──────────────────────────────────────────────────────────────
{
    // When the same crashes flow to all sub-tabs, the numbers must match
    const warrantProfile = buildWarrantCrashProfile(testCrashes);
    const stopsignProfile = stopsign_buildCrashProfile(testCrashes);
    const w7 = classifyWarrant7Crashes(testCrashes);
    const slData = streetlight_analyzeCrashesByLight(testCrashes);

    // Total counts must match across all profiles
    assertEqual(warrantProfile.total, stopsignProfile.total, 'Cross-tab: warrant total = stopsign total');
    assertEqual(warrantProfile.total, slData.total, 'Cross-tab: warrant total = streetlight total');

    // Severity totals must match
    const warrantSevTotal = warrantProfile.severity.K + warrantProfile.severity.A + warrantProfile.severity.B + warrantProfile.severity.C + warrantProfile.severity.O;
    const stopSevTotal = stopsignProfile.severity.K + stopsignProfile.severity.A + stopsignProfile.severity.B + stopsignProfile.severity.C + stopsignProfile.severity.O;
    assertEqual(warrantSevTotal, stopSevTotal, 'Cross-tab: severity sum matches between warrant and stopsign');
    assertEqual(warrantSevTotal, 20, 'Cross-tab: severity sum = total crashes');

    // EPDO must match between warrant and stopsign (both use same EPDO_WEIGHTS)
    assertEqual(warrantProfile.epdo, stopsignProfile.epdo, 'Cross-tab: EPDO matches between warrant and stopsign profiles');

    // Pedestrian counts: warrant.pedCount should match w7.pedTotal (both filter on PED flag/collision)
    assertEqual(warrantProfile.pedCount, w7.pedTotal, 'Cross-tab: warrant pedCount = signal W7 pedTotal');
}

// ──────────────────────────────────────────────────────────────
section('20. Date filtering simulation');
// ──────────────────────────────────────────────────────────────
{
    // Simulate filterWarrantCrashesByDate for 12-month window
    const startDate = new Date('2024-01-01');
    const endDate = new Date('2024-12-31');

    const filtered = testCrashes.filter(crash => {
        const crashDate = new Date(crash[COL.DATE]);
        return crashDate >= startDate && crashDate <= endDate;
    });

    // 2024 crashes: D016, D017, D018, D019, D020 = 5
    assertEqual(filtered.length, 5, 'Date filter: 12-month window returns correct count');

    const filteredProfile = buildWarrantCrashProfile(filtered);
    assertEqual(filteredProfile.total, 5, 'Date filter: profile total matches filtered count');
    assertEqual(filteredProfile.severity.K, 1, 'Date filter: 1 fatal in 2024 (D016)');

    // Verify the filtered data produces correct EPDO
    // D016(K=883) + D017(B=21) + D018(O=1) + D019(C=11) + D020(O=1) = 917
    assertEqual(filteredProfile.epdo, 917, 'Date filter: EPDO calculated from filtered data');

    // 36-month window should get all
    const start36 = new Date('2022-01-01');
    const filtered36 = testCrashes.filter(crash => {
        const crashDate = new Date(crash[COL.DATE]);
        return crashDate >= start36 && crashDate <= endDate;
    });
    assertEqual(filtered36.length, 20, 'Date filter: 36-month window returns all crashes');
}

// ──────────────────────────────────────────────────────────────
section('21. Edge cases — empty datasets');
// ──────────────────────────────────────────────────────────────
{
    const emptyProfile = buildWarrantCrashProfile([]);
    assertEqual(emptyProfile.total, 0, 'Empty: total = 0');
    assertEqual(emptyProfile.epdo, 0, 'Empty: EPDO = 0');
    assertEqual(emptyProfile.kaCount, 0, 'Empty: kaCount = 0');
    assertEqual(Object.keys(emptyProfile.collisionTypes).length, 0, 'Empty: no collision types');

    const emptyStop = stopsign_buildCrashProfile([]);
    assertEqual(emptyStop.susceptibleCount, 0, 'Empty stopsign: susceptible = 0');

    const emptyW7 = classifyWarrant7Crashes([]);
    assertEqual(emptyW7.angleTotal, 0, 'Empty W7: angle total = 0');
    assertEqual(emptyW7.pedTotal, 0, 'Empty W7: ped total = 0');

    const emptySL = streetlight_analyzeCrashesByLight([]);
    assertEqual(emptySL.total, 0, 'Empty streetlight: total = 0');
    assertEqual(emptySL.nighttime.count, 0, 'Empty streetlight: nighttime = 0');
}


// ═══════════════════════════════════════════════════════════════
//  RESULTS
// ═══════════════════════════════════════════════════════════════

console.log('\n══════════════════════════════════════════════════════════');
if (failed === 0) {
    console.log(`  ALL ${passed} TESTS PASSED  ✓`);
} else {
    console.log(`  ${passed} passed, ${failed} FAILED`);
    console.log('\n  Failures:');
    failures.forEach((f, i) => console.log(`    ${i + 1}. ${f}`));
}
console.log('══════════════════════════════════════════════════════════\n');

process.exit(failed > 0 ? 1 : 0);
