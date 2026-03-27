/**
 * Test suite for StateAdapter - runs against real Douglas County CDOT data.
 * Usage: node states/test_adapter.js
 */

const fs = require('fs');
const path = require('path');

// Load the adapter
const StateAdapter = require('./state_adapter.js');

// ─── CSV Parser (minimal, handles quoted fields) ───
function parseCSV(text) {
    const lines = text.split('\n');
    if (lines.length < 2) return [];

    // Parse header (handle BOM)
    const headerLine = lines[0].replace(/^\uFEFF/, '');
    const headers = parseCSVLine(headerLine);

    const rows = [];
    for (let i = 1; i < lines.length; i++) {
        const line = lines[i].trim();
        if (!line) continue;
        const values = parseCSVLine(line);
        const row = {};
        for (let j = 0; j < headers.length; j++) {
            row[headers[j]] = values[j] || '';
        }
        rows.push(row);
    }
    return { headers, rows };
}

function parseCSVLine(line) {
    const result = [];
    let current = '';
    let inQuotes = false;
    for (let i = 0; i < line.length; i++) {
        const ch = line[i];
        if (ch === '"') {
            if (inQuotes && line[i + 1] === '"') {
                current += '"';
                i++;
            } else {
                inQuotes = !inQuotes;
            }
        } else if (ch === ',' && !inQuotes) {
            result.push(current);
            current = '';
        } else {
            current += ch;
        }
    }
    result.push(current);
    return result;
}

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
        const msg = `${message} — expected "${expected}", got "${actual}"`;
        failures.push(msg);
        console.log(`  FAIL: ${msg}`);
    }
}

function assertIn(value, validSet, message) {
    if (validSet.includes(value)) {
        passed++;
    } else {
        failed++;
        const msg = `${message} — "${value}" not in [${validSet.join(', ')}]`;
        failures.push(msg);
        console.log(`  FAIL: ${msg}`);
    }
}

// ─── Load Data ───
console.log('=== StateAdapter Test Suite ===\n');

const csvPath = path.join(__dirname, '..', 'data', 'CDOT', 'Douglas_County.csv');
if (!fs.existsSync(csvPath)) {
    console.error(`ERROR: Data file not found: ${csvPath}`);
    process.exit(1);
}

const csvText = fs.readFileSync(csvPath, 'utf-8');
const { headers, rows } = parseCSV(csvText);
console.log(`Loaded ${rows.length} rows from Douglas_County.csv`);
console.log(`Columns: ${headers.length}\n`);

// ═══════════════════════════════════════════════
// TEST 1: State Detection
// ═══════════════════════════════════════════════
console.log('--- Test 1: State Detection ---');

const detectedState = StateAdapter.detect(headers);
assertEqual(detectedState, 'colorado', 'Should detect Colorado from CDOT headers');
assertEqual(StateAdapter.getDetectedState(), 'colorado', 'getDetectedState() should return colorado');
assertEqual(StateAdapter.needsNormalization(), true, 'needsNormalization() should be true for Colorado');
assert(StateAdapter.getStateName().includes('Colorado'), 'getStateName() should include Colorado');

// Test Virginia detection
const vaHeaders = ['Document Nbr', 'Crash Severity', 'RTE Name', 'SYSTEM', 'K_People'];
StateAdapter.detect(vaHeaders);
assertEqual(StateAdapter.getDetectedState(), 'virginia', 'Should detect Virginia from TREDS headers');
assertEqual(StateAdapter.needsNormalization(), false, 'needsNormalization() should be false for Virginia');

// Reset to Colorado for remaining tests
StateAdapter.detect(headers);

console.log('');

// ═══════════════════════════════════════════════
// TEST 2: Row Normalization - Basic Fields
// ═══════════════════════════════════════════════
console.log('--- Test 2: Basic Field Normalization ---');

const testRow1 = rows[0]; // First data row
const norm1 = StateAdapter.normalizeRow(testRow1);

// Must have these critical fields
assert(norm1['Document Nbr'] !== undefined && norm1['Document Nbr'] !== '', 'Document Nbr should exist and be non-empty');
assert(norm1['Crash Date'] !== undefined, 'Crash Date should exist');
assert(norm1['Crash Year'] !== undefined && norm1['Crash Year'] !== '', 'Crash Year should be derived');
assert(norm1['Crash Military Time'] !== undefined, 'Crash Military Time should exist');
assert(norm1['x'] !== undefined, 'x (longitude) should exist');
assert(norm1['y'] !== undefined, 'y (latitude) should exist');
assert(norm1['Physical Juris Name'] !== undefined, 'Physical Juris Name (jurisdiction) should exist');

console.log(`  Sample: ID=${norm1['Document Nbr']}, Year=${norm1['Crash Year']}, Sev=${norm1['Crash Severity']}, Route=${norm1['RTE Name']}`);
console.log('');

// ═══════════════════════════════════════════════
// TEST 3: Severity Derivation
// ═══════════════════════════════════════════════
console.log('--- Test 3: Severity Derivation ---');

const severityCounts = { K: 0, A: 0, B: 0, C: 0, O: 0, invalid: 0 };
const validSeverities = ['K', 'A', 'B', 'C', 'O'];

for (const row of rows) {
    const norm = StateAdapter.normalizeRow(row);
    const sev = norm['Crash Severity'];
    if (validSeverities.includes(sev)) {
        severityCounts[sev]++;
    } else {
        severityCounts.invalid++;
    }
}

assert(severityCounts.invalid === 0, `All rows should have valid severity (got ${severityCounts.invalid} invalid)`);
assert(severityCounts.K >= 0, 'Should have K (fatal) count >= 0');
assert(severityCounts.O > 0, 'Should have some O (property damage only) crashes');
assert(severityCounts.K + severityCounts.A + severityCounts.B + severityCounts.C + severityCounts.O === rows.length,
    'Severity counts should sum to total rows');

console.log(`  Severity distribution: K=${severityCounts.K}, A=${severityCounts.A}, B=${severityCounts.B}, C=${severityCounts.C}, O=${severityCounts.O}`);

// Verify severity derivation logic with manual check
// Find a row with Injury 04 > 0 (should be K)
const fatalRow = rows.find(r => parseInt(r['Injury 04']) > 0);
if (fatalRow) {
    const normFatal = StateAdapter.normalizeRow(fatalRow);
    assertEqual(normFatal['Crash Severity'], 'K', 'Row with Injury 04 > 0 should be severity K');
    console.log(`  Verified: CUID ${fatalRow['CUID']} with ${fatalRow['Injury 04']} killed → Severity K`);
}

// Find a row with Injury 03 > 0 but Injury 04 = 0 (should be A)
const seriousRow = rows.find(r => parseInt(r['Injury 03']) > 0 && parseInt(r['Injury 04'] || '0') === 0);
if (seriousRow) {
    const normSerious = StateAdapter.normalizeRow(seriousRow);
    assertEqual(normSerious['Crash Severity'], 'A', 'Row with Injury 03 > 0 and Injury 04 = 0 should be severity A');
}

// Find a PDO row (all injury counts = 0)
const pdoRow = rows.find(r =>
    parseInt(r['Injury 04'] || '0') === 0 &&
    parseInt(r['Injury 03'] || '0') === 0 &&
    parseInt(r['Injury 02'] || '0') === 0 &&
    parseInt(r['Injury 01'] || '0') === 0
);
if (pdoRow) {
    const normPdo = StateAdapter.normalizeRow(pdoRow);
    assertEqual(normPdo['Crash Severity'], 'O', 'Row with all injuries = 0 should be severity O');
}

// Verify K_People, A_People, etc. are numeric
const normCheck = StateAdapter.normalizeRow(rows[0]);
assert(typeof normCheck['K_People'] === 'number', 'K_People should be numeric');
assert(typeof normCheck['A_People'] === 'number', 'A_People should be numeric');
assert(typeof normCheck['B_People'] === 'number', 'B_People should be numeric');
assert(typeof normCheck['C_People'] === 'number', 'C_People should be numeric');

console.log('');

// ═══════════════════════════════════════════════
// TEST 4: Road System Mapping
// ═══════════════════════════════════════════════
console.log('--- Test 4: Road System Mapping ---');

const systemCounts = {};
const validSystems = ['Non-DOT secondary', 'Primary', 'Secondary', 'Interstate'];

for (const row of rows) {
    const norm = StateAdapter.normalizeRow(row);
    const sys = norm['SYSTEM'];
    systemCounts[sys] = (systemCounts[sys] || 0) + 1;
}

console.log('  Mapped road systems:');
for (const [sys, count] of Object.entries(systemCounts)) {
    console.log(`    ${sys}: ${count}`);
    assertIn(sys, validSystems, `Road system "${sys}" should be a valid mapped value`);
}

// Verify specific mappings
const cityStreetRow = rows.find(r => r['System Code'] === 'City Street');
if (cityStreetRow) {
    assertEqual(StateAdapter.normalizeRow(cityStreetRow)['SYSTEM'], 'Non-DOT secondary',
        'City Street should map to Non-DOT secondary');
}
const countyRoadRow = rows.find(r => r['System Code'] === 'County Road');
if (countyRoadRow) {
    assertEqual(StateAdapter.normalizeRow(countyRoadRow)['SYSTEM'], 'Non-DOT secondary',
        'County Road should map to Non-DOT secondary');
}
const stateHwyRow = rows.find(r => r['System Code'] === 'State Highway');
if (stateHwyRow) {
    assertEqual(StateAdapter.normalizeRow(stateHwyRow)['SYSTEM'], 'Primary',
        'State Highway should map to Primary');
}
const interstateRow = rows.find(r => r['System Code'] === 'Interstate Highway');
if (interstateRow) {
    assertEqual(StateAdapter.normalizeRow(interstateRow)['SYSTEM'], 'Interstate',
        'Interstate Highway should map to Interstate');
}

console.log('');

// ═══════════════════════════════════════════════
// TEST 5: Route Name Building
// ═══════════════════════════════════════════════
console.log('--- Test 5: Route Name Building ---');

// Interstate should be I-XX format
if (interstateRow) {
    const normInt = StateAdapter.normalizeRow(interstateRow);
    assert(normInt['RTE Name'].startsWith('I-'), `Interstate route should start with I-, got "${normInt['RTE Name']}"`);
    console.log(`  Interstate: ${interstateRow['Rd_Number']} → ${normInt['RTE Name']}`);
}

// State highway should have a name
if (stateHwyRow) {
    const normSH = StateAdapter.normalizeRow(stateHwyRow);
    assert(normSH['RTE Name'] && normSH['RTE Name'] !== '', `State highway should have a route name, got "${normSH['RTE Name']}"`);
    console.log(`  State Hwy: ${stateHwyRow['Rd_Number']} / ${stateHwyRow['Location 1']} → ${normSH['RTE Name']}`);
}

// City street should use Location 1
if (cityStreetRow) {
    const normCS = StateAdapter.normalizeRow(cityStreetRow);
    assert(normCS['RTE Name'] && normCS['RTE Name'] !== '', `City street should have a route name, got "${normCS['RTE Name']}"`);
    console.log(`  City Street: ${cityStreetRow['Location 1']} → ${normCS['RTE Name']}`);
}

// No route should be empty or undefined
let emptyRoutes = 0;
for (const row of rows) {
    const norm = StateAdapter.normalizeRow(row);
    if (!norm['RTE Name'] || norm['RTE Name'] === '' || norm['RTE Name'] === 'undefined') {
        emptyRoutes++;
    }
}
assert(emptyRoutes === 0, `No rows should have empty route names (found ${emptyRoutes})`);

console.log('');

// ═══════════════════════════════════════════════
// TEST 6: Node/Intersection Building
// ═══════════════════════════════════════════════
console.log('--- Test 6: Node/Intersection Building ---');

let intersectionWithNode = 0;
let intersectionWithoutNode = 0;
let nonIntersectionWithNode = 0;

for (const row of rows) {
    const norm = StateAdapter.normalizeRow(row);
    const rd = (row['Road Description'] || '').trim();
    const node = norm['Node'];

    if (rd === 'At Intersection' || rd === 'Intersection Related' || rd === 'Roundabout') {
        if (node && node !== '') intersectionWithNode++;
        else intersectionWithoutNode++;
    } else {
        if (node && node !== '') nonIntersectionWithNode++;
    }
}

console.log(`  Intersections with node ID: ${intersectionWithNode}`);
console.log(`  Intersections missing node: ${intersectionWithoutNode}`);
console.log(`  Non-intersection with node (should be 0): ${nonIntersectionWithNode}`);

assertEqual(nonIntersectionWithNode, 0, 'Non-intersection crashes should not have node IDs');
assert(intersectionWithNode > 0, 'Should have some intersection crashes with node IDs');

// Verify node format
const intRow = rows.find(r => r['Road Description'] === 'At Intersection' && r['Location 1'] && r['Location 2']);
if (intRow) {
    const normInt2 = StateAdapter.normalizeRow(intRow);
    assert(normInt2['Node'].includes(' & '), `Node should be "Road1 & Road2" format, got "${normInt2['Node']}"`);
    console.log(`  Sample node: "${normInt2['Node']}"`);
}

console.log('');

// ═══════════════════════════════════════════════
// TEST 7: Boolean Flag Derivation
// ═══════════════════════════════════════════════
console.log('--- Test 7: Boolean Flag Derivation ---');

const flagCounts = {
    'Pedestrian?': { Y: 0, N: 0, other: 0 },
    'Bike?': { Y: 0, N: 0, other: 0 },
    'Alcohol?': { Y: 0, N: 0, other: 0 },
    'Speed?': { Y: 0, N: 0, other: 0 },
    'Hitrun?': { Y: 0, N: 0, other: 0 },
    'Motorcycle?': { Y: 0, N: 0, other: 0 },
    'Night?': { Y: 0, N: 0, other: 0 },
    'Distracted?': { Y: 0, N: 0, other: 0 },
    'Drowsy?': { Y: 0, N: 0, other: 0 },
    'Drug Related?': { Y: 0, N: 0, other: 0 },
    'Young?': { Y: 0, N: 0, other: 0 },
    'Senior?': { Y: 0, N: 0, other: 0 },
    'Unrestrained?': { Y: 0, N: 0, other: 0 },
    'School Zone': { Y: 0, N: 0, other: 0 },
    'Work Zone Related': { Y: 0, N: 0, other: 0 }
};

for (const row of rows) {
    const norm = StateAdapter.normalizeRow(row);
    for (const flag of Object.keys(flagCounts)) {
        const val = norm[flag];
        if (val === 'Y') flagCounts[flag].Y++;
        else if (val === 'N') flagCounts[flag].N++;
        else flagCounts[flag].other++;
    }
}

console.log('  Flag      | Y    | N    | Invalid');
console.log('  ----------|------|------|--------');
for (const [flag, counts] of Object.entries(flagCounts)) {
    const pad = (s, n) => String(s).padStart(n);
    console.log(`  ${flag.padEnd(18)}| ${pad(counts.Y, 4)} | ${pad(counts.N, 4)} | ${pad(counts.other, 4)}`);
    assertEqual(counts.other, 0, `${flag} should only have Y/N values (got ${counts.other} invalid)`);
    assertEqual(counts.Y + counts.N, rows.length, `${flag} Y+N should equal total rows`);
}

// Sanity checks - known data patterns
assert(flagCounts['Night?'].Y > 0, 'Should have some nighttime crashes');
assert(flagCounts['Pedestrian?'].Y > 0 || flagCounts['Pedestrian?'].N === rows.length,
    'Pedestrian flag should be valid');

// Verify pedestrian detection against NM Type
const pedRow = rows.find(r =>
    (r['TU-1 NM Type'] || '').includes('Pedestrian') ||
    (r['TU-2 NM Type'] || '').includes('Pedestrian')
);
if (pedRow) {
    const normPed = StateAdapter.normalizeRow(pedRow);
    assertEqual(normPed['Pedestrian?'], 'Y', 'Row with Pedestrian NM Type should have Pedestrian?=Y');
    console.log(`  Verified: CUID ${pedRow['CUID']} with NM Type containing Pedestrian → Pedestrian?=Y`);
}

// Verify bike detection
const bikeRow = rows.find(r =>
    (r['TU-1 NM Type'] || '').includes('Bicycle') ||
    (r['TU-2 NM Type'] || '').includes('Bicycle')
);
if (bikeRow) {
    const normBike = StateAdapter.normalizeRow(bikeRow);
    assertEqual(normBike['Bike?'], 'Y', 'Row with Bicycle NM Type should have Bike?=Y');
}

console.log('');

// ═══════════════════════════════════════════════
// TEST 8: Collision Type Mapping
// ═══════════════════════════════════════════════
console.log('--- Test 8: Collision Type Mapping ---');

const collisionCounts = {};
let unmappedCollisions = 0;
const unmappedValues = new Set();

for (const row of rows) {
    const norm = StateAdapter.normalizeRow(row);
    const ct = norm['Collision Type'];
    collisionCounts[ct] = (collisionCounts[ct] || 0) + 1;
    if (ct === '' || ct === 'undefined' || ct === undefined) {
        unmappedCollisions++;
    }
}

console.log('  Mapped collision types:');
for (const [ct, count] of Object.entries(collisionCounts).sort((a, b) => b[1] - a[1])) {
    console.log(`    ${ct}: ${count}`);
}

assert(unmappedCollisions === 0, `No rows should have empty collision type (found ${unmappedCollisions})`);
assert(Object.keys(collisionCounts).length > 1, 'Should have multiple collision type categories');

console.log('');

// ═══════════════════════════════════════════════
// TEST 9: Coordinates
// ═══════════════════════════════════════════════
console.log('--- Test 9: Coordinates ---');

let validCoords = 0;
let invalidCoords = 0;
let outOfBounds = 0;

for (const row of rows) {
    const norm = StateAdapter.normalizeRow(row);
    const x = parseFloat(norm['x']);
    const y = parseFloat(norm['y']);

    if (isNaN(x) || isNaN(y) || x === 0 || y === 0) {
        invalidCoords++;
    } else {
        validCoords++;
        // Colorado bounds check
        if (y < 36.99 || y > 41.01 || x < -109.06 || x > -102.04) {
            outOfBounds++;
        }
    }
}

console.log(`  Valid coordinates: ${validCoords}`);
console.log(`  Invalid/zero: ${invalidCoords}`);
console.log(`  Out of Colorado bounds: ${outOfBounds}`);

assert(validCoords > rows.length * 0.9, 'At least 90% of rows should have valid coordinates');
assert(outOfBounds < rows.length * 0.01, 'Less than 1% should be out of Colorado bounds');

console.log('');

// ═══════════════════════════════════════════════
// TEST 10: Date/Time Parsing
// ═══════════════════════════════════════════════
console.log('--- Test 10: Date/Time Parsing ---');

let validYear = 0;
let invalidYear = 0;
const yearCounts = {};

for (const row of rows) {
    const norm = StateAdapter.normalizeRow(row);
    const year = norm['Crash Year'];
    if (year && !isNaN(parseInt(year)) && parseInt(year) >= 2019 && parseInt(year) <= 2026) {
        validYear++;
        yearCounts[year] = (yearCounts[year] || 0) + 1;
    } else {
        invalidYear++;
    }
}

console.log(`  Valid years: ${validYear}, Invalid: ${invalidYear}`);
for (const [year, count] of Object.entries(yearCounts).sort()) {
    console.log(`    ${year}: ${count} crashes`);
}

assertEqual(invalidYear, 0, 'All rows should have valid year derived from date');

// Check military time format (should be HHMM, no colons)
const normTime = StateAdapter.normalizeRow(rows[0]);
const time = normTime['Crash Military Time'];
assert(!time.includes(':'), `Military time should not contain colons, got "${time}"`);
assert(time.length <= 4, `Military time should be <= 4 chars, got "${time}" (length ${time.length})`);

console.log('');

// ═══════════════════════════════════════════════
// TEST 11: Virginia Pass-Through
// ═══════════════════════════════════════════════
console.log('--- Test 11: Virginia Pass-Through ---');

StateAdapter.detect(['Document Nbr', 'Crash Severity', 'RTE Name', 'SYSTEM']);
const vaRow = { 'Document Nbr': '12345', 'Crash Severity': 'A', 'RTE Name': 'I-64' };
const normVA = StateAdapter.normalizeRow(vaRow);
assertEqual(normVA['Document Nbr'], '12345', 'Virginia row should pass through unchanged');
assertEqual(normVA['Crash Severity'], 'A', 'Virginia severity should pass through');
assertEqual(normVA['RTE Name'], 'I-64', 'Virginia route should pass through');

// Reset to Colorado
StateAdapter.detect(headers);

console.log('');

// ═══════════════════════════════════════════════
// TEST 12: Edge Cases
// ═══════════════════════════════════════════════
console.log('--- Test 12: Edge Cases ---');

// Empty row
const emptyRow = {};
headers.forEach(h => emptyRow[h] = '');
try {
    const normEmpty = StateAdapter.normalizeRow(emptyRow);
    assert(normEmpty['Crash Severity'] === 'O', 'Empty row should default to severity O');
    assert(normEmpty['Document Nbr'] === '', 'Empty row should have empty ID');
    passed++;
    console.log('  Empty row: handled correctly');
} catch (e) {
    failed++;
    failures.push(`Empty row threw error: ${e.message}`);
    console.log(`  FAIL: Empty row threw error: ${e.message}`);
}

// Row with "Non Crash" system code
const nonCrashRow = { ...emptyRow, 'System Code': 'Non Crash', 'CUID': 'TEST1' };
try {
    const normNC = StateAdapter.normalizeRow(nonCrashRow);
    assert(normNC['SYSTEM'] !== undefined, 'Non Crash system code should not crash');
    console.log(`  Non Crash system code: mapped to "${normNC['SYSTEM']}"`);
} catch (e) {
    failed++;
    failures.push(`Non Crash row threw error: ${e.message}`);
}

// Row with special characters in location
const specialRow = { ...emptyRow, 'Location 1': "O'BRIEN AVE", 'Location 2': 'KING\'S CT', 'Road Description': 'At Intersection' };
try {
    const normSpecial = StateAdapter.normalizeRow(specialRow);
    assert(normSpecial['Node'].includes('&'), 'Special char locations should still build node');
    console.log(`  Special chars in location: "${normSpecial['Node']}"`);
} catch (e) {
    failed++;
    failures.push(`Special char row threw error: ${e.message}`);
}

console.log('');

// ═══════════════════════════════════════════════
// TEST 13: Data Consistency - Totals Match
// ═══════════════════════════════════════════════
console.log('--- Test 13: Data Consistency ---');

// Total killed/injured should be consistent
let totalKilledFromSeverity = 0;
let totalKilledFromColumn = 0;

for (const row of rows) {
    const norm = StateAdapter.normalizeRow(row);
    totalKilledFromSeverity += norm['K_People'];
    totalKilledFromColumn += parseInt(row['Number Killed']) || 0;
}

assertEqual(totalKilledFromSeverity, totalKilledFromColumn,
    `K_People total (${totalKilledFromSeverity}) should match Number Killed total (${totalKilledFromColumn})`);
console.log(`  Total killed: adapter=${totalKilledFromSeverity}, original=${totalKilledFromColumn}`);

// Verify row count preservation
let normalizedCount = 0;
for (const row of rows) {
    StateAdapter.normalizeRow(row);
    normalizedCount++;
}
assertEqual(normalizedCount, rows.length, 'All rows should normalize without errors');

console.log('');

// ═══════════════════════════════════════════════
// TEST 14: Filter Profile Compatibility
// ═══════════════════════════════════════════════
console.log('--- Test 14: Filter Profile Compatibility ---');

// Simulate the filter logic that exists in the app
function filterByProfile(normalizedRows, profile) {
    const profiles = {
        countyOnly: ['Non-DOT secondary'],
        countyPlusVDOT: ['Non-DOT secondary', 'Primary', 'Secondary'],
        allRoads: ['Non-DOT secondary', 'Primary', 'Secondary', 'Interstate']
    };
    const allowedSystems = profiles[profile];
    return normalizedRows.filter(r => allowedSystems.includes(r['SYSTEM']));
}

const allNormalized = rows.map(r => StateAdapter.normalizeRow(r));

const countyOnly = filterByProfile(allNormalized, 'countyOnly');
const noInterstate = filterByProfile(allNormalized, 'countyPlusVDOT');
const allRoads = filterByProfile(allNormalized, 'allRoads');

console.log(`  countyOnly: ${countyOnly.length} crashes`);
console.log(`  noInterstate: ${noInterstate.length} crashes`);
console.log(`  allRoads: ${allRoads.length} crashes`);

assert(countyOnly.length > 0, 'countyOnly should have crashes');
assert(noInterstate.length >= countyOnly.length, 'noInterstate should be >= countyOnly');
assert(allRoads.length >= noInterstate.length, 'allRoads should be >= noInterstate');
assertEqual(allRoads.length, rows.length, 'allRoads should include all rows');

console.log('');

// ═══════════════════════════════════════════════
// TEST 15: Performance
// ═══════════════════════════════════════════════
console.log('--- Test 15: Performance ---');

const startTime = Date.now();
for (const row of rows) {
    StateAdapter.normalizeRow(row);
}
const elapsed = Date.now() - startTime;
const rowsPerSec = Math.round(rows.length / (elapsed / 1000));

console.log(`  Normalized ${rows.length} rows in ${elapsed}ms (${rowsPerSec} rows/sec)`);
assert(elapsed < 5000, `Should normalize ${rows.length} rows in under 5 seconds (took ${elapsed}ms)`);
assert(rowsPerSec > 1000, `Should process at least 1000 rows/sec (got ${rowsPerSec})`);

console.log('');

// ═══════════════════════════════════════════════
// SUMMARY
// ═══════════════════════════════════════════════
console.log('═══════════════════════════════════════');
console.log(`RESULTS: ${passed} passed, ${failed} failed`);
console.log('═══════════════════════════════════════');

if (failures.length > 0) {
    console.log('\nFAILURES:');
    failures.forEach((f, i) => console.log(`  ${i + 1}. ${f}`));
}

process.exit(failed > 0 ? 1 : 0);
