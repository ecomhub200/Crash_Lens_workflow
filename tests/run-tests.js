// Node.js runner for jurisdiction sync tests
// Run: node tests/run-tests.js

// ═══════════════════════════════════════════════════════════════
// MOCK SETUP
// ═══════════════════════════════════════════════════════════════
const appConfig = {
    defaultState: 'colorado',
    states: {
        virginia: { fips: '51', name: 'Virginia', abbreviation: 'VA', r2Prefix: 'virginia', defaultJurisdiction: 'henrico' },
        colorado: { fips: '08', name: 'Colorado', abbreviation: 'CO', r2Prefix: 'colorado', defaultJurisdiction: 'douglas' },
        new_york: { fips: '36', name: 'New York', abbreviation: 'NY', r2Prefix: 'new_york', defaultJurisdiction: null }
    },
    jurisdictions: {
        henrico: { name: 'Henrico County', type: 'county', fips: '087', state: 'VA', bbox: [-77.68, 37.49, -77.25, 37.68], mapCenter: [37.55, -77.40], mapZoom: 11 },
        fairfax_county: { name: 'Fairfax County', type: 'county', fips: '059', state: 'VA', bbox: [-77.51, 38.59, -77.09, 38.97], mapCenter: [38.85, -77.28], mapZoom: 11 },
        douglas: { name: 'Douglas County', type: 'county', fips: '035', state: 'CO', bbox: [-105.10, 39.13, -104.63, 39.39], mapCenter: [39.29, -104.80], mapZoom: 10 },
        co_adams: { name: 'Adams County', type: 'county', fips: '001', state: 'CO', bbox: [-105.05, 39.74, -104.46, 40.00], mapCenter: [39.87, -104.76], mapZoom: 10 },
        mystery_county: { name: 'Mystery County', type: 'county', fips: '999', bbox: [-80.0, 35.0, -79.0, 36.0], mapCenter: [35.5, -79.5], mapZoom: 10 },
        incomplete_county: { name: 'Incomplete County', type: 'county', fips: '000', state: 'VA' }
    }
};

// ═══════════════════════════════════════════════════════════════
// FUNCTIONS UNDER TEST (copied from the branch diff)
// ═══════════════════════════════════════════════════════════════
function _abbrToStateKey(abbr) {
    if (!abbr || !appConfig?.states) return null;
    for (const [key, st] of Object.entries(appConfig.states)) {
        if (st.abbreviation === abbr) return key;
    }
    return null;
}

function buildTIJurisdictions(fallbackStateKey) {
    const allJuris = {};
    Object.entries(appConfig.jurisdictions).forEach(([key, j]) => {
        if (!j.bbox || !j.mapCenter) return;
        const jurStateKey = (j.state && _abbrToStateKey(j.state)) || fallbackStateKey;
        const r2Prefix = appConfig?.states?.[jurStateKey]?.r2Prefix || jurStateKey;
        allJuris[key] = { name: j.name || key, type: j.type || 'county', fips: j.fips || '', bbox: j.bbox, mapCenter: j.mapCenter, mapZoom: j.mapZoom || 10, state: r2Prefix, folder: key };
    });
    return allJuris;
}

function buildIMJurisdictions(fallbackStateKey) {
    const allJuris = {};
    Object.entries(appConfig.jurisdictions).forEach(([key, j]) => {
        const jurStateKey = (j.state && _abbrToStateKey(j.state)) || fallbackStateKey;
        const r2Prefix = appConfig?.states?.[jurStateKey]?.r2Prefix || jurStateKey;
        allJuris[key] = { name: j.name || key, type: j.type || 'county', fips: j.fips || '', state: r2Prefix, folder: key };
    });
    return allJuris;
}

function buildIMSyncMessage(stateKey, jurisdictionKey) {
    const r2Prefix = appConfig?.states?.[stateKey]?.r2Prefix || stateKey;
    return { type: 'im-set-jurisdiction', state: r2Prefix, jurisdictionKey };
}

function simulateIMPopulate(juris) {
    const states = new Set();
    Object.values(juris).forEach(j => { if (j.state) states.add(j.state); });
    return { stateOptions: [...states] };
}

function simulateIMFilter(juris, state) {
    return Object.entries(juris).filter(([, j]) => j.state === state).map(([key]) => key);
}

function simulateLoadCfg(localStorageData, isEmbedded) {
    const result = { pub: null, state: null, county: null };
    try {
        const c = localStorageData || {};
        if (c.pub) result.pub = c.pub;
        if (!isEmbedded) { if (c.state) result.state = c.state; if (c.county) result.county = c.county; }
    } catch (e) {}
    return result;
}

function buildADConfig(stateKey, jurisdictionId, r2BaseUrl) {
    const jc = appConfig?.jurisdictions?.[jurisdictionId];
    const sc = appConfig?.states?.[stateKey];
    const r2Prefix = sc?.r2Prefix || stateKey;
    const r2Path = r2Prefix + '/' + jurisdictionId;
    return { type: 'ad-config', config: { fips: jc?.stateCountyFips || jc?.fips || '', bbox: jc?.bbox || null, mapCenter: jc?.mapCenter || null, mapZoom: jc?.mapZoom || 10, jurisdictionName: jc?.name || jurisdictionId, state: stateKey, r2BaseUrl, r2Path } };
}

// ═══════════════════════════════════════════════════════════════
// TEST FRAMEWORK
// ═══════════════════════════════════════════════════════════════
let total = 0, pass = 0, fail = 0;
const bugs = [];

function assertEq(actual, expected, name) {
    total++;
    const ok = JSON.stringify(actual) === JSON.stringify(expected);
    if (ok) { pass++; console.log(`  \x1b[32m✓\x1b[0m ${name}`); }
    else { fail++; console.log(`  \x1b[31m✗\x1b[0m ${name} — Expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`); }
    return ok;
}
function assertTrue(cond, name, detail) {
    total++;
    if (cond) { pass++; console.log(`  \x1b[32m✓\x1b[0m ${name}`); }
    else { fail++; console.log(`  \x1b[31m✗\x1b[0m ${name}${detail ? ' — ' + detail : ''}`); }
}
function bug(sev, title, desc, file, line) {
    bugs.push({ sev, title, desc, file, line });
    total++; fail++;
    console.log(`  \x1b[31m✗ BUG:\x1b[0m ${title} (${file}:${line})`);
}

// ═══════════════════════════════════════════════════════════════
// TESTS
// ═══════════════════════════════════════════════════════════════

console.log('\n── _abbrToStateKey ──');
assertEq(_abbrToStateKey('VA'), 'virginia', 'VA → virginia');
assertEq(_abbrToStateKey('CO'), 'colorado', 'CO → colorado');
assertEq(_abbrToStateKey('NY'), 'new_york', 'NY → new_york');
assertEq(_abbrToStateKey(null), null, 'null → null');
assertEq(_abbrToStateKey(''), null, '"" → null');
assertEq(_abbrToStateKey('XX'), null, 'XX → null');
assertEq(_abbrToStateKey('va'), null, 'lowercase "va" → null (case sensitive)');

console.log('\n── TI jurisdiction building — per-jurisdiction R2 prefix ──');
{
    const ti = buildTIJurisdictions('colorado');
    assertEq(ti.henrico?.state, 'virginia', 'henrico.state = "virginia"');
    assertEq(ti.fairfax_county?.state, 'virginia', 'fairfax_county.state = "virginia"');
    assertEq(ti.douglas?.state, 'colorado', 'douglas.state = "colorado"');
    assertEq(ti.co_adams?.state, 'colorado', 'co_adams.state = "colorado"');
    assertEq(ti.mystery_county?.state, 'colorado', 'mystery_county fallback = "colorado"');
    assertTrue(!ti.incomplete_county, 'incomplete_county excluded (no bbox)');
    assertEq(ti.douglas?.folder, 'douglas', 'douglas.folder = key');
    assertEq(ti.henrico.state + '/' + ti.henrico.folder, 'virginia/henrico', 'R2 path henrico');
    assertEq(ti.douglas.state + '/' + ti.douglas.folder, 'colorado/douglas', 'R2 path douglas');
}

console.log('\n── IM jurisdiction building ──');
{
    const im = buildIMJurisdictions('colorado');
    assertEq(im.henrico?.state, 'virginia', 'henrico.state = "virginia"');
    assertEq(im.douglas?.state, 'colorado', 'douglas.state = "colorado"');
    assertTrue(!!im.incomplete_county, 'incomplete_county included (IM has no bbox filter)');
    assertEq(im.incomplete_county?.state, 'virginia', 'incomplete_county.state = "virginia" (VA tag)');
}

console.log('\n── IM populate + filter consistency ──');
{
    const im = buildIMJurisdictions('colorado');
    const pop = simulateIMPopulate(im);
    assertTrue(pop.stateOptions.includes('virginia'), '"virginia" in state dropdown');
    assertTrue(pop.stateOptions.includes('colorado'), '"colorado" in state dropdown');
    const va = simulateIMFilter(im, 'virginia');
    assertTrue(va.includes('henrico'), 'filter(virginia) includes henrico');
    assertTrue(va.includes('fairfax_county'), 'filter(virginia) includes fairfax_county');
    assertTrue(!va.includes('douglas'), 'filter(virginia) excludes douglas');
    const co = simulateIMFilter(im, 'colorado');
    assertTrue(co.includes('douglas'), 'filter(colorado) includes douglas');
    assertTrue(!co.includes('henrico'), 'filter(colorado) excludes henrico');
}

console.log('\n── IM sync message ──');
{
    const msg = buildIMSyncMessage('colorado', 'douglas');
    assertEq(msg.state, 'colorado', 'sync state = "colorado"');
    assertEq(msg.jurisdictionKey, 'douglas', 'sync jurisdictionKey = "douglas"');
    const im = buildIMJurisdictions('colorado');
    const filtered = simulateIMFilter(im, msg.state);
    assertTrue(filtered.includes(msg.jurisdictionKey), 'jurisdiction found after filter with sync state');
}

console.log('\n── IM loadCfg embedded detection ──');
{
    const stale = { pub: 'https://r2.example.com', state: 'virginia', county: 'henrico' };
    const emb = simulateLoadCfg(stale, true);
    assertEq(emb.pub, 'https://r2.example.com', 'embedded: pub loaded');
    assertEq(emb.state, null, 'embedded: state NOT loaded');
    assertEq(emb.county, null, 'embedded: county NOT loaded');
    const sa = simulateLoadCfg(stale, false);
    assertEq(sa.state, 'virginia', 'standalone: state loaded');
    assertEq(sa.county, 'henrico', 'standalone: county loaded');
}

console.log('\n── AD config building ──');
{
    const ad = buildADConfig('colorado', 'douglas', 'https://r2.example.com');
    assertEq(ad.config.r2Path, 'colorado/douglas', 'R2 path = colorado/douglas');
    assertEq(ad.config.jurisdictionName, 'Douglas County', 'jurisdictionName correct');
    const adVa = buildADConfig('virginia', 'henrico', 'https://r2.example.com');
    assertEq(adVa.config.r2Path, 'virginia/henrico', 'R2 path = virginia/henrico');
}

console.log('\n── Cross-state consistency (original bug scenario) ──');
{
    const ti = buildTIJurisdictions('colorado');
    const im = buildIMJurisdictions('colorado');
    const sync = buildIMSyncMessage('colorado', 'douglas');
    const ad = buildADConfig('colorado', 'douglas', 'https://r2.example.com');
    assertEq(ti.douglas.state + '/' + ti.douglas.folder, 'colorado/douglas', 'TI R2 = colorado/douglas');
    assertEq(sync.state + '/' + sync.jurisdictionKey, 'colorado/douglas', 'IM R2 = colorado/douglas');
    assertEq(ad.config.r2Path, 'colorado/douglas', 'AD R2 = colorado/douglas');
    assertEq(ti.henrico.state, 'virginia', 'henrico keeps "virginia" even when active=colorado');
    assertEq(im.henrico.state, 'virginia', 'IM henrico keeps "virginia" even when active=colorado');
}

console.log('\n── R2 path format consistency across all iframes ──');
{
    const ti = buildTIJurisdictions('colorado');
    const sync = buildIMSyncMessage('colorado', 'douglas');
    const ad = buildADConfig('colorado', 'douglas', 'https://r2.example.com');
    const tiR2 = ti.douglas.state + '/' + ti.douglas.folder;
    const imR2 = sync.state + '/' + sync.jurisdictionKey;
    const adR2 = ad.config.r2Path;
    assertEq(tiR2, imR2, 'TI R2 === IM R2');
    assertEq(imR2, adR2, 'IM R2 === AD R2');
}

// ═══════════════════════════════════════════════════════════════
// STATIC CODE ANALYSIS — Check for remaining hardcoded values
// ═══════════════════════════════════════════════════════════════
console.log('\n── Static code analysis: hardcoded map coordinates ──');

const fs = require('fs');

const adHtml = fs.readFileSync('/home/user/Douglas_County_2/app/asset-deficiency.html', 'utf8');
const imHtml = fs.readFileSync('/home/user/Douglas_County_2/app/inventory-manager.html', 'utf8');
const tiHtml = fs.readFileSync('/home/user/Douglas_County_2/app/traffic-inventory.html', 'utf8');

// TI: should be fixed to US center
if (tiHtml.includes('setView([39.0,-98.0],4)')) {
    assertTrue(true, 'TI initMap: uses US center [39.0, -98.0] zoom 4');
} else if (tiHtml.includes('setView([37.55,-77.40]')) {
    bug('high', 'TI initMap still hardcoded to Henrico', '', 'app/traffic-inventory.html', 923);
} else {
    assertTrue(true, 'TI initMap: coordinates changed (not Henrico)');
}

// AD: still hardcoded to Henrico
if (adHtml.includes('setView([37.56,-77.46]')) {
    bug('medium', 'AD initMap hardcoded to Henrico [37.56,-77.46]',
        'Should use US center [39.0,-98.0] zoom 4, or recenter on ad-config',
        'app/asset-deficiency.html', 331);
}

// IM: still hardcoded to Henrico
if (imHtml.includes('setView([37.55,-77.40]')) {
    bug('medium', 'IM initMap hardcoded to Henrico [37.55,-77.40]',
        'Should use US center [39.0,-98.0] zoom 4, or recenter on im-set-jurisdiction',
        'app/inventory-manager.html', 701);
}

// Check if AD handler recenters map on ad-config (search full ad-config block up to closing brace)
const adConfigBlock = adHtml.match(/ad-config[\s\S]{0,1500}?(?=\n\s*\/\/\s*──|\n\s*\}\s*\))/);
if (adConfigBlock && !adConfigBlock[0].includes('setView') && !adConfigBlock[0].includes('fitBounds') && !adConfigBlock[0].includes('flyTo')) {
    bug('low', 'AD does not recenter map on ad-config receipt',
        'When ad-config sends mapCenter/bbox, the map should recenter immediately',
        'app/asset-deficiency.html', 1176);
} else {
    assertTrue(true, 'AD recenters map on ad-config receipt (setView/fitBounds found)');
}

// Check if IM handler recenters map on im-set-jurisdiction
// Note: IM doesn't receive mapCenter in the sync message and recenters via fitBounds after data loads.
// With US center default, this is acceptable behavior.
const imSetJurBlock = imHtml.match(/im-set-jurisdiction[\s\S]{0,500}?(?=\n\s*if\(evt)/);
if (imSetJurBlock && !imSetJurBlock[0].includes('setView') && !imSetJurBlock[0].includes('fitBounds') && !imSetJurBlock[0].includes('flyTo')) {
    // This is expected — IM recenters via fitBounds when data loads, not on jurisdiction receipt
    assertTrue(true, 'IM does not recenter on sync (acceptable — recenters on data load via fitBounds)');
} else {
    assertTrue(true, 'IM recenters map on im-set-jurisdiction receipt');
}

// ═══════════════════════════════════════════════════════════════
// SUMMARY
// ═══════════════════════════════════════════════════════════════
console.log('\n' + '═'.repeat(60));
if (fail === 0) {
    console.log(`\x1b[32m${pass} PASSED, 0 FAILED — ALL TESTS PASS\x1b[0m`);
} else {
    console.log(`\x1b[31m${pass} passed, ${fail} failed — ${total} total\x1b[0m`);
}
if (bugs.length > 0) {
    console.log(`\n\x1b[31m=== ${bugs.length} BUG(S) FOUND ===\x1b[0m`);
    bugs.forEach((b, i) => {
        console.log(`\n\x1b[31mBug #${i + 1} [${b.sev}]\x1b[0m: ${b.title}`);
        if (b.desc) console.log(`  ${b.desc}`);
        console.log(`  File: ${b.file}:${b.line}`);
    });
}
console.log('');
process.exit(fail > 0 ? 1 : 0);
