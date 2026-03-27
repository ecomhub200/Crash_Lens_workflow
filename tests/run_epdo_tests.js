/**
 * Node.js Test Runner for test_dynamic_epdo_scoring.js
 * Mocks DOM, localStorage, and app globals so the browser-targeted test can run headlessly.
 */

// ── DOM Mock ──────────────────────────────────────────────────────────
const elementStore = {};
function mockElement(id) {
    if (!elementStore[id]) {
        elementStore[id] = {
            _id: id,
            _tag: 'div',
            textContent: '',
            value: '',
            checked: false,
            style: { display: '', transform: '' },
            getAttribute(attr) { return this['_attr_' + attr] || null; },
            setAttribute(attr, val) { this['_attr_' + attr] = String(val); },
        };
    }
    return elementStore[id];
}

const _origGetElementById = global.document?.getElementById;
global.document = global.document || {};
global.document.getElementById = function(id) {
    return mockElement(id);
};

// Pre-create all known EPDO DOM elements
const knownIds = [
    'stateSelect',
    'epdoSectionToggle', 'epdoActiveLabel', 'epdoChevron', 'epdoSectionContent',
    'epdoPresetStateDefault', 'epdoPresetHSM', 'epdoPresetVDOT', 'epdoPresetFHWA', 'epdoPresetCustom',
    'epdoStateDefaultDesc',
    'epdoCustomInputs', 'epdoCustomK', 'epdoCustomA', 'epdoCustomB', 'epdoCustomC', 'epdoCustomO',
    'epdoWeightsLabel', 'epdoGlossaryK', 'epdoGlossaryA', 'epdoGlossaryB', 'epdoGlossaryC', 'epdoGlossaryO',
    'epdoGlossaryDef', 'epdoExampleValue',
    'kpiEPDO', 'kpiEPDOAvg',
    'hotspotTable', 'hotspotBody', 'grantLocationTable', 'grantLocationBody',
    'jurisdictionSelect',
    'tierSelector', 'tierRegionRow', 'tierMPORow', 'tierRegionSelect', 'tierMPOSelect',
    'tierScopeIndicator', 'tierScopeText',
];
knownIds.forEach(id => mockElement(id));

// Set default values
mockElement('stateSelect').value = '08'; // Colorado
mockElement('epdoCustomK').value = '462';
mockElement('epdoCustomA').value = '62';
mockElement('epdoCustomB').value = '12';
mockElement('epdoCustomC').value = '5';
mockElement('epdoCustomO').value = '1';
mockElement('epdoSectionContent').style.display = 'none';
mockElement('epdoChevron').style.transform = 'rotate(-90deg)';

// ── localStorage Mock ─────────────────────────────────────────────────
const _store = {};
global.localStorage = {
    getItem(key) { return _store[key] !== undefined ? _store[key] : null; },
    setItem(key, val) { _store[key] = String(val); },
    removeItem(key) { delete _store[key]; },
    clear() { Object.keys(_store).forEach(k => delete _store[k]); },
};

// ── console.warn / console.log passthrough ────────────────────────────
// (already available in Node)

// ── safeJsonParse shim ────────────────────────────────────────────────
global.safeJsonParse = function(str, fallback) {
    try { return JSON.parse(str); } catch { return fallback; }
};

// ── App Global State ──────────────────────────────────────────────────
global.appConfig = {
    apis: { tigerweb: { stateFips: '08' } },
    defaultState: 'colorado',
    states: { colorado: { fips: '08' } }
};

global.crashState = { loaded: false, sampleRows: [], aggregates: {}, hotspots: [] };
global.cmfState = { selectedLocation: null, locationCrashes: [], filteredCrashes: [], crashProfile: null };
global.warrantsState = { selectedLocation: null, locationCrashes: [], filteredCrashes: [], crashProfile: null };
global.grantState = { allRankedLocations: [], loaded: false, rankingCache: { key: null, locations: [] } };
global.baState = { locationCrashes: [], locationStats: null };
global.safetyState = { activeCategory: null, data: {} };
global.selectionState = { location: null, crashes: [], crashProfile: null, fromTab: null };

// ── EPDO Core (extracted from app/index.html) ─────────────────────────

global.EPDO_WEIGHTS = { K: 883, A: 94, B: 21, C: 11, O: 1 };
global.EPDO_ACTIVE_PRESET = 'hsm2010';

global.EPDO_PRESETS = {
    hsm2010:  { name: 'HSM Standard (2010)', weights: { K: 462, A: 62, B: 12, C: 5, O: 1 },
                description: 'Highway Safety Manual standard weights (AASHTO/FHWA)' },
    vdot2024: { name: 'VDOT 2024', weights: { K: 1032, A: 53, B: 16, C: 10, O: 1 },
                description: 'Derived from VDOT 2024 crash cost ratios ($12.8M K / $12.4K O)' },
    fhwa2022: { name: 'FHWA 2022', weights: { K: 975, A: 48, B: 13, C: 8, O: 1 },
                description: 'Derived from FHWA 2022 crash cost ratios ($11.6M K / $11.9K O)' },
    custom:   { name: 'Custom', weights: { K: 883, A: 94, B: 21, C: 11, O: 1 },
                description: 'User-defined custom weights' },
    stateDefault: { name: 'State Default (Auto)', weights: { K: 883, A: 94, B: 21, C: 11, O: 1 },
                description: 'Automatically uses recommended EPDO weights for the selected state',
                isAuto: true }
};

global.STATE_EPDO_WEIGHTS = {
    '51': { name: 'Virginia (VDOT 2025)', weights: { K: 1032, A: 53, B: 16, C: 10, O: 1 },
            source: 'VDOT 2024 crash cost memo ($12.8M K / $12.4K O)' },
    '06': { name: 'California (Caltrans 2023)', weights: { K: 1100, A: 58, B: 17, C: 11, O: 1 },
            source: 'Caltrans TASAS methodology, adjusted for CA cost of living' },
    '48': { name: 'Texas (TxDOT 2023)', weights: { K: 920, A: 55, B: 14, C: 9, O: 1 },
            source: 'TxDOT crash cost data via AASHTOWare Safety' },
    '12': { name: 'Florida (FDOT 2023)', weights: { K: 985, A: 50, B: 15, C: 9, O: 1 },
            source: 'FDOT Safety Office HSIP crash cost estimates' },
    '36': { name: 'New York (NYSDOT 2023)', weights: { K: 1050, A: 55, B: 15, C: 10, O: 1 },
            source: 'NYSDOT Safety Analysis System crash costs' },
    '17': { name: 'Illinois (IDOT 2023)', weights: { K: 850, A: 45, B: 10, C: 5, O: 1 },
            source: 'IDOT crash cost ratios for HSIP analysis' },
    '37': { name: 'North Carolina (NCDOT 2023)', weights: { K: 770, A: 77, B: 8, C: 8, O: 1 },
            source: 'NCDOT HSIP severity methodology (elevated A weight)' },
    '42': { name: 'Pennsylvania (PennDOT 2023)', weights: { K: 975, A: 48, B: 13, C: 8, O: 1 },
            source: 'PennDOT uses FHWA 2022 national baseline' },
    '39': { name: 'Ohio (ODOT 2023)', weights: { K: 975, A: 48, B: 13, C: 8, O: 1 },
            source: 'ODOT uses FHWA 2022 national baseline' },
    '13': { name: 'Georgia (GDOT 2023)', weights: { K: 975, A: 48, B: 13, C: 8, O: 1 },
            source: 'GDOT AASHTO Safety / Numetric crash cost integration' },
    '25': { name: 'Massachusetts (MassDOT 2024)', weights: { K: 1200, A: 60, B: 18, C: 12, O: 1 },
            source: 'MassDOT 2024 crash costs ($19.4M K / $16.2K O)' },
    '34': { name: 'New Jersey (NJDOT 2023)', weights: { K: 1050, A: 55, B: 15, C: 10, O: 1 },
            source: 'NJDOT crash cost analysis (high cost of living adjustment)' },
    '53': { name: 'Washington (WSDOT 2023)', weights: { K: 975, A: 48, B: 13, C: 8, O: 1 },
            source: 'WSDOT uses FHWA 2022 national baseline' },
    '04': { name: 'Arizona (ADOT)', weights: { K: 883, A: 94, B: 21, C: 11, O: 1 },
            source: 'ADOT uses HSM Standard (2010)' },
    '26': { name: 'Michigan (MDOT 2023)', weights: { K: 975, A: 48, B: 13, C: 8, O: 1 },
            source: 'MDOT uses FHWA 2022 national baseline' },
    '49': { name: 'Utah (UDOT 2024)', weights: { K: 975, A: 48, B: 13, C: 8, O: 1 },
            source: 'UDOT annual crash cost update (FHWA methodology)' },
    '41': { name: 'Oregon (ODOT 2023)', weights: { K: 975, A: 48, B: 13, C: 8, O: 1 },
            source: 'Oregon DOT uses FHWA 2022 national baseline' },
    '22': { name: 'Louisiana (LaDOTD 2023)', weights: { K: 975, A: 48, B: 13, C: 8, O: 1 },
            source: 'LaDOTD uses FHWA crash cost methodology' },
    '35': { name: 'New Mexico (NMDOT 2023)', weights: { K: 567, A: 33, B: 33, C: 33, O: 1 },
            source: 'NMDOT uses simplified injury categories' },
    '56': { name: 'Wyoming (WYDOT 2023)', weights: { K: 975, A: 48, B: 13, C: 8, O: 1 },
            source: 'WYDOT uses FHWA national baseline' },
    '09': { name: 'Connecticut (CTDOT 2023)', weights: { K: 1050, A: 55, B: 15, C: 10, O: 1 },
            source: 'CTDOT crash cost analysis (high cost of living)' },
    '01': { name: 'Alabama', weights: { K: 883, A: 94, B: 21, C: 11, O: 1 }, source: 'FHWA 2025 (FHWA-SA-25-021)' },
    '02': { name: 'Alaska', weights: { K: 883, A: 94, B: 21, C: 11, O: 1 }, source: 'FHWA 2025 (FHWA-SA-25-021)' },
    '05': { name: 'Arkansas', weights: { K: 883, A: 94, B: 21, C: 11, O: 1 }, source: 'FHWA 2025 (FHWA-SA-25-021)' },
    '08': { name: 'Colorado (CDOT)', weights: { K: 883, A: 94, B: 21, C: 11, O: 1 }, source: 'FHWA 2025 (FHWA-SA-25-021)' },
    '10': { name: 'Delaware (FHWA 2025)', weights: { K: 883, A: 94, B: 21, C: 11, O: 1 }, source: 'FHWA-SA-25-021 (Oct 2025)' },
    '11': { name: 'District of Columbia', weights: { K: 883, A: 94, B: 21, C: 11, O: 1 }, source: 'FHWA 2025 (FHWA-SA-25-021)' },
    '15': { name: 'Hawaii', weights: { K: 883, A: 94, B: 21, C: 11, O: 1 }, source: 'FHWA 2025 (FHWA-SA-25-021)' },
    '16': { name: 'Idaho', weights: { K: 883, A: 94, B: 21, C: 11, O: 1 }, source: 'FHWA 2025 (FHWA-SA-25-021)' },
    '18': { name: 'Indiana', weights: { K: 883, A: 94, B: 21, C: 11, O: 1 }, source: 'FHWA 2025 (FHWA-SA-25-021)' },
    '19': { name: 'Iowa', weights: { K: 883, A: 94, B: 21, C: 11, O: 1 }, source: 'FHWA 2025 (FHWA-SA-25-021)' },
    '20': { name: 'Kansas', weights: { K: 883, A: 94, B: 21, C: 11, O: 1 }, source: 'FHWA 2025 (FHWA-SA-25-021)' },
    '21': { name: 'Kentucky', weights: { K: 883, A: 94, B: 21, C: 11, O: 1 }, source: 'FHWA 2025 (FHWA-SA-25-021)' },
    '23': { name: 'Maine', weights: { K: 883, A: 94, B: 21, C: 11, O: 1 }, source: 'FHWA 2025 (FHWA-SA-25-021)' },
    '24': { name: 'Maryland', weights: { K: 883, A: 94, B: 21, C: 11, O: 1 }, source: 'FHWA 2025 (FHWA-SA-25-021)' },
    '27': { name: 'Minnesota', weights: { K: 883, A: 94, B: 21, C: 11, O: 1 }, source: 'FHWA 2025 (FHWA-SA-25-021)' },
    '28': { name: 'Mississippi', weights: { K: 883, A: 94, B: 21, C: 11, O: 1 }, source: 'FHWA 2025 (FHWA-SA-25-021)' },
    '29': { name: 'Missouri', weights: { K: 883, A: 94, B: 21, C: 11, O: 1 }, source: 'FHWA 2025 (FHWA-SA-25-021)' },
    '30': { name: 'Montana', weights: { K: 883, A: 94, B: 21, C: 11, O: 1 }, source: 'FHWA 2025 (FHWA-SA-25-021)' },
    '31': { name: 'Nebraska', weights: { K: 883, A: 94, B: 21, C: 11, O: 1 }, source: 'FHWA 2025 (FHWA-SA-25-021)' },
    '32': { name: 'Nevada', weights: { K: 883, A: 94, B: 21, C: 11, O: 1 }, source: 'FHWA 2025 (FHWA-SA-25-021)' },
    '33': { name: 'New Hampshire', weights: { K: 883, A: 94, B: 21, C: 11, O: 1 }, source: 'FHWA 2025 (FHWA-SA-25-021)' },
    '38': { name: 'North Dakota', weights: { K: 883, A: 94, B: 21, C: 11, O: 1 }, source: 'FHWA 2025 (FHWA-SA-25-021)' },
    '40': { name: 'Oklahoma', weights: { K: 883, A: 94, B: 21, C: 11, O: 1 }, source: 'FHWA 2025 (FHWA-SA-25-021)' },
    '44': { name: 'Rhode Island', weights: { K: 883, A: 94, B: 21, C: 11, O: 1 }, source: 'FHWA 2025 (FHWA-SA-25-021)' },
    '45': { name: 'South Carolina', weights: { K: 883, A: 94, B: 21, C: 11, O: 1 }, source: 'FHWA 2025 (FHWA-SA-25-021)' },
    '46': { name: 'South Dakota', weights: { K: 883, A: 94, B: 21, C: 11, O: 1 }, source: 'FHWA 2025 (FHWA-SA-25-021)' },
    '47': { name: 'Tennessee', weights: { K: 883, A: 94, B: 21, C: 11, O: 1 }, source: 'FHWA 2025 (FHWA-SA-25-021)' },
    '50': { name: 'Vermont', weights: { K: 883, A: 94, B: 21, C: 11, O: 1 }, source: 'FHWA 2025 (FHWA-SA-25-021)' },
    '54': { name: 'West Virginia', weights: { K: 883, A: 94, B: 21, C: 11, O: 1 }, source: 'FHWA 2025 (FHWA-SA-25-021)' },
    '55': { name: 'Wisconsin', weights: { K: 883, A: 94, B: 21, C: 11, O: 1 }, source: 'FHWA 2025 (FHWA-SA-25-021)' },
    '_default': { name: 'FHWA 2025', weights: { K: 883, A: 94, B: 21, C: 11, O: 1 },
                  source: 'Highway Safety Manual standard EPDO weights' }
};

global.calcEPDO = s => (s.K||0)*EPDO_WEIGHTS.K + (s.A||0)*EPDO_WEIGHTS.A + (s.B||0)*EPDO_WEIGHTS.B + (s.C||0)*EPDO_WEIGHTS.C + (s.O||0)*EPDO_WEIGHTS.O;

global.calculateEPDO = function(severity) {
    return calcEPDO(severity);
};

global.getStateEPDOWeights = function(stateFips) {
    const padded = String(stateFips).padStart(2, '0');
    return STATE_EPDO_WEIGHTS[padded] || STATE_EPDO_WEIGHTS['_default'];
};

global.getCurrentStateFips = function() {
    const select = document.getElementById('stateSelect');
    if (select?.value) return select.value;
    if (typeof appConfig !== 'undefined' && appConfig?.apis?.tigerweb?.stateFips) {
        return appConfig.apis.tigerweb.stateFips;
    }
    return '08';
};

global.applyStateDefaultEPDO = function(stateFips, stateName) {
    if (EPDO_ACTIVE_PRESET !== 'stateDefault') return;
    const stateEntry = getStateEPDOWeights(stateFips);
    EPDO_WEIGHTS = { ...stateEntry.weights };
    EPDO_PRESETS.stateDefault.weights = { ...stateEntry.weights };
    EPDO_PRESETS.stateDefault.name = `State Default (${stateEntry.name})`;
    EPDO_PRESETS.stateDefault.description = stateEntry.source;
    updateEPDOPresetUI();
    updateEPDOWeightLabels();
};

global.updateEPDOPresetUI = function() {
    const radioMap = {
        stateDefault: 'epdoPresetStateDefault',
        hsm2010: 'epdoPresetHSM',
        vdot2024: 'epdoPresetVDOT',
        fhwa2022: 'epdoPresetFHWA',
        custom: 'epdoPresetCustom'
    };
    Object.entries(radioMap).forEach(([key, id]) => {
        const radio = document.getElementById(id);
        if (radio) radio.checked = (key === EPDO_ACTIVE_PRESET);
    });
    const customInputs = document.getElementById('epdoCustomInputs');
    if (customInputs) customInputs.style.display = EPDO_ACTIVE_PRESET === 'custom' ? 'grid' : 'none';
    const activeLabel = document.getElementById('epdoActiveLabel');
    if (activeLabel) {
        const presetName = EPDO_PRESETS[EPDO_ACTIVE_PRESET]?.name || 'Custom';
        activeLabel.textContent = `${presetName} — K=${EPDO_WEIGHTS.K}`;
    }
    const stateDescEl = document.getElementById('epdoStateDefaultDesc');
    if (stateDescEl && EPDO_PRESETS.stateDefault) {
        const w = EPDO_PRESETS.stateDefault.weights;
        stateDescEl.textContent = `${EPDO_PRESETS.stateDefault.name}: K=${w.K}, A=${w.A}, B=${w.B}, C=${w.C}, O=1`;
    }
};

global.updateEPDOWeightLabels = function() {
    const label = `Weights: K=${EPDO_WEIGHTS.K}, A=${EPDO_WEIGHTS.A}, B=${EPDO_WEIGHTS.B}, C=${EPDO_WEIGHTS.C}, O=${EPDO_WEIGHTS.O}`;
    const presetName = EPDO_PRESETS[EPDO_ACTIVE_PRESET]?.name || 'Custom';
    const dashLabel = document.getElementById('epdoWeightsLabel');
    if (dashLabel) dashLabel.textContent = `${label} (${presetName})`;
    const glossaryDef = document.getElementById('epdoGlossaryDef');
    if (glossaryDef) glossaryDef.textContent = `Weighted severity score: ${label}. Prioritizes locations with severe crashes over high-volume minor crash locations. Using ${presetName} preset.`;
    ['K','A','B','C','O'].forEach(sev => {
        const el = document.getElementById('epdoGlossary' + sev);
        if (el) el.textContent = String(EPDO_WEIGHTS[sev]);
    });
    const exampleEl = document.getElementById('epdoExampleValue');
    if (exampleEl) exampleEl.textContent = (2 * EPDO_WEIGHTS.K).toLocaleString();
};

global.recalculateAllEPDO = function() {
    if (!crashState.loaded) return;
    // In test harness, we don't call the downstream update functions
};

global.loadEPDOPreset = function(presetKey) {
    if (presetKey === 'custom') {
        EPDO_WEIGHTS = {
            K: parseInt(document.getElementById('epdoCustomK')?.value) || 462,
            A: parseInt(document.getElementById('epdoCustomA')?.value) || 62,
            B: parseInt(document.getElementById('epdoCustomB')?.value) || 12,
            C: parseInt(document.getElementById('epdoCustomC')?.value) || 5,
            O: parseInt(document.getElementById('epdoCustomO')?.value) || 1
        };
        EPDO_PRESETS.custom.weights = { ...EPDO_WEIGHTS };
    } else if (presetKey === 'stateDefault') {
        const currentStateFips = getCurrentStateFips();
        const stateEntry = getStateEPDOWeights(currentStateFips);
        EPDO_WEIGHTS = { ...stateEntry.weights };
        EPDO_PRESETS.stateDefault.weights = { ...stateEntry.weights };
        EPDO_PRESETS.stateDefault.name = `State Default (${stateEntry.name})`;
        EPDO_PRESETS.stateDefault.description = stateEntry.source;
    } else {
        const preset = EPDO_PRESETS[presetKey];
        if (!preset) { console.warn('[EPDO] Unknown preset:', presetKey); return; }
        EPDO_WEIGHTS = { ...preset.weights };
    }
    EPDO_ACTIVE_PRESET = presetKey;
    localStorage.setItem('epdoActivePreset', presetKey);
    if (presetKey === 'custom') localStorage.setItem('epdoCustomWeights', JSON.stringify(EPDO_WEIGHTS));
    updateEPDOPresetUI();
    updateEPDOWeightLabels();
    recalculateAllEPDO();
};

global.loadSavedEPDOPreset = function() {
    let saved = localStorage.getItem('epdoActivePreset');
    if (!saved) {
        saved = 'stateDefault';
        localStorage.setItem('epdoActivePreset', 'stateDefault');
    }
    if (saved && EPDO_PRESETS[saved]) {
        if (saved === 'custom') {
            const cw = safeJsonParse(localStorage.getItem('epdoCustomWeights'), null);
            if (cw) {
                EPDO_PRESETS.custom.weights = cw;
                ['K','A','B','C','O'].forEach(f => {
                    const el = document.getElementById('epdoCustom' + f);
                    if (el) el.value = cw[f] || (f === 'O' ? 1 : 0);
                });
            }
        }
        if (saved === 'stateDefault') {
            const currentStateFips = getCurrentStateFips();
            const stateEntry = getStateEPDOWeights(currentStateFips);
            EPDO_WEIGHTS = { ...stateEntry.weights };
            EPDO_PRESETS.stateDefault.weights = { ...stateEntry.weights };
            EPDO_PRESETS.stateDefault.name = `State Default (${stateEntry.name})`;
            EPDO_PRESETS.stateDefault.description = stateEntry.source;
        } else if (saved === 'custom') {
            EPDO_WEIGHTS = { ...EPDO_PRESETS.custom.weights };
        } else {
            const preset = EPDO_PRESETS[saved];
            EPDO_WEIGHTS = { ...preset.weights };
        }
        EPDO_ACTIVE_PRESET = saved;
        updateEPDOPresetUI();
        updateEPDOWeightLabels();
    }
};

global.saveCustomEPDOWeights = function() {
    if (EPDO_ACTIVE_PRESET === 'custom') loadEPDOPreset('custom');
};

global.toggleEPDOSection = function() {
    const content = document.getElementById('epdoSectionContent');
    const chevron = document.getElementById('epdoChevron');
    const toggle = document.getElementById('epdoSectionToggle');
    if (!content) return;
    const isExpanded = content.style.display !== 'none';
    content.style.display = isExpanded ? 'none' : 'flex';
    if (chevron) chevron.style.transform = isExpanded ? 'rotate(-90deg)' : 'rotate(0deg)';
    if (toggle) toggle.setAttribute('aria-expanded', String(!isExpanded));
};

// ── Stubs for functions that the test checks existence of ─────────────
global.roundabout_autoPopulateCrashData = function() {};
global.streetlight_analyzeCrashesByLight = function() {};
global.updateDashboard = function() {};
global.analyzeHotspots = function() {};
global.rankLocationsForGrants = function() {};
global.buildCMFCrashProfile = function() {};
global.updateCMFUI = function() {};
global.updateMapStats = function() {};
global.updateAIContextIndicator = function() {};

// ── alert shim ────────────────────────────────────────────────────────
global.alert = function(msg) { console.log('[alert]', msg); };

// ── setTimeout shim ───────────────────────────────────────────────────
// Already available in Node

// ── Run the test ──────────────────────────────────────────────────────
console.log('\nLoading test_dynamic_epdo_scoring.js...\n');

const fs = require('fs');
const path = require('path');
const testCode = fs.readFileSync(path.join(__dirname, 'test_dynamic_epdo_scoring.js'), 'utf8');
const result = eval(testCode);

// Exit with code 1 if any tests failed
if (result && result.failed > 0) {
    process.exit(1);
}
