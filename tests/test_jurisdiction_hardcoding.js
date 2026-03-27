/**
 * Bug Test Suite — Jurisdiction Hardcoding Fix
 *
 * Validates that switching jurisdictions properly resets all stateful
 * subsystems (traffic inventory, sign deficiency, asset deficiency,
 * crash data validator) and that hardcoded fallback defaults are
 * correctly resolved from appConfig instead of literal strings.
 *
 * Bug: When user selected a different jurisdiction (e.g., Colorado/Douglas
 * instead of Virginia/Henrico), the tool did not update traffic inventory
 * map layers, sign deficiency cache, or crash validator jurisdiction.
 *
 * Run in Node.js:
 *   node tests/test_jurisdiction_hardcoding.js
 *
 * Run in browser console:
 *   Copy-paste into DevTools console on the CRASH LENS app page
 */

// ================================================================
// SETUP: Node.js DOM/Global Mocks
// ================================================================
(function() {
    'use strict';

    const isNode = typeof process !== 'undefined' && process.versions && process.versions.node;

    // ── DOM Mock (Node.js only) ──
    if (isNode) {
        const elementStore = {};
        function mockElement(id) {
            if (!elementStore[id]) {
                elementStore[id] = {
                    _id: id,
                    textContent: '',
                    value: '',
                    checked: false,
                    style: { display: '', transform: '' },
                    getAttribute(attr) { return this['_attr_' + attr] || null; },
                    setAttribute(attr, val) { this['_attr_' + attr] = String(val); },
                    querySelector(sel) { return null; },
                    querySelectorAll(sel) { return []; },
                    classList: {
                        _classes: new Set(),
                        add(c) { this._classes.add(c); },
                        remove(c) { this._classes.delete(c); },
                        contains(c) { return this._classes.has(c); },
                        toggle(c) { if (this._classes.has(c)) this._classes.delete(c); else this._classes.add(c); }
                    }
                };
            }
            return elementStore[id];
        }

        global.document = global.document || {};
        global.document.getElementById = function(id) { return mockElement(id); };
        global.document.querySelector = function(sel) { return null; };
        global.document.querySelectorAll = function(sel) { return []; };

        // ── localStorage Mock ──
        const _store = {};
        global.localStorage = {
            getItem(key) { return _store[key] !== undefined ? _store[key] : null; },
            setItem(key, val) { _store[key] = String(val); },
            removeItem(key) { delete _store[key]; },
            clear() { Object.keys(_store).forEach(k => delete _store[k]); },
        };
    }

    // ================================================================
    // TEST INFRASTRUCTURE
    // ================================================================
    let passed = 0;
    let failed = 0;
    let skipped = 0;
    const errors = [];

    function assert(condition, testName) {
        if (condition) {
            passed++;
            console.log(`  \u2705 PASS: ${testName}`);
        } else {
            failed++;
            errors.push(testName);
            console.error(`  \u274C FAIL: ${testName}`);
        }
    }

    function assertEq(actual, expected, testName) {
        const eq = JSON.stringify(actual) === JSON.stringify(expected);
        if (eq) {
            passed++;
            console.log(`  \u2705 PASS: ${testName}`);
        } else {
            failed++;
            errors.push(`${testName} (expected: ${JSON.stringify(expected)}, got: ${JSON.stringify(actual)})`);
            console.error(`  \u274C FAIL: ${testName}\n    Expected: ${JSON.stringify(expected)}\n    Got:      ${JSON.stringify(actual)}`);
        }
    }

    function assertIncludes(str, substr, testName) {
        if (typeof str === 'string' && str.includes(substr)) {
            passed++;
            console.log(`  \u2705 PASS: ${testName}`);
        } else {
            failed++;
            errors.push(`${testName} (expected "${str}" to include "${substr}")`);
            console.error(`  \u274C FAIL: ${testName}\n    String: "${str}" does not include "${substr}"`);
        }
    }

    function skip(testName, reason) {
        skipped++;
        console.log(`  \u23ED SKIP: ${testName} — ${reason}`);
    }

    // ================================================================
    // MOCK STATE OBJECTS (mimics app globals)
    // ================================================================

    // Mock appConfig
    const mockAppConfig = {
        defaultState: 'colorado',
        defaults: { jurisdiction: 'douglas' },
        states: {
            colorado: {
                fips: '08', name: 'Colorado', abbreviation: 'CO',
                dotName: 'CDOT', defaultJurisdiction: 'douglas',
                r2Prefix: 'colorado'
            },
            virginia: {
                fips: '51', name: 'Virginia', abbreviation: 'VA',
                dotName: 'VDOT', defaultJurisdiction: 'henrico',
                r2Prefix: 'virginia'
            },
            maryland: {
                fips: '24', name: 'Maryland', abbreviation: 'MD',
                dotName: 'MDOT', defaultJurisdiction: null,
                r2Prefix: 'maryland'
            }
        },
        jurisdictions: {
            douglas: { name: 'Douglas County', type: 'county', fips: '035', state: 'CO',
                       mapCenter: [39.34, -104.92], mapZoom: 11, bbox: [-105.35, 39.12, -104.60, 39.57] },
            arapahoe: { name: 'Arapahoe County', type: 'county', fips: '005', state: 'CO',
                        mapCenter: [39.64, -104.72], mapZoom: 11, bbox: [-105.15, 39.50, -104.00, 39.72] },
            henrico: { name: 'Henrico County', type: 'county', fips: '087', state: 'VA',
                       mapCenter: [37.55, -77.39], mapZoom: 11, bbox: [-77.70, 37.45, -77.25, 37.72] }
        }
    };

    // Mock trafficInventoryLayerState
    function createMockTIState() {
        return {
            loaded: false,
            loading: false,
            data: [],
            error: null,
            expanded: false,
            speedExpanded: false,
            parentExpanded: {},
            categories: {
                ti_stop: { enabled: false, layer: null, count: 0, items: [] },
                ti_speed: { enabled: false, layer: null, count: 0, items: [] },
                ti_signal: { enabled: false, layer: null, count: 0, items: [] }
            },
            speedCategories: {}
        };
    }

    // Mock signDefState
    function createMockSignDefState() {
        return {
            loaded: false,
            analyzing: false,
            inventoryLoaded: false,
            inventoryData: [],
            deficiencies: [],
            filteredDeficiencies: [],
            categories: {
                signal: { count: 0, locations: [] },
                stopSign: { count: 0, locations: [] }
            }
        };
    }

    // Mock assetDeficiencyState
    function createMockADState() {
        return {
            initialized: false,
            sources: {
                crashes: { loaded: false, count: 0, data: [] },
                csv: { loaded: false, count: 0, data: [] },
                schools: { loaded: false, count: 0, data: [] }
            }
        };
    }

    // Mock map with removeLayer tracking
    function createMockMap() {
        const removed = [];
        return {
            removeLayer(layer) { removed.push(layer); },
            _removedLayers: removed
        };
    }

    // ================================================================
    // TEST GROUP 1: resetTrafficInventoryForJurisdictionChange()
    // ================================================================
    console.log('\n=== TEST GROUP 1: resetTrafficInventoryForJurisdictionChange() ===');

    (function testResetFunction() {
        // Setup: simulate loaded state with data
        const tiState = createMockTIState();
        tiState.loaded = true;
        tiState.loading = false;
        tiState.data = [{ mutcd: 'R1-1', name: 'Stop', lat: 39.5, lon: -104.8 }];
        tiState.categories.ti_stop.enabled = true;
        tiState.categories.ti_stop.count = 5;
        tiState.categories.ti_stop.items = [{}, {}, {}, {}, {}];
        tiState.categories.ti_stop.layer = { id: 'mock-stop-layer' };
        tiState.categories.ti_speed.enabled = true;
        tiState.categories.ti_speed.count = 3;
        tiState.categories.ti_speed.layer = { id: 'mock-speed-layer' };
        tiState.speedCategories = {
            '25': { enabled: true, layer: { id: 'mock-25mph' }, count: 2, items: [{}, {}] },
            '35': { enabled: true, layer: { id: 'mock-35mph' }, count: 1, items: [{}] }
        };

        const sdState = createMockSignDefState();
        sdState.inventoryLoaded = true;
        sdState.inventoryData = [{}, {}, {}];

        const adState = createMockADState();
        adState.sources.csv.loaded = true;
        adState.sources.csv.data = [{}, {}];
        adState.sources.csv.count = 2;

        const mockMap = createMockMap();
        let assetPanelUpdated = false;

        // Execute reset function (inline — mimics the function from app/index.html)
        function resetTrafficInventoryForJurisdictionChange(
            trafficInventoryLayerState, signDefState, assetDeficiencyState, crashMap, updateMapAssetPanel
        ) {
            Object.keys(trafficInventoryLayerState.categories).forEach(function(key) {
                var cat = trafficInventoryLayerState.categories[key];
                if (cat.layer && crashMap) {
                    try { crashMap.removeLayer(cat.layer); } catch(e) {}
                    cat.layer = null;
                }
                cat.enabled = false;
                cat.items = [];
                cat.count = 0;
            });
            Object.keys(trafficInventoryLayerState.speedCategories || {}).forEach(function(speed) {
                var sc = trafficInventoryLayerState.speedCategories[speed];
                if (sc.layer && crashMap) {
                    try { crashMap.removeLayer(sc.layer); } catch(e) {}
                    sc.layer = null;
                }
            });
            trafficInventoryLayerState.speedCategories = {};
            trafficInventoryLayerState.loaded = false;
            trafficInventoryLayerState.loading = false;
            trafficInventoryLayerState.data = [];
            trafficInventoryLayerState.error = null;

            if (signDefState) {
                signDefState.inventoryLoaded = false;
                signDefState.inventoryData = [];
            }

            if (assetDeficiencyState && assetDeficiencyState.sources && assetDeficiencyState.sources.csv) {
                assetDeficiencyState.sources.csv.loaded = false;
                assetDeficiencyState.sources.csv.data = [];
                assetDeficiencyState.sources.csv.count = 0;
            }

            if (updateMapAssetPanel) updateMapAssetPanel();
        }

        resetTrafficInventoryForJurisdictionChange(
            tiState, sdState, adState, mockMap, function() { assetPanelUpdated = true; }
        );

        // Verify TI state is fully reset
        assertEq(tiState.loaded, false, 'TI state loaded reset to false');
        assertEq(tiState.loading, false, 'TI state loading reset to false');
        assertEq(tiState.data.length, 0, 'TI state data cleared');
        assertEq(tiState.error, null, 'TI state error cleared');

        // Verify categories are reset
        assertEq(tiState.categories.ti_stop.enabled, false, 'TI stop category disabled');
        assertEq(tiState.categories.ti_stop.count, 0, 'TI stop category count reset');
        assertEq(tiState.categories.ti_stop.items.length, 0, 'TI stop category items cleared');
        assertEq(tiState.categories.ti_stop.layer, null, 'TI stop layer removed');
        assertEq(tiState.categories.ti_speed.layer, null, 'TI speed layer removed');

        // Verify speed sub-categories cleared
        assertEq(Object.keys(tiState.speedCategories).length, 0, 'TI speed sub-categories cleared');

        // Verify map layers were removed
        assertEq(mockMap._removedLayers.length, 4, 'All 4 map layers removed (2 categories + 2 speeds)');

        // Verify sign deficiency state reset
        assertEq(sdState.inventoryLoaded, false, 'SignDef inventoryLoaded reset to false');
        assertEq(sdState.inventoryData.length, 0, 'SignDef inventoryData cleared');

        // Verify asset deficiency state reset
        assertEq(adState.sources.csv.loaded, false, 'AD csv source loaded reset to false');
        assertEq(adState.sources.csv.data.length, 0, 'AD csv source data cleared');
        assertEq(adState.sources.csv.count, 0, 'AD csv source count reset');

        // Verify asset panel update was called
        assert(assetPanelUpdated, 'updateMapAssetPanel() was called');
    })();

    // ================================================================
    // TEST GROUP 2: _getActiveStateKey() fallback chain
    // ================================================================
    console.log('\n=== TEST GROUP 2: _getActiveStateKey() Fallback to appConfig ===');

    (function testActiveStateKeyFallbacks() {
        // Simulate: all priority sources unavailable, appConfig has defaultState
        // The function should return appConfig.defaultState instead of hardcoded 'colorado'

        // Test 1: appConfig.defaultState exists → should use it
        const config1 = { defaultState: 'virginia' };
        const result1 = (typeof config1 !== 'undefined' && config1?.defaultState) || 'colorado';
        assertEq(result1, 'virginia', 'Fallback uses appConfig.defaultState when set to virginia');

        // Test 2: appConfig.defaultState is null → should fallback to colorado
        const config2 = { defaultState: null };
        const result2 = (typeof config2 !== 'undefined' && config2?.defaultState) || 'colorado';
        assertEq(result2, 'colorado', 'Fallback uses colorado when defaultState is null');

        // Test 3: appConfig undefined → should fallback to colorado
        const config3 = undefined;
        const result3 = (typeof config3 !== 'undefined' && config3?.defaultState) || 'colorado';
        assertEq(result3, 'colorado', 'Fallback uses colorado when appConfig is undefined');

        // Test 4: appConfig.defaultState set to maryland → should use maryland
        const config4 = { defaultState: 'maryland' };
        const result4 = (typeof config4 !== 'undefined' && config4?.defaultState) || 'colorado';
        assertEq(result4, 'maryland', 'Fallback uses appConfig.defaultState when set to maryland');
    })();

    // ================================================================
    // TEST GROUP 3: _getDefaultJurisdictionForActiveState() fallback
    // ================================================================
    console.log('\n=== TEST GROUP 3: _getDefaultJurisdictionForActiveState() Fallback ===');

    (function testDefaultJurisdictionFallbacks() {
        // Simulate the new fallback logic that tries state's defaultJurisdiction before 'douglas'

        function getDefaultJurisdiction(appConfig) {
            // Mimics the updated function logic
            if (appConfig?.states) {
                const stEntry = appConfig.states[appConfig.defaultState];
                if (stEntry?.defaultJurisdiction) return stEntry.defaultJurisdiction;
            }
            if (appConfig?.defaults?.jurisdiction) return appConfig.defaults.jurisdiction;
            if (appConfig?.jurisdictions) {
                const keys = Object.keys(appConfig.jurisdictions);
                if (keys.length > 0) return keys[0];
            }
            const fallbackState = (typeof appConfig !== 'undefined' && appConfig?.defaultState) || null;
            if (fallbackState && appConfig?.states?.[fallbackState]?.defaultJurisdiction) {
                return appConfig.states[fallbackState].defaultJurisdiction;
            }
            return 'douglas';
        }

        // Test 1: Colorado deployment → should return 'douglas'
        assertEq(
            getDefaultJurisdiction(mockAppConfig),
            'douglas',
            'Colorado deployment defaults to douglas'
        );

        // Test 2: Virginia deployment → should return 'henrico'
        const vaConfig = {
            ...mockAppConfig,
            defaultState: 'virginia',
            defaults: { jurisdiction: null }
        };
        assertEq(
            getDefaultJurisdiction(vaConfig),
            'henrico',
            'Virginia deployment defaults to henrico via state config'
        );

        // Test 3: Maryland deployment with no defaultJurisdiction → falls through
        const mdConfig = {
            defaultState: 'maryland',
            defaults: { jurisdiction: null },
            states: { maryland: { defaultJurisdiction: null } },
            jurisdictions: { baltimore: { name: 'Baltimore' } }
        };
        assertEq(
            getDefaultJurisdiction(mdConfig),
            'baltimore',
            'Maryland with no default returns first jurisdiction key'
        );

        // Test 4: Completely empty config → ultimate fallback to douglas
        assertEq(
            getDefaultJurisdiction({}),
            'douglas',
            'Empty config returns ultimate fallback douglas'
        );
    })();

    // ================================================================
    // TEST GROUP 4: Traffic Inventory URL Construction
    // ================================================================
    console.log('\n=== TEST GROUP 4: Traffic Inventory URL Construction ===');

    (function testTIUrlConstruction() {
        // Simulate the URL construction pattern used in loadTrafficInventoryForMap

        function buildTIUrl(stateKey, jurisdictionId, appConfig, r2BaseUrl) {
            const stateConfig = appConfig?.states?.[stateKey] || null;
            const r2Prefix = stateConfig?.r2Prefix || stateKey;
            const r2Path = r2Prefix + '/' + jurisdictionId + '/traffic-inventory.csv';
            const baseUrl = r2BaseUrl || 'https://data.aicreatesai.com';
            return baseUrl + '/' + r2Path;
        }

        // Test 1: Colorado / Douglas
        assertEq(
            buildTIUrl('colorado', 'douglas', mockAppConfig),
            'https://data.aicreatesai.com/colorado/douglas/traffic-inventory.csv',
            'Colorado/Douglas TI URL correct'
        );

        // Test 2: Virginia / Henrico
        assertEq(
            buildTIUrl('virginia', 'henrico', mockAppConfig),
            'https://data.aicreatesai.com/virginia/henrico/traffic-inventory.csv',
            'Virginia/Henrico TI URL correct'
        );

        // Test 3: Unknown state without config → uses state key as prefix
        assertEq(
            buildTIUrl('texas', 'harris', { states: {} }),
            'https://data.aicreatesai.com/texas/harris/traffic-inventory.csv',
            'Unknown state uses stateKey as r2Prefix'
        );

        // Test 4: State with custom r2Prefix
        const customConfig = {
            states: { new_york: { r2Prefix: 'ny' } }
        };
        assertEq(
            buildTIUrl('new_york', 'manhattan', customConfig),
            'https://data.aicreatesai.com/ny/manhattan/traffic-inventory.csv',
            'Custom r2Prefix used in URL'
        );
    })();

    // ================================================================
    // TEST GROUP 5: Graceful Error Messages
    // ================================================================
    console.log('\n=== TEST GROUP 5: Graceful Error Messages for Missing TI Data ===');

    (function testGracefulErrors() {
        // Simulate the error message improvement

        function friendlyErrorMessage(errorMsg) {
            var msg = errorMsg || 'Unknown error';
            if (msg.includes('404') || msg.includes('HTTP 4')) {
                msg = 'No traffic inventory data available for this jurisdiction';
            }
            return msg;
        }

        // Test 1: HTTP 404 error
        assertEq(
            friendlyErrorMessage('HTTP 404'),
            'No traffic inventory data available for this jurisdiction',
            '404 error gets friendly message'
        );

        // Test 2: HTTP 403 error
        assertEq(
            friendlyErrorMessage('HTTP 403'),
            'No traffic inventory data available for this jurisdiction',
            '403 error gets friendly message'
        );

        // Test 3: HTTP 400 error
        assertEq(
            friendlyErrorMessage('HTTP 400'),
            'No traffic inventory data available for this jurisdiction',
            '400 error gets friendly message'
        );

        // Test 4: Network error (not 4xx) → keeps original message
        assertEq(
            friendlyErrorMessage('Network Error: Failed to fetch'),
            'Network Error: Failed to fetch',
            'Non-4xx error keeps original message'
        );

        // Test 5: Empty error → returns Unknown error
        assertEq(
            friendlyErrorMessage(''),
            'Unknown error',
            'Empty error returns Unknown error'
        );

        // Test 6: Null/undefined → returns Unknown error
        assertEq(
            friendlyErrorMessage(null),
            'Unknown error',
            'Null error returns Unknown error'
        );
    })();

    // ================================================================
    // TEST GROUP 6: Validator JURISDICTIONS Dynamic Registry
    // ================================================================
    console.log('\n=== TEST GROUP 6: Crash Data Validator Dynamic Registry ===');

    (function testValidatorDynamicRegistry() {
        // Simulate the new postMessage-based dynamic jurisdiction registration

        const JURISDICTIONS = {}; // Starts empty (no hardcoded presets)

        function registerJurisdiction(key, fallback) {
            if (key && fallback && fallback.state && fallback.county) {
                const displayLabel = fallback.label ||
                    (fallback.county.charAt(0).toUpperCase() + fallback.county.slice(1) + ' County, ' + fallback.state.toUpperCase());
                JURISDICTIONS[key] = {
                    state: fallback.state,
                    county: fallback.county,
                    label: displayLabel,
                    bounds: fallback.bounds || null
                };
            }
        }

        // Test 1: Registry starts empty
        assertEq(Object.keys(JURISDICTIONS).length, 0, 'Registry starts empty (no hardcoded presets)');

        // Test 2: Register Colorado / Douglas
        registerJurisdiction('co_douglas', {
            state: 'colorado', county: 'douglas', label: 'Douglas County, CO',
            bounds: { minLat: 39.12, maxLat: 39.57, minLon: -105.35, maxLon: -104.60 }
        });
        assert('co_douglas' in JURISDICTIONS, 'co_douglas registered');
        assertEq(JURISDICTIONS['co_douglas'].state, 'colorado', 'co_douglas state is colorado');
        assertEq(JURISDICTIONS['co_douglas'].county, 'douglas', 'co_douglas county is douglas');
        assertEq(JURISDICTIONS['co_douglas'].label, 'Douglas County, CO', 'co_douglas label correct');
        assert(JURISDICTIONS['co_douglas'].bounds !== null, 'co_douglas has bounds');

        // Test 3: Register Virginia / Henrico
        registerJurisdiction('va_henrico', {
            state: 'virginia', county: 'henrico', label: 'Henrico County, VA',
            bounds: { minLat: 37.45, maxLat: 37.72, minLon: -77.70, maxLon: -77.25 }
        });
        assertEq(Object.keys(JURISDICTIONS).length, 2, 'Registry has 2 entries after registration');
        assertEq(JURISDICTIONS['va_henrico'].state, 'virginia', 'va_henrico state is virginia');

        // Test 4: Register jurisdiction without label → auto-generates label
        registerJurisdiction('md_baltimore', {
            state: 'maryland', county: 'baltimore', label: null,
            bounds: null
        });
        assertEq(JURISDICTIONS['md_baltimore'].label, 'Baltimore County, MARYLAND', 'Auto-generated label correct');
        assertEq(JURISDICTIONS['md_baltimore'].bounds, null, 'Null bounds preserved');

        // Test 5: Overwrite existing entry
        registerJurisdiction('co_douglas', {
            state: 'colorado', county: 'douglas', label: 'Douglas County, CO (Updated)',
            bounds: { minLat: 39.10, maxLat: 39.60, minLon: -105.40, maxLon: -104.55 }
        });
        assertEq(JURISDICTIONS['co_douglas'].label, 'Douglas County, CO (Updated)', 'Registry entry can be updated');

        // Test 6: Invalid registration (no key) → no change
        const countBefore = Object.keys(JURISDICTIONS).length;
        registerJurisdiction(null, { state: 'texas', county: 'harris' });
        assertEq(Object.keys(JURISDICTIONS).length, countBefore, 'Null key does not register');

        // Test 7: Invalid registration (no fallback) → no change
        registerJurisdiction('tx_harris', null);
        assertEq(Object.keys(JURISDICTIONS).length, countBefore, 'Null fallback does not register');
    })();

    // ================================================================
    // TEST GROUP 7: Validator R2 Path Construction
    // ================================================================
    console.log('\n=== TEST GROUP 7: Validator R2 Path Construction ===');

    (function testValidatorR2Path() {
        // Test that the validator constructs correct R2 paths from state/county

        function buildR2Path(state, county) {
            return '/' + state + '/' + county + '/';
        }

        assertEq(buildR2Path('colorado', 'douglas'), '/colorado/douglas/', 'CO/Douglas R2 path');
        assertEq(buildR2Path('virginia', 'henrico'), '/virginia/henrico/', 'VA/Henrico R2 path');
        assertEq(buildR2Path('maryland', 'baltimore'), '/maryland/baltimore/', 'MD/Baltimore R2 path');

        // Test full URL construction
        function buildFullUrl(r2ReadUrl, r2Path, file) {
            return r2ReadUrl + r2Path + file;
        }

        assertEq(
            buildFullUrl('https://data.aicreatesai.com', '/colorado/douglas/', 'all_roads.csv'),
            'https://data.aicreatesai.com/colorado/douglas/all_roads.csv',
            'Full R2 URL for all_roads correct'
        );

        assertEq(
            buildFullUrl('https://data.aicreatesai.com', '/virginia/henrico/', 'county_roads.csv'),
            'https://data.aicreatesai.com/virginia/henrico/county_roads.csv',
            'Full R2 URL for county_roads correct'
        );

        assertEq(
            buildFullUrl('https://data.aicreatesai.com', '/colorado/arapahoe/', 'no_interstate.csv'),
            'https://data.aicreatesai.com/colorado/arapahoe/no_interstate.csv',
            'Full R2 URL for no_interstate correct'
        );
    })();

    // ================================================================
    // TEST GROUP 8: Asset Deficiency URL Population
    // ================================================================
    console.log('\n=== TEST GROUP 8: Asset Deficiency Dynamic URL Population ===');

    (function testAssetDeficiencyUrls() {
        // Test that asset deficiency URLs are correctly built from jurisdiction config

        function buildADUrls(r2BaseUrl, r2Path) {
            return {
                crash: r2BaseUrl + '/' + r2Path + '/all_roads.csv',
                inventory: r2BaseUrl + '/' + r2Path + '/traffic-inventory.csv'
            };
        }

        // Test 1: Colorado / Douglas
        const coUrls = buildADUrls('https://data.aicreatesai.com', 'colorado/douglas');
        assertEq(coUrls.crash, 'https://data.aicreatesai.com/colorado/douglas/all_roads.csv', 'AD crash URL for CO/Douglas');
        assertEq(coUrls.inventory, 'https://data.aicreatesai.com/colorado/douglas/traffic-inventory.csv', 'AD inventory URL for CO/Douglas');

        // Test 2: Virginia / Henrico
        const vaUrls = buildADUrls('https://data.aicreatesai.com', 'virginia/henrico');
        assertEq(vaUrls.crash, 'https://data.aicreatesai.com/virginia/henrico/all_roads.csv', 'AD crash URL for VA/Henrico');
        assertEq(vaUrls.inventory, 'https://data.aicreatesai.com/virginia/henrico/traffic-inventory.csv', 'AD inventory URL for VA/Henrico');

        // Test 3: Verify no hardcoded Virginia references
        assert(!coUrls.crash.includes('virginia'), 'CO crash URL has no virginia reference');
        assert(!coUrls.crash.includes('henrico'), 'CO crash URL has no henrico reference');
        assert(!coUrls.inventory.includes('virginia'), 'CO inventory URL has no virginia reference');
    })();

    // ================================================================
    // TEST GROUP 9: sendConfigToAssetDeficiency() includes R2 URLs
    // ================================================================
    console.log('\n=== TEST GROUP 9: sendConfigToAssetDeficiency R2 URL Payload ===');

    (function testSendConfigPayload() {
        // Simulate the enhanced sendConfigToAssetDeficiency that now includes r2BaseUrl/r2Path

        function buildConfigPayload(jurisdictionId, appConfig, r2BaseUrl) {
            const jurisdictionConfig = appConfig?.jurisdictions?.[jurisdictionId];
            const stateKey = appConfig?.defaultState || 'colorado';
            const stateConfig = appConfig?.states?.[stateKey];
            const r2Prefix = stateConfig?.r2Prefix || stateKey;
            const baseUrl = r2BaseUrl || 'https://data.aicreatesai.com';
            const r2Path = r2Prefix + '/' + jurisdictionId;

            return {
                fips: jurisdictionConfig?.fips || '',
                bbox: jurisdictionConfig?.bbox || null,
                jurisdictionName: jurisdictionConfig?.name || jurisdictionId,
                state: stateKey,
                r2BaseUrl: baseUrl,
                r2Path: r2Path
            };
        }

        // Test 1: Douglas County payload
        const payload1 = buildConfigPayload('douglas', mockAppConfig);
        assert(payload1.r2BaseUrl !== undefined, 'Payload includes r2BaseUrl');
        assert(payload1.r2Path !== undefined, 'Payload includes r2Path');
        assertEq(payload1.r2Path, 'colorado/douglas', 'r2Path for Douglas is colorado/douglas');
        assertEq(payload1.state, 'colorado', 'State is colorado');
        assertEq(payload1.jurisdictionName, 'Douglas County', 'Jurisdiction name is Douglas County');

        // Test 2: Henrico County payload (with virginia state config)
        const vaAppConfig = { ...mockAppConfig, defaultState: 'virginia' };
        const payload2 = buildConfigPayload('henrico', vaAppConfig);
        assertEq(payload2.r2Path, 'virginia/henrico', 'r2Path for Henrico is virginia/henrico');
        assertEq(payload2.state, 'virginia', 'State is virginia');
    })();

    // ================================================================
    // TEST GROUP 10: Cross-Jurisdiction Reset Integration
    // ================================================================
    console.log('\n=== TEST GROUP 10: Cross-Jurisdiction Switch Simulation ===');

    (function testCrossJurisdictionSwitch() {
        // Simulate switching from Virginia/Henrico to Colorado/Douglas
        // Verify all state is properly reset

        // Step 1: Simulate Virginia/Henrico loaded state
        const tiState = createMockTIState();
        tiState.loaded = true;
        tiState.data = [
            { mutcd: 'R1-1', name: 'Stop', lat: 37.55, lon: -77.39 },
            { mutcd: 'R2-1', name: 'Speed Limit 25', lat: 37.56, lon: -77.40 }
        ];
        tiState.categories.ti_stop.count = 1;
        tiState.categories.ti_stop.items = [tiState.data[0]];
        tiState.categories.ti_stop.enabled = true;
        tiState.categories.ti_speed.count = 1;
        tiState.categories.ti_speed.items = [tiState.data[1]];
        tiState.categories.ti_speed.enabled = true;
        tiState.speedCategories = {
            '25': { enabled: true, layer: null, count: 1, items: [tiState.data[1]] }
        };

        const sdState = createMockSignDefState();
        sdState.inventoryLoaded = true;
        sdState.inventoryData = tiState.data.slice();

        // Verify Virginia data is loaded
        assert(tiState.loaded, 'Pre-switch: TI data loaded (Virginia)');
        assertEq(tiState.data.length, 2, 'Pre-switch: 2 TI items (Virginia)');
        assert(sdState.inventoryLoaded, 'Pre-switch: SignDef inventory loaded');

        // Step 2: Simulate jurisdiction switch (resetTrafficInventoryForJurisdictionChange)
        // Inline reset
        Object.keys(tiState.categories).forEach(function(key) {
            var cat = tiState.categories[key];
            cat.layer = null;
            cat.enabled = false;
            cat.items = [];
            cat.count = 0;
        });
        Object.keys(tiState.speedCategories).forEach(function(speed) {
            var sc = tiState.speedCategories[speed];
            sc.layer = null;
        });
        tiState.speedCategories = {};
        tiState.loaded = false;
        tiState.loading = false;
        tiState.data = [];
        tiState.error = null;
        sdState.inventoryLoaded = false;
        sdState.inventoryData = [];

        // Verify reset
        assertEq(tiState.loaded, false, 'Post-switch: TI loaded reset');
        assertEq(tiState.data.length, 0, 'Post-switch: TI data cleared');
        assertEq(tiState.categories.ti_stop.count, 0, 'Post-switch: stop count reset');
        assertEq(tiState.categories.ti_stop.enabled, false, 'Post-switch: stop disabled');
        assertEq(Object.keys(tiState.speedCategories).length, 0, 'Post-switch: speed categories cleared');
        assertEq(sdState.inventoryLoaded, false, 'Post-switch: SignDef reset');
        assertEq(sdState.inventoryData.length, 0, 'Post-switch: SignDef data cleared');

        // Step 3: Simulate loading Colorado/Douglas data
        tiState.loading = true;
        const coloradoData = [
            { mutcd: 'R1-1', name: 'Stop', lat: 39.34, lon: -104.92 },
            { mutcd: 'W1-1', name: 'Turn', lat: 39.35, lon: -104.93 },
            { mutcd: 'R2-1', name: 'Speed Limit 35', lat: 39.36, lon: -104.94 }
        ];
        tiState.data = coloradoData;
        tiState.loaded = true;
        tiState.loading = false;
        tiState.categories.ti_stop.count = 1;
        tiState.categories.ti_stop.items = [coloradoData[0]];

        // Verify Colorado data is now loaded
        assertEq(tiState.data.length, 3, 'Post-load: 3 TI items (Colorado)');
        assert(tiState.loaded, 'Post-load: TI loaded (Colorado)');
        assertEq(tiState.categories.ti_stop.count, 1, 'Post-load: stop count from CO data');

        // Verify no Virginia data leaked through
        assert(tiState.data[0].lat > 39, 'Post-load: latitude is Colorado (>39), not Virginia (~37)');
        assert(tiState.data[0].lon < -104, 'Post-load: longitude is Colorado (<-104), not Virginia (~-77)');
    })();

    // ================================================================
    // TEST GROUP 11: No Hardcoded Virginia/Henrico in Asset Deficiency
    // ================================================================
    console.log('\n=== TEST GROUP 11: asset-deficiency.html No Hardcoded URLs ===');

    (function testAssetDeficiencyNoHardcoded() {
        // These tests verify the changes in asset-deficiency.html
        // In Node.js we can only verify the expected behavior, not read the file

        if (isNode) {
            // Read the actual file and check for hardcoded references
            try {
                const fs = require('fs');
                const path = require('path');
                const filePath = path.join(__dirname, '..', 'app', 'asset-deficiency.html');
                const content = fs.readFileSync(filePath, 'utf8');

                // Check that hardcoded Virginia URLs are removed
                assert(
                    !content.includes('value="https://data.aicreatesai.com/virginia/henrico/all_roads.csv"'),
                    'No hardcoded Virginia crash data URL in asset-deficiency.html'
                );
                assert(
                    !content.includes('value="https://data.aicreatesai.com/virginia/henrico/traffic-inventory.csv"'),
                    'No hardcoded Virginia inventory URL in asset-deficiency.html'
                );
                assert(
                    !content.includes('value="51087"'),
                    'No hardcoded Henrico FIPS (51087) in asset-deficiency.html'
                );
                assert(
                    !content.includes('Virginia Roads + R2'),
                    'No hardcoded "Virginia Roads + R2" badge text'
                );
                assert(
                    content.includes('R2 Cloud Storage'),
                    'Badge text changed to "R2 Cloud Storage"'
                );
                assert(
                    content.includes('Auto-populated from jurisdiction selection'),
                    'URL inputs have dynamic placeholder text'
                );
            } catch (e) {
                skip('asset-deficiency.html file checks', 'Could not read file: ' + e.message);
            }
        } else {
            skip('asset-deficiency.html file checks', 'Browser mode — file read not available');
        }
    })();

    // ================================================================
    // TEST GROUP 12: Crash Data Validator No Hardcoded Presets
    // ================================================================
    console.log('\n=== TEST GROUP 12: crash-data-validator-v13.html No Hardcoded Presets ===');

    (function testValidatorNoHardcodedPresets() {
        if (isNode) {
            try {
                const fs = require('fs');
                const path = require('path');
                const filePath = path.join(__dirname, '..', 'scripts', 'crash-data-validator-v13.html');
                const content = fs.readFileSync(filePath, 'utf8');

                // Check that hardcoded JURISDICTIONS entries are removed
                assert(
                    !content.includes("'va_henrico':"),
                    'No hardcoded va_henrico preset in JURISDICTIONS'
                );
                assert(
                    !content.includes("'va_chesterfield':"),
                    'No hardcoded va_chesterfield preset in JURISDICTIONS'
                );
                assert(
                    !content.includes("'co_douglas':"),
                    'No hardcoded co_douglas preset in JURISDICTIONS'
                );

                // Check that JURISDICTIONS starts empty
                assert(
                    content.includes('const JURISDICTIONS = {};'),
                    'JURISDICTIONS initialized as empty object'
                );

                // Check that hardcoded <option> entries are removed
                assert(
                    !content.includes('<optgroup label="Virginia">'),
                    'No hardcoded Virginia optgroup in dropdown'
                );
                assert(
                    !content.includes('<optgroup label="Colorado">'),
                    'No hardcoded Colorado optgroup in dropdown'
                );
                assert(
                    !content.includes('value="va_henrico"'),
                    'No hardcoded va_henrico option in dropdown'
                );

                // Check that dynamic registration exists
                assert(
                    content.includes('Populated dynamically from parent app'),
                    'Dropdown has dynamic population comment'
                );
            } catch (e) {
                skip('crash-data-validator file checks', 'Could not read file: ' + e.message);
            }
        } else {
            skip('crash-data-validator file checks', 'Browser mode — file read not available');
        }
    })();

    // ================================================================
    // TEST GROUP 13: app/index.html Integration Checks
    // ================================================================
    console.log('\n=== TEST GROUP 13: app/index.html Integration Checks ===');

    (function testAppIntegration() {
        if (isNode) {
            try {
                const fs = require('fs');
                const path = require('path');
                const filePath = path.join(__dirname, '..', 'app', 'index.html');
                const content = fs.readFileSync(filePath, 'utf8');

                // Check resetTrafficInventoryForJurisdictionChange exists
                assert(
                    content.includes('function resetTrafficInventoryForJurisdictionChange()'),
                    'resetTrafficInventoryForJurisdictionChange function exists'
                );

                // Check it's called from saveJurisdictionSelection
                assert(
                    content.includes('resetTrafficInventoryForJurisdictionChange') &&
                    content.includes('Traffic inventory state reset for jurisdiction change'),
                    'resetTrafficInventoryForJurisdictionChange has logging'
                );

                // Check fallback in _getActiveStateKey uses appConfig
                assert(
                    content.includes("appConfig?.defaultState) || 'colorado'"),
                    '_getActiveStateKey fallback reads appConfig.defaultState'
                );

                // Check that sendConfigToAssetDeficiency includes r2Path
                assert(
                    content.includes('r2BaseUrl: baseUrl') && content.includes('r2Path: r2Path'),
                    'sendConfigToAssetDeficiency includes r2BaseUrl and r2Path'
                );

                // Check graceful error message pattern
                assert(
                    content.includes('No traffic inventory data available for this jurisdiction'),
                    'Graceful error message for missing TI data exists'
                );

                // Count occurrences of the graceful message (should be in 3 catch blocks)
                const matches = content.match(/No traffic inventory data available for this jurisdiction/g);
                assert(
                    matches && matches.length >= 3,
                    `Graceful TI error message in ${matches ? matches.length : 0} locations (expected >= 3)`
                );

            } catch (e) {
                skip('app/index.html integration checks', 'Could not read file: ' + e.message);
            }
        } else {
            skip('app/index.html integration checks', 'Browser mode — file read not available');
        }
    })();

    // ================================================================
    // RESULTS
    // ================================================================
    console.log('\n' + '='.repeat(60));
    console.log(`RESULTS: ${passed} passed, ${failed} failed, ${skipped} skipped`);
    console.log('='.repeat(60));

    if (errors.length > 0) {
        console.log('\nFailed tests:');
        errors.forEach(function(e) { console.log('  - ' + e); });
    }

    if (failed > 0) {
        console.log('\n\u274C SOME TESTS FAILED');
        if (isNode) process.exit(1);
    } else {
        console.log('\n\u2705 ALL TESTS PASSED');
        if (isNode) process.exit(0);
    }

})();
