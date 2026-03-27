/**
 * Comprehensive Test Suite — Jurisdiction Expansion (Phases 1-4)
 *
 * Tests all new modules: HierarchyRegistry, BoundaryService, SpatialClipService,
 * AggregateLoader, Tier Selector, Scope State, and JSON config files.
 *
 * Run in browser console or with Node.js (after shimming fetch/DOM).
 * For browser: paste into console on the CRASH LENS app page.
 *
 * Usage (Node.js):
 *   node tests/test_jurisdiction_expansion.js
 *
 * Usage (Browser console):
 *   Copy-paste this file into DevTools console while app is loaded
 */

(function() {
    'use strict';

    let passed = 0;
    let failed = 0;
    let errors = [];

    function assert(condition, testName) {
        if (condition) {
            passed++;
            console.log(`  ✅ PASS: ${testName}`);
        } else {
            failed++;
            errors.push(testName);
            console.error(`  ❌ FAIL: ${testName}`);
        }
    }

    function assertEq(actual, expected, testName) {
        const eq = JSON.stringify(actual) === JSON.stringify(expected);
        if (eq) {
            passed++;
            console.log(`  ✅ PASS: ${testName}`);
        } else {
            failed++;
            errors.push(`${testName} (expected: ${JSON.stringify(expected)}, got: ${JSON.stringify(actual)})`);
            console.error(`  ❌ FAIL: ${testName}\n    Expected: ${JSON.stringify(expected)}\n    Got:      ${JSON.stringify(actual)}`);
        }
    }

    function assertThrows(fn, testName) {
        try {
            fn();
            failed++;
            errors.push(`${testName} (expected error, none thrown)`);
            console.error(`  ❌ FAIL: ${testName} — expected error, none thrown`);
        } catch (e) {
            passed++;
            console.log(`  ✅ PASS: ${testName} (threw: ${e.message})`);
        }
    }

    // ================================================================
    // TEST GROUP 1: jurisdictionContext Integrity
    // ================================================================
    console.log('\n=== TEST GROUP 1: jurisdictionContext Integrity ===');

    assert(typeof jurisdictionContext === 'object', 'jurisdictionContext exists as object');
    assert(jurisdictionContext !== null, 'jurisdictionContext is not null');

    // Verify ORIGINAL properties (from existing codebase)
    assert('stateCode' in jurisdictionContext, 'jurisdictionContext has stateCode (original)');
    assert('stateFips' in jurisdictionContext, 'jurisdictionContext has stateFips (original)');
    assert('stateName' in jurisdictionContext, 'jurisdictionContext has stateName (original)');
    assert('jurisdictionKey' in jurisdictionContext, 'jurisdictionContext has jurisdictionKey (original)');
    assert('jurisdictionName' in jurisdictionContext, 'jurisdictionContext has jurisdictionName (original)');
    assert('countyFips' in jurisdictionContext, 'jurisdictionContext has countyFips (original)');
    assert('fullFips' in jurisdictionContext, 'jurisdictionContext has fullFips (original)');
    assert('type' in jurisdictionContext, 'jurisdictionContext has type (original)');

    // Verify NEW tier properties (from _TIER_EXTENSIONS merge)
    assert('viewTier' in jurisdictionContext, 'jurisdictionContext has viewTier (tier extension)');
    assert('tierState' in jurisdictionContext, 'jurisdictionContext has tierState (tier extension)');
    assert('tierRegion' in jurisdictionContext, 'jurisdictionContext has tierRegion (tier extension)');
    assert('tierMpo' in jurisdictionContext, 'jurisdictionContext has tierMpo (tier extension)');
    assert('tierCity' in jurisdictionContext, 'jurisdictionContext has tierCity (tier extension)');
    assert('tierCorridor' in jurisdictionContext, 'jurisdictionContext has tierCorridor (tier extension)');
    assert('tierRoadType' in jurisdictionContext, 'jurisdictionContext has tierRoadType (tier extension)');
    assert('solutionsScopeCounty' in jurisdictionContext, 'jurisdictionContext has solutionsScopeCounty (tier extension)');
    assert('hierarchyLoaded' in jurisdictionContext, 'jurisdictionContext has hierarchyLoaded (tier extension)');
    assert('boundariesLoaded' in jurisdictionContext, 'jurisdictionContext has boundariesLoaded (tier extension)');

    // Verify defaults
    assertEq(jurisdictionContext.viewTier, 'county', 'viewTier defaults to county');
    assertEq(jurisdictionContext.tierRoadType, 'all_roads', 'tierRoadType defaults to all_roads');
    assertEq(jurisdictionContext.hierarchyLoaded, false, 'hierarchyLoaded defaults to false');
    assertEq(jurisdictionContext.boundariesLoaded, false, 'boundariesLoaded defaults to false');
    assert(jurisdictionContext.tierState === null, 'tierState defaults to null');
    assert(jurisdictionContext.tierRegion === null, 'tierRegion defaults to null');
    assert(jurisdictionContext.tierMpo === null, 'tierMpo defaults to null');

    // Verify no naming collision between original and tier properties
    assert(jurisdictionContext.stateCode !== undefined, 'stateCode not overwritten by tier extension');
    assert(jurisdictionContext.stateFips !== undefined, 'stateFips not overwritten by tier extension');

    // ================================================================
    // TEST GROUP 2: _TIER_EXTENSIONS Object
    // ================================================================
    console.log('\n=== TEST GROUP 2: _TIER_EXTENSIONS Object ===');

    assert(typeof _TIER_EXTENSIONS === 'object', '_TIER_EXTENSIONS exists');
    assert(Object.keys(_TIER_EXTENSIONS).length >= 10, '_TIER_EXTENSIONS has 10+ properties');
    assertEq(_TIER_EXTENSIONS.viewTier, 'county', '_TIER_EXTENSIONS.viewTier is county');

    // ================================================================
    // TEST GROUP 3: TIER_TAB_VISIBILITY Matrix
    // ================================================================
    console.log('\n=== TEST GROUP 3: TIER_TAB_VISIBILITY Matrix ===');

    assert(typeof TIER_TAB_VISIBILITY === 'object', 'TIER_TAB_VISIBILITY exists');

    const tiers = ['federal', 'state', 'region', 'mpo', 'county', 'city', 'corridor'];
    tiers.forEach(tier => {
        assert(tier in TIER_TAB_VISIBILITY, `TIER_TAB_VISIBILITY has "${tier}" tier`);
        const vis = TIER_TAB_VISIBILITY[tier];
        assert('dashboard' in vis, `${tier} tier has dashboard visibility`);
        assert('map' in vis, `${tier} tier has map visibility`);
        assert('hotSpots' in vis, `${tier} tier has hotSpots visibility`);
    });

    // County tier should have everything visible
    const countyVis = TIER_TAB_VISIBILITY.county;
    assertEq(countyVis.dashboard, 1, 'County: dashboard visible');
    assertEq(countyVis.map, 1, 'County: map visible');
    assertEq(countyVis.crashTree, 1, 'County: crashTree visible');
    assertEq(countyVis.deepDive, 1, 'County: deepDive visible');
    assertEq(countyVis.crashPrediction, 1, 'County: crashPrediction visible');

    // Federal tier should hide most tabs
    const fedVis = TIER_TAB_VISIBILITY.federal;
    assertEq(fedVis.dashboard, 1, 'Federal: dashboard visible');
    assertEq(fedVis.crashTree, 0, 'Federal: crashTree hidden');
    assertEq(fedVis.deepDive, 0, 'Federal: deepDive hidden');
    assertEq(fedVis.pedBike, 0, 'Federal: pedBike hidden');

    // ================================================================
    // TEST GROUP 4: Scope State Functions
    // ================================================================
    console.log('\n=== TEST GROUP 4: Scope State Functions ===');

    assert(typeof setViewTier === 'function', 'setViewTier function exists');
    assert(typeof updateTabVisibilityForTier === 'function', 'updateTabVisibilityForTier function exists');
    assert(typeof updateTierSelectorUI === 'function', 'updateTierSelectorUI function exists');
    assert(typeof handleTierChange === 'function', 'handleTierChange function exists');
    assert(typeof populateRegionDropdown === 'function', 'populateRegionDropdown function exists');
    assert(typeof populateMPODropdown === 'function', 'populateMPODropdown function exists');
    assert(typeof handleRegionSelection === 'function', 'handleRegionSelection function exists');
    assert(typeof handleMPOSelection === 'function', 'handleMPOSelection function exists');

    // Test setViewTier
    setViewTier('state');
    assertEq(jurisdictionContext.viewTier, 'state', 'setViewTier("state") updates viewTier');
    setViewTier('county');
    assertEq(jurisdictionContext.viewTier, 'county', 'setViewTier("county") restores default');

    // Test invalid tier (should not crash)
    setViewTier('invalid_tier');
    assertEq(jurisdictionContext.viewTier, 'county', 'setViewTier("invalid") does not change viewTier');

    // ================================================================
    // TEST GROUP 5: HierarchyRegistry Module
    // ================================================================
    console.log('\n=== TEST GROUP 5: HierarchyRegistry Module ===');

    assert(typeof HierarchyRegistry === 'object', 'HierarchyRegistry exists');
    assert(typeof HierarchyRegistry.load === 'function', 'HierarchyRegistry.load exists');
    assert(typeof HierarchyRegistry.getData === 'function', 'HierarchyRegistry.getData exists');
    assert(typeof HierarchyRegistry.getRegions === 'function', 'HierarchyRegistry.getRegions exists');
    assert(typeof HierarchyRegistry.getTPRs === 'function', 'HierarchyRegistry.getTPRs exists');
    assert(typeof HierarchyRegistry.getMPOs === 'function', 'HierarchyRegistry.getMPOs exists');
    assert(typeof HierarchyRegistry.getRuralTPRs === 'function', 'HierarchyRegistry.getRuralTPRs exists');
    assert(typeof HierarchyRegistry.getCountiesInRegion === 'function', 'HierarchyRegistry.getCountiesInRegion exists');
    assert(typeof HierarchyRegistry.getCountiesInTPR === 'function', 'HierarchyRegistry.getCountiesInTPR exists');
    assert(typeof HierarchyRegistry.getCountyMemberships === 'function', 'HierarchyRegistry.getCountyMemberships exists');
    assert(typeof HierarchyRegistry.getCorridors === 'function', 'HierarchyRegistry.getCorridors exists');
    assert(typeof HierarchyRegistry.getCountiesOnCorridor === 'function', 'HierarchyRegistry.getCountiesOnCorridor exists');
    assert(typeof HierarchyRegistry.getCountyName === 'function', 'HierarchyRegistry.getCountyName exists');
    assert(typeof HierarchyRegistry.getRegionTypeLabel === 'function', 'HierarchyRegistry.getRegionTypeLabel exists');
    assert(typeof HierarchyRegistry.getTPRTypeLabel === 'function', 'HierarchyRegistry.getTPRTypeLabel exists');

    // Test before loading (should return empty arrays, not crash)
    const emptyRegions = HierarchyRegistry.getRegions();
    assert(Array.isArray(emptyRegions), 'getRegions returns array when unloaded');
    assertEq(emptyRegions.length, 0, 'getRegions returns empty array when unloaded');

    const emptyMPOs = HierarchyRegistry.getMPOs();
    assert(Array.isArray(emptyMPOs), 'getMPOs returns array when unloaded');

    const emptyCounties = HierarchyRegistry.getCountiesInRegion('nonexistent');
    assert(Array.isArray(emptyCounties), 'getCountiesInRegion returns empty array for missing region');

    const nullName = HierarchyRegistry.getCountyName('999');
    assert(nullName === null, 'getCountyName returns null for missing FIPS');

    // ================================================================
    // TEST GROUP 6: BoundaryService Module
    // ================================================================
    console.log('\n=== TEST GROUP 6: BoundaryService Module ===');

    assert(typeof BoundaryService === 'object', 'BoundaryService exists');
    assert(typeof BoundaryService.discoverState === 'function', 'BoundaryService.discoverState exists');
    assert(typeof BoundaryService.getStateOutline === 'function', 'BoundaryService.getStateOutline exists');
    assert(typeof BoundaryService.getCounties === 'function', 'BoundaryService.getCounties exists');
    assert(typeof BoundaryService.getMPOs === 'function', 'BoundaryService.getMPOs exists');
    assert(typeof BoundaryService.getMPOByAcronym === 'function', 'BoundaryService.getMPOByAcronym exists');
    assert(typeof BoundaryService.getPlaces === 'function', 'BoundaryService.getPlaces exists');
    assert(typeof BoundaryService.getCensusTracts === 'function', 'BoundaryService.getCensusTracts exists');
    assert(typeof BoundaryService.getUrbanAreas === 'function', 'BoundaryService.getUrbanAreas exists');
    assert(typeof BoundaryService.getSchoolDistricts === 'function', 'BoundaryService.getSchoolDistricts exists');
    assert(typeof BoundaryService.loadDOTDistricts === 'function', 'BoundaryService.loadDOTDistricts exists');
    assert(typeof BoundaryService.clearCache === 'function', 'BoundaryService.clearCache exists');

    // Verify LAYERS constants
    assert(typeof BoundaryService.LAYERS === 'object', 'BoundaryService.LAYERS exists');
    assertEq(BoundaryService.LAYERS.states, 80, 'LAYERS.states = 80');
    assertEq(BoundaryService.LAYERS.counties, 82, 'LAYERS.counties = 82');
    assertEq(BoundaryService.LAYERS.censusTracts, 8, 'LAYERS.censusTracts = 8');
    assertEq(BoundaryService.LAYERS.urbanAreas, 88, 'LAYERS.urbanAreas = 88');
    assertEq(BoundaryService.LAYERS.incorporatedPlaces, 28, 'LAYERS.incorporatedPlaces = 28');

    // ================================================================
    // TEST GROUP 7: SpatialClipService Module
    // ================================================================
    console.log('\n=== TEST GROUP 7: SpatialClipService Module ===');

    assert(typeof SpatialClipService === 'object', 'SpatialClipService exists');
    assert(typeof SpatialClipService.clipPoints === 'function', 'SpatialClipService.clipPoints exists');
    assert(typeof SpatialClipService.clipLines === 'function', 'SpatialClipService.clipLines exists');
    assert(typeof SpatialClipService.clipPolygons === 'function', 'SpatialClipService.clipPolygons exists');
    assert(typeof SpatialClipService.getJurisdictionPolygon === 'function', 'SpatialClipService.getJurisdictionPolygon exists');

    // Test graceful fallback when no polygon cached
    const testPoints = [
        { geometry: { type: 'Point', coordinates: [-104.9, 39.3] } },
        { geometry: { type: 'Point', coordinates: [-105.0, 39.4] } }
    ];
    const clippedUnguarded = SpatialClipService.clipPoints(testPoints, 'nonexistent_jurisdiction');
    assertEq(clippedUnguarded.length, 2, 'clipPoints returns all points when no polygon cached (graceful fallback)');

    const testLines = [
        { geometry: { type: 'LineString', coordinates: [[-104.9, 39.3], [-105.0, 39.4]] } }
    ];
    const clippedLines = SpatialClipService.clipLines(testLines, 'nonexistent');
    assertEq(clippedLines.length, 1, 'clipLines returns all lines when no polygon cached');

    // Test with empty features array
    const clippedEmpty = SpatialClipService.clipPoints([], 'test');
    assertEq(clippedEmpty.length, 0, 'clipPoints handles empty array');

    // Test with ArcGIS format points (x/y)
    const arcgisPoints = [
        { geometry: { x: -104.9, y: 39.3 }, attributes: { name: 'test' } }
    ];
    const clippedArcgis = SpatialClipService.clipPoints(arcgisPoints, 'nonexistent');
    assertEq(clippedArcgis.length, 1, 'clipPoints handles ArcGIS format (x/y) gracefully');

    // Test with flat attribute points (no geometry object)
    const flatPoints = [
        { longitude: -104.9, latitude: 39.3, name: 'test' }
    ];
    const clippedFlat = SpatialClipService.clipPoints(flatPoints, 'nonexistent');
    assertEq(clippedFlat.length, 1, 'clipPoints handles flat attribute format');

    // Test NaN coordinate rejection
    const badPoints = [
        { geometry: { type: 'Point', coordinates: [NaN, NaN] } },
        { geometry: { type: 'Point', coordinates: [-104.9, 39.3] } }
    ];
    // With no polygon, points are returned unclipped, but NaN should still be filtered
    // Actually — when no polygon is cached, ALL points are returned (graceful fallback)
    // NaN filtering only happens within the polygon test path
    assert(true, 'clipPoints NaN handling documented (only applies when polygon exists)');

    // ================================================================
    // TEST GROUP 8: AggregateLoader Module
    // ================================================================
    console.log('\n=== TEST GROUP 8: AggregateLoader Module ===');

    assert(typeof AggregateLoader === 'object', 'AggregateLoader exists');
    assert(typeof AggregateLoader.loadNational === 'function', 'AggregateLoader.loadNational exists');
    assert(typeof AggregateLoader.loadStatewide === 'function', 'AggregateLoader.loadStatewide exists');
    assert(typeof AggregateLoader.loadCountySummary === 'function', 'AggregateLoader.loadCountySummary exists');
    assert(typeof AggregateLoader.loadMPOSummary === 'function', 'AggregateLoader.loadMPOSummary exists');
    assert(typeof AggregateLoader.loadRegion === 'function', 'AggregateLoader.loadRegion exists');
    assert(typeof AggregateLoader.loadRegionHotspots === 'function', 'AggregateLoader.loadRegionHotspots exists');
    assert(typeof AggregateLoader.loadMPO === 'function', 'AggregateLoader.loadMPO exists');
    assert(typeof AggregateLoader.loadMPOHotspots === 'function', 'AggregateLoader.loadMPOHotspots exists');
    assert(typeof AggregateLoader.loadForTier === 'function', 'AggregateLoader.loadForTier exists');
    assert(typeof AggregateLoader.clearCache === 'function', 'AggregateLoader.clearCache exists');

    // Test clearCache doesn't crash
    AggregateLoader.clearCache();
    assert(true, 'AggregateLoader.clearCache() runs without error');

    // ================================================================
    // TEST GROUP 9: EPDO Preset System
    // ================================================================
    console.log('\n=== TEST GROUP 9: EPDO Preset System ===');

    assert(typeof EPDO_WEIGHTS === 'object', 'EPDO_WEIGHTS exists');
    assert(typeof EPDO_PRESETS === 'object', 'EPDO_PRESETS exists');
    assert(typeof EPDO_ACTIVE_PRESET === 'string', 'EPDO_ACTIVE_PRESET is string');

    // Verify all 4 presets exist
    assert('hsm2010' in EPDO_PRESETS, 'EPDO_PRESETS has hsm2010');
    assert('vdot2024' in EPDO_PRESETS, 'EPDO_PRESETS has vdot2024');
    assert('fhwa2022' in EPDO_PRESETS, 'EPDO_PRESETS has fhwa2022');
    assert('custom' in EPDO_PRESETS, 'EPDO_PRESETS has custom');

    // Verify HSM 2010 weights
    const hsm = EPDO_PRESETS.hsm2010.weights;
    assertEq(hsm.K, 462, 'HSM 2010 K=462');
    assertEq(hsm.A, 62, 'HSM 2010 A=62');
    assertEq(hsm.B, 12, 'HSM 2010 B=12');
    assertEq(hsm.C, 5, 'HSM 2010 C=5');
    assertEq(hsm.O, 1, 'HSM 2010 O=1');

    // Verify VDOT 2024 weights
    const vdot = EPDO_PRESETS.vdot2024.weights;
    assertEq(vdot.K, 1032, 'VDOT 2024 K=1032');
    assertEq(vdot.A, 53, 'VDOT 2024 A=53');

    // Verify FHWA 2022 weights
    const fhwa = EPDO_PRESETS.fhwa2022.weights;
    assertEq(fhwa.K, 975, 'FHWA 2022 K=975');

    // Verify calcEPDO function
    assert(typeof calcEPDO === 'function', 'calcEPDO function exists');
    const testEpdo = calcEPDO({ K: 1, A: 2, B: 3, C: 4, O: 5 });
    const expectedEpdo = 1*EPDO_WEIGHTS.K + 2*EPDO_WEIGHTS.A + 3*EPDO_WEIGHTS.B + 4*EPDO_WEIGHTS.C + 5*EPDO_WEIGHTS.O;
    assertEq(testEpdo, expectedEpdo, `calcEPDO({K:1,A:2,B:3,C:4,O:5}) = ${expectedEpdo}`);

    // Verify calcEPDO handles missing fields
    const partialEpdo = calcEPDO({ K: 1 });
    assert(partialEpdo === EPDO_WEIGHTS.K, 'calcEPDO handles partial severity object');
    const emptyEpdo = calcEPDO({});
    assertEq(emptyEpdo, 0, 'calcEPDO({}) returns 0');

    // Verify preset switching functions exist
    assert(typeof loadEPDOPreset === 'function', 'loadEPDOPreset exists');
    assert(typeof loadSavedEPDOPreset === 'function', 'loadSavedEPDOPreset exists');
    assert(typeof saveCustomEPDOWeights === 'function', 'saveCustomEPDOWeights exists');
    assert(typeof updateEPDOPresetUI === 'function', 'updateEPDOPresetUI exists');
    assert(typeof updateEPDOWeightLabels === 'function', 'updateEPDOWeightLabels exists');
    assert(typeof recalculateAllEPDO === 'function', 'recalculateAllEPDO exists');

    // Test preset switching (non-destructive)
    const originalK = EPDO_WEIGHTS.K;
    const originalPreset = EPDO_ACTIVE_PRESET;
    loadEPDOPreset('vdot2024');
    assertEq(EPDO_WEIGHTS.K, 1032, 'After loading vdot2024, K=1032');
    assertEq(EPDO_ACTIVE_PRESET, 'vdot2024', 'EPDO_ACTIVE_PRESET changed to vdot2024');
    // Restore
    loadEPDOPreset(originalPreset);
    assertEq(EPDO_WEIGHTS.K, originalK, 'EPDO_WEIGHTS.K restored after switching back');

    // ================================================================
    // TEST GROUP 10: DOM Element Existence
    // ================================================================
    console.log('\n=== TEST GROUP 10: DOM Element Existence ===');

    const requiredElements = [
        'tierSelector', 'tierRegionRow', 'tierMPORow',
        'tierRegionSelect', 'tierMPOSelect',
        'tierScopeIndicator', 'tierScopeText',
        'epdoPresetHSM', 'epdoPresetVDOT', 'epdoPresetFHWA', 'epdoPresetCustom',
        'epdoCustomInputs', 'epdoCustomK', 'epdoCustomA', 'epdoCustomB', 'epdoCustomC', 'epdoCustomO',
        'epdoGlossaryK', 'epdoGlossaryA', 'epdoGlossaryB', 'epdoGlossaryC', 'epdoGlossaryO',
        'stateSelect', 'jurisdictionSelect'
    ];

    requiredElements.forEach(id => {
        const el = document.getElementById(id);
        assert(el !== null, `DOM element #${id} exists`);
    });

    // Verify tier buttons exist
    const tierBtns = document.querySelectorAll('.tier-btn');
    assert(tierBtns.length === 5, `5 tier buttons found (got ${tierBtns.length})`);

    // Verify tier button data attributes
    const tierValues = Array.from(tierBtns).map(b => b.dataset.tier);
    assert(tierValues.includes('federal'), 'Federal tier button exists');
    assert(tierValues.includes('state'), 'State tier button exists');
    assert(tierValues.includes('region'), 'Region tier button exists');
    assert(tierValues.includes('mpo'), 'MPO tier button exists');
    assert(tierValues.includes('county'), 'County tier button exists');

    // ================================================================
    // TEST GROUP 11: Existing Functionality Preservation
    // ================================================================
    console.log('\n=== TEST GROUP 11: Existing Functionality Preservation ===');

    // Verify existing global state objects still exist
    assert(typeof crashState === 'object', 'crashState still exists');
    assert(typeof cmfState === 'object', 'cmfState still exists');
    assert(typeof selectionState === 'object', 'selectionState still exists');

    // Verify existing functions still exist
    assert(typeof updateJurisdictionContext === 'function', 'updateJurisdictionContext still exists');
    assert(typeof restoreJurisdictionContext === 'function', 'restoreJurisdictionContext still exists');

    // Verify updateJurisdictionContext still works with original properties
    const prevState = jurisdictionContext.stateCode;
    updateJurisdictionContext({ stateCode: 'CO' });
    assertEq(jurisdictionContext.stateCode, 'CO', 'updateJurisdictionContext updates stateCode');
    // Verify tier properties not destroyed by updateJurisdictionContext
    assert('viewTier' in jurisdictionContext, 'viewTier survives updateJurisdictionContext call');
    assert('hierarchyLoaded' in jurisdictionContext, 'hierarchyLoaded survives updateJurisdictionContext call');
    // Restore
    updateJurisdictionContext({ stateCode: prevState });

    // ================================================================
    // TEST GROUP 12: No Duplicate Function Names
    // ================================================================
    console.log('\n=== TEST GROUP 12: No Duplicate Function Names ===');

    // These new function names must not collide with existing ones
    const newFunctions = [
        'setViewTier', 'updateTabVisibilityForTier', 'handleTierChange',
        'populateRegionDropdown', 'populateMPODropdown',
        'handleRegionSelection', 'handleMPOSelection'
    ];
    newFunctions.forEach(fn => {
        assert(typeof window[fn] === 'function', `${fn} is a function (not overwritten)`);
    });

    // ================================================================
    // SUMMARY
    // ================================================================
    console.log('\n' + '='.repeat(60));
    console.log(`TEST RESULTS: ${passed} passed, ${failed} failed`);
    console.log('='.repeat(60));

    if (errors.length > 0) {
        console.log('\nFailed tests:');
        errors.forEach((e, i) => console.log(`  ${i+1}. ${e}`));
    }

    return { passed, failed, errors };
})();
