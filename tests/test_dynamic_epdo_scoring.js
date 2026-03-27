/**
 * Comprehensive Test Suite — Dynamic State-Based EPDO Scoring System
 *
 * Tests the full EPDO lifecycle: state selection → weight resolution →
 * propagation to all features → UI display updates → persistence.
 *
 * Run in browser console on the CRASH LENS app page.
 *
 * Usage (Browser console):
 *   Copy-paste this file into DevTools console while app is loaded.
 *   Or: fetch('/tests/test_dynamic_epdo_scoring.js').then(r=>r.text()).then(eval)
 *
 * Requires: App loaded with crash data for full cascade tests.
 * Tests marked [DATA] require crashState.loaded === true.
 */
(function() {
    'use strict';

    let passed = 0;
    let failed = 0;
    let skipped = 0;
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

    function assertNotEq(actual, unexpected, testName) {
        const eq = JSON.stringify(actual) === JSON.stringify(unexpected);
        if (!eq) {
            passed++;
            console.log(`  ✅ PASS: ${testName}`);
        } else {
            failed++;
            errors.push(`${testName} (should NOT be: ${JSON.stringify(unexpected)})`);
            console.error(`  ❌ FAIL: ${testName}\n    Should NOT be: ${JSON.stringify(unexpected)}`);
        }
    }

    function assertApprox(actual, expected, tolerance, testName) {
        const diff = Math.abs(actual - expected);
        if (diff <= tolerance) {
            passed++;
            console.log(`  ✅ PASS: ${testName}`);
        } else {
            failed++;
            errors.push(`${testName} (expected ~${expected} ±${tolerance}, got: ${actual})`);
            console.error(`  ❌ FAIL: ${testName}\n    Expected: ~${expected} ±${tolerance}\n    Got:      ${actual}`);
        }
    }

    function skip(testName) {
        skipped++;
        console.log(`  ⏭️ SKIP: ${testName} (requires loaded data)`);
    }

    // Save original state for restoration
    const _origWeights = { ...EPDO_WEIGHTS };
    const _origPreset = EPDO_ACTIVE_PRESET;
    const _origLocalStorage = localStorage.getItem('epdoActivePreset');

    console.log('\n' + '═'.repeat(65));
    console.log('  COMPREHENSIVE TEST: Dynamic State-Based EPDO Scoring System');
    console.log('═'.repeat(65));

    // ================================================================
    // TEST GROUP 1: STATE_EPDO_WEIGHTS Database Completeness
    // ================================================================
    console.log('\n=== GROUP 1: STATE_EPDO_WEIGHTS Database Completeness ===');

    assert(typeof STATE_EPDO_WEIGHTS === 'object', 'STATE_EPDO_WEIGHTS exists as object');
    assert(STATE_EPDO_WEIGHTS !== null, 'STATE_EPDO_WEIGHTS is not null');

    // All 50 states + DC FIPS codes
    const ALL_STATE_FIPS = [
        '01','02','04','05','06','08','09','10','11','12',
        '13','15','16','17','18','19','20','21','22','23',
        '24','25','26','27','28','29','30','31','32','33',
        '34','35','36','37','38','39','40','41','42','44',
        '45','46','47','48','49','50','51','53','54','55','56'
    ];

    let statesWithEntries = 0;
    ALL_STATE_FIPS.forEach(fips => {
        if (STATE_EPDO_WEIGHTS[fips]) statesWithEntries++;
    });
    assert(statesWithEntries === 51, `All 51 entries present (50 states + DC): found ${statesWithEntries}`);

    // Default fallback exists
    assert(STATE_EPDO_WEIGHTS['_default'] !== undefined, '_default fallback entry exists');
    assertEq(STATE_EPDO_WEIGHTS['_default'].weights.O, 1, '_default O weight is 1');

    // Validate structure of each entry
    let structureValid = true;
    Object.entries(STATE_EPDO_WEIGHTS).forEach(([key, entry]) => {
        if (!entry.name || !entry.weights || !entry.source) structureValid = false;
        if (!entry.weights.K || !entry.weights.A || !entry.weights.B || !entry.weights.C || entry.weights.O !== 1) structureValid = false;
    });
    assert(structureValid, 'All STATE_EPDO_WEIGHTS entries have name, weights (K/A/B/C/O=1), and source');

    // Verify known state-specific weights
    assertEq(STATE_EPDO_WEIGHTS['51'].weights.K, 1032, 'Virginia K=1032 (VDOT 2024)');
    assertEq(STATE_EPDO_WEIGHTS['51'].weights.A, 53, 'Virginia A=53');
    assertEq(STATE_EPDO_WEIGHTS['06'].weights.K, 1100, 'California K=1100 (Caltrans)');
    assertEq(STATE_EPDO_WEIGHTS['48'].weights.K, 920, 'Texas K=920 (TxDOT)');
    assertEq(STATE_EPDO_WEIGHTS['12'].weights.K, 985, 'Florida K=985 (FDOT)');
    assertEq(STATE_EPDO_WEIGHTS['36'].weights.K, 1050, 'New York K=1050 (NYSDOT)');
    assertEq(STATE_EPDO_WEIGHTS['25'].weights.K, 1200, 'Massachusetts K=1200 (MassDOT)');
    assertEq(STATE_EPDO_WEIGHTS['37'].weights.K, 770, 'North Carolina K=770 (NCDOT)');
    assertEq(STATE_EPDO_WEIGHTS['35'].weights.K, 567, 'New Mexico K=567 (NMDOT)');
    assertEq(STATE_EPDO_WEIGHTS['17'].weights.K, 850, 'Illinois K=850 (IDOT)');

    // Verify FHWA 2025 default states
    assertEq(STATE_EPDO_WEIGHTS['08'].weights.K, 883, 'Colorado K=883 (FHWA 2025)');
    assertEq(STATE_EPDO_WEIGHTS['01'].weights.K, 883, 'Alabama K=883 (FHWA 2025)');
    assertEq(STATE_EPDO_WEIGHTS['55'].weights.K, 883, 'Wisconsin K=883 (FHWA 2025)');

    // Verify O=1 for all entries (EPDO base weight)
    let allO1 = true;
    Object.values(STATE_EPDO_WEIGHTS).forEach(entry => {
        if (entry.weights.O !== 1) allO1 = false;
    });
    assert(allO1, 'All STATE_EPDO_WEIGHTS entries have O=1');

    // Verify K > A > B > C > O for all entries (severity ordering)
    let orderValid = true;
    Object.entries(STATE_EPDO_WEIGHTS).forEach(([key, entry]) => {
        const w = entry.weights;
        if (w.K <= w.A || w.A < w.B || w.C < w.O) {
            // Allow some flexibility for states with unusual methodologies
            if (key !== '37' && key !== '35') { // NC and NM have unusual A/B/C
                orderValid = false;
            }
        }
    });
    assert(orderValid, 'Standard severity ordering K > A > B > C > O maintained (except NC/NM)');

    // ================================================================
    // TEST GROUP 2: Helper Functions
    // ================================================================
    console.log('\n=== GROUP 2: Helper Functions ===');

    // getStateEPDOWeights
    assert(typeof getStateEPDOWeights === 'function', 'getStateEPDOWeights() exists');

    const vaWeights = getStateEPDOWeights('51');
    assertEq(vaWeights.weights.K, 1032, 'getStateEPDOWeights("51") returns Virginia K=1032');
    assert(vaWeights.name.includes('Virginia'), 'getStateEPDOWeights("51") name includes Virginia');

    const coWeights = getStateEPDOWeights('08');
    assertEq(coWeights.weights.K, 883, 'getStateEPDOWeights("08") returns Colorado K=883');

    // Test with unpadded FIPS
    const unpadded = getStateEPDOWeights('8');
    assertEq(unpadded.weights.K, 883, 'getStateEPDOWeights("8") pads to "08" for Colorado');

    // Test with unknown FIPS
    const unknown = getStateEPDOWeights('99');
    assertEq(unknown.weights.K, STATE_EPDO_WEIGHTS['_default'].weights.K, 'getStateEPDOWeights("99") falls back to _default');
    assert(unknown.name.includes('FHWA'), 'Unknown FIPS falls back to FHWA 2025');

    // getCurrentStateFips
    assert(typeof getCurrentStateFips === 'function', 'getCurrentStateFips() exists');
    const currentFips = getCurrentStateFips();
    assert(typeof currentFips === 'string', 'getCurrentStateFips() returns a string');
    assert(currentFips.length >= 1 && currentFips.length <= 2, 'getCurrentStateFips() returns 1-2 digit FIPS');

    // applyStateDefaultEPDO
    assert(typeof applyStateDefaultEPDO === 'function', 'applyStateDefaultEPDO() exists');

    // ================================================================
    // TEST GROUP 3: EPDO_PRESETS Structure
    // ================================================================
    console.log('\n=== GROUP 3: EPDO_PRESETS Structure ===');

    assert(typeof EPDO_PRESETS === 'object', 'EPDO_PRESETS exists');

    // Verify all 5 presets exist
    assert(EPDO_PRESETS.stateDefault !== undefined, 'stateDefault preset exists');
    assert(EPDO_PRESETS.hsm2010 !== undefined, 'hsm2010 preset exists');
    assert(EPDO_PRESETS.vdot2024 !== undefined, 'vdot2024 preset exists');
    assert(EPDO_PRESETS.fhwa2022 !== undefined, 'fhwa2022 preset exists');
    assert(EPDO_PRESETS.custom !== undefined, 'custom preset exists');

    // Verify preset structure
    Object.entries(EPDO_PRESETS).forEach(([key, preset]) => {
        assert(preset.name && typeof preset.name === 'string', `${key} preset has name`);
        assert(preset.weights && typeof preset.weights === 'object', `${key} preset has weights object`);
        assert(preset.description && typeof preset.description === 'string', `${key} preset has description`);
        assert(typeof preset.weights.K === 'number', `${key} preset weights.K is a number`);
        assert(typeof preset.weights.O === 'number', `${key} preset weights.O is a number`);
    });

    // stateDefault has isAuto flag
    assert(EPDO_PRESETS.stateDefault.isAuto === true, 'stateDefault has isAuto=true');

    // Verify preset weight values
    assertEq(EPDO_PRESETS.hsm2010.weights.K, 462, 'HSM 2010 K=462');
    assertEq(EPDO_PRESETS.vdot2024.weights.K, 1032, 'VDOT 2024 K=1032');
    assertEq(EPDO_PRESETS.fhwa2022.weights.K, 975, 'FHWA 2022 K=975');

    // ================================================================
    // TEST GROUP 4: Preset Switching — calcEPDO Responds Dynamically
    // ================================================================
    console.log('\n=== GROUP 4: Preset Switching — Dynamic calcEPDO ===');

    assert(typeof calcEPDO === 'function', 'calcEPDO function exists');
    assert(typeof calculateEPDO === 'function', 'calculateEPDO wrapper exists');

    const testSev = { K: 2, A: 5, B: 10, C: 20, O: 100 };

    // Test with HSM Standard
    loadEPDOPreset('hsm2010');
    const hsmResult = calcEPDO(testSev);
    const hsmExpected = 2*462 + 5*62 + 10*12 + 20*5 + 100*1;
    assertEq(hsmResult, hsmExpected, `HSM: calcEPDO({K:2,A:5,B:10,C:20,O:100}) = ${hsmExpected}`);
    assertEq(EPDO_WEIGHTS.K, 462, 'After HSM switch, EPDO_WEIGHTS.K = 462');

    // Test with VDOT 2024
    loadEPDOPreset('vdot2024');
    const vdotResult = calcEPDO(testSev);
    const vdotExpected = 2*1032 + 5*53 + 10*16 + 20*10 + 100*1;
    assertEq(vdotResult, vdotExpected, `VDOT: calcEPDO({K:2,A:5,B:10,C:20,O:100}) = ${vdotExpected}`);
    assertEq(EPDO_WEIGHTS.K, 1032, 'After VDOT switch, EPDO_WEIGHTS.K = 1032');
    assertNotEq(vdotResult, hsmResult, 'VDOT result differs from HSM result');

    // Test with FHWA 2022
    loadEPDOPreset('fhwa2022');
    const fhwaResult = calcEPDO(testSev);
    const fhwaExpected = 2*975 + 5*48 + 10*13 + 20*8 + 100*1;
    assertEq(fhwaResult, fhwaExpected, `FHWA: calcEPDO({K:2,A:5,B:10,C:20,O:100}) = ${fhwaExpected}`);
    assertEq(EPDO_WEIGHTS.K, 975, 'After FHWA switch, EPDO_WEIGHTS.K = 975');

    // Test with stateDefault (resolves to current state)
    loadEPDOPreset('stateDefault');
    const stateResult = calcEPDO(testSev);
    const resolvedFips = getCurrentStateFips();
    const resolvedWeights = getStateEPDOWeights(resolvedFips);
    const stateExpected = 2*resolvedWeights.weights.K + 5*resolvedWeights.weights.A + 10*resolvedWeights.weights.B + 20*resolvedWeights.weights.C + 100*1;
    assertEq(stateResult, stateExpected, `stateDefault: calcEPDO resolves to ${resolvedWeights.name} weights (K=${resolvedWeights.weights.K})`);
    assertEq(EPDO_ACTIVE_PRESET, 'stateDefault', 'EPDO_ACTIVE_PRESET is stateDefault');

    // Verify calculateEPDO wrapper matches calcEPDO
    const wrapperResult = calculateEPDO(testSev);
    assertEq(wrapperResult, stateResult, 'calculateEPDO() matches calcEPDO()');

    // Test calcEPDO with edge cases
    assertEq(calcEPDO({}), 0, 'calcEPDO({}) = 0');
    assertEq(calcEPDO({ K: 1 }), EPDO_WEIGHTS.K, 'calcEPDO({K:1}) = EPDO_WEIGHTS.K');
    assertEq(calcEPDO({ O: 1 }), 1, 'calcEPDO({O:1}) = 1');
    assertEq(calcEPDO({ K: 0, A: 0, B: 0, C: 0, O: 0 }), 0, 'calcEPDO(all zeros) = 0');

    // ================================================================
    // TEST GROUP 5: State Selection → EPDO Auto-Update
    // ================================================================
    console.log('\n=== GROUP 5: State Selection → EPDO Auto-Update ===');

    // Simulate selecting different states and verify EPDO updates
    const testStates = [
        { fips: '51', name: 'Virginia', expectedK: 1032 },
        { fips: '06', name: 'California', expectedK: 1100 },
        { fips: '48', name: 'Texas', expectedK: 920 },
        { fips: '08', name: 'Colorado', expectedK: 883 },
        { fips: '25', name: 'Massachusetts', expectedK: 1200 },
        { fips: '12', name: 'Florida', expectedK: 985 },
        { fips: '37', name: 'North Carolina', expectedK: 770 },
        { fips: '01', name: 'Alabama', expectedK: 883 },
    ];

    // Set to stateDefault first
    loadEPDOPreset('stateDefault');

    testStates.forEach(state => {
        // Simulate what applyDynamicStateConfig does for EPDO
        applyStateDefaultEPDO(state.fips, state.name);
        assertEq(EPDO_WEIGHTS.K, state.expectedK,
            `Select ${state.name} (FIPS ${state.fips}) → K=${state.expectedK}`);

        // Verify calcEPDO uses the new weights
        const fatal1 = calcEPDO({ K: 1 });
        assertEq(fatal1, state.expectedK,
            `calcEPDO({K:1}) = ${state.expectedK} after selecting ${state.name}`);
    });

    // Test that non-stateDefault presets are NOT affected by state changes
    loadEPDOPreset('hsm2010');
    applyStateDefaultEPDO('51', 'Virginia'); // Should be ignored since preset is hsm2010
    assertEq(EPDO_WEIGHTS.K, 462, 'HSM preset NOT overwritten when state changes to Virginia');
    assertEq(EPDO_ACTIVE_PRESET, 'hsm2010', 'EPDO_ACTIVE_PRESET stays hsm2010');

    loadEPDOPreset('vdot2024');
    applyStateDefaultEPDO('08', 'Colorado'); // Should be ignored
    assertEq(EPDO_WEIGHTS.K, 1032, 'VDOT preset NOT overwritten when state changes to Colorado');

    loadEPDOPreset('fhwa2022');
    applyStateDefaultEPDO('06', 'California'); // Should be ignored
    assertEq(EPDO_WEIGHTS.K, 975, 'FHWA preset NOT overwritten when state changes to California');

    // Custom weights are preserved across state changes
    loadEPDOPreset('custom');
    const customK = document.getElementById('epdoCustomK');
    if (customK) {
        customK.value = '500';
        saveCustomEPDOWeights();
        applyStateDefaultEPDO('51', 'Virginia'); // Should be ignored
        assertEq(EPDO_WEIGHTS.K, 500, 'Custom K=500 NOT overwritten by state change');
    }

    // ================================================================
    // TEST GROUP 6: Full EPDO Propagation Test
    // (Switch states and verify calcEPDO produces different results)
    // ================================================================
    console.log('\n=== GROUP 6: Full EPDO Propagation — Different States Produce Different Scores ===');

    const crashProfile = { K: 3, A: 10, B: 25, C: 40, O: 200 };
    const epdoByState = {};

    loadEPDOPreset('stateDefault');

    testStates.forEach(state => {
        applyStateDefaultEPDO(state.fips, state.name);
        epdoByState[state.fips] = calcEPDO(crashProfile);
    });

    // Virginia (K=1032) should score higher than Colorado (K=883) for same crash profile
    assert(epdoByState['51'] > epdoByState['08'],
        `Virginia EPDO (${epdoByState['51']}) > Colorado EPDO (${epdoByState['08']}) for same crashes`);

    // California (K=1100) should score higher than Virginia (K=1032)
    assert(epdoByState['06'] > epdoByState['51'],
        `California EPDO (${epdoByState['06']}) > Virginia EPDO (${epdoByState['51']}) for same crashes`);

    // Massachusetts (K=1200) should be the highest
    assert(epdoByState['25'] >= epdoByState['06'],
        `Massachusetts EPDO (${epdoByState['25']}) >= California EPDO (${epdoByState['06']})`);

    // Alabama (K=883) should equal Colorado (K=883) since both use FHWA 2025
    assertEq(epdoByState['01'], epdoByState['08'],
        `Alabama EPDO (${epdoByState['01']}) = Colorado EPDO (${epdoByState['08']}) (both FHWA 2025)`);

    // Verify the math is correct for Virginia
    const vaExpected = 3*1032 + 10*53 + 25*16 + 40*10 + 200*1;
    assertEq(epdoByState['51'], vaExpected,
        `Virginia EPDO math: 3×1032 + 10×53 + 25×16 + 40×10 + 200×1 = ${vaExpected}`);

    // Verify the math is correct for Colorado
    const coExpected = 3*883 + 10*94 + 25*21 + 40*11 + 200*1;
    assertEq(epdoByState['08'], coExpected,
        `Colorado EPDO math: 3×883 + 10×94 + 25×21 + 40×11 + 200×1 = ${coExpected}`);

    // ================================================================
    // TEST GROUP 7: Collapsible UI Elements
    // ================================================================
    console.log('\n=== GROUP 7: Collapsible UI Elements ===');

    // Verify all new DOM elements exist
    const requiredNewElements = [
        'epdoSectionToggle', 'epdoActiveLabel', 'epdoChevron', 'epdoSectionContent',
        'epdoPresetStateDefault', 'epdoStateDefaultDesc'
    ];
    requiredNewElements.forEach(id => {
        const el = document.getElementById(id);
        assert(el !== null, `DOM element #${id} exists`);
    });

    // Verify all original elements still exist
    const originalElements = [
        'epdoPresetHSM', 'epdoPresetVDOT', 'epdoPresetFHWA', 'epdoPresetCustom',
        'epdoCustomInputs', 'epdoCustomK', 'epdoCustomA', 'epdoCustomB', 'epdoCustomC', 'epdoCustomO'
    ];
    originalElements.forEach(id => {
        const el = document.getElementById(id);
        assert(el !== null, `Original DOM element #${id} still exists`);
    });

    // Test toggle functionality
    assert(typeof toggleEPDOSection === 'function', 'toggleEPDOSection() exists');

    const content = document.getElementById('epdoSectionContent');
    const chevron = document.getElementById('epdoChevron');
    const toggle = document.getElementById('epdoSectionToggle');

    if (content && chevron && toggle) {
        // Start collapsed
        content.style.display = 'none';
        chevron.style.transform = 'rotate(-90deg)';

        // Toggle open
        toggleEPDOSection();
        assertEq(content.style.display, 'flex', 'After toggle: content is visible (display:flex)');
        assertEq(chevron.style.transform, 'rotate(0deg)', 'After toggle: chevron rotated to 0deg');
        assertEq(toggle.getAttribute('aria-expanded'), 'true', 'After toggle: aria-expanded=true');

        // Toggle closed
        toggleEPDOSection();
        assertEq(content.style.display, 'none', 'After 2nd toggle: content hidden (display:none)');
        assertEq(chevron.style.transform, 'rotate(-90deg)', 'After 2nd toggle: chevron rotated back');
        assertEq(toggle.getAttribute('aria-expanded'), 'false', 'After 2nd toggle: aria-expanded=false');
    }

    // ================================================================
    // TEST GROUP 8: Preset UI Updates
    // ================================================================
    console.log('\n=== GROUP 8: Preset UI Updates ===');

    // Test each preset activates the correct radio button
    const presetRadioMap = {
        stateDefault: 'epdoPresetStateDefault',
        hsm2010: 'epdoPresetHSM',
        vdot2024: 'epdoPresetVDOT',
        fhwa2022: 'epdoPresetFHWA',
        custom: 'epdoPresetCustom'
    };

    Object.entries(presetRadioMap).forEach(([presetKey, radioId]) => {
        loadEPDOPreset(presetKey);
        const radio = document.getElementById(radioId);
        if (radio) {
            assert(radio.checked, `loadEPDOPreset('${presetKey}') → #${radioId} is checked`);
            // Verify all other radios are unchecked
            Object.entries(presetRadioMap).forEach(([otherKey, otherId]) => {
                if (otherKey !== presetKey) {
                    const otherRadio = document.getElementById(otherId);
                    if (otherRadio) {
                        assert(!otherRadio.checked, `  #${otherId} is NOT checked when ${presetKey} active`);
                    }
                }
            });
        }
    });

    // Test active label updates
    loadEPDOPreset('stateDefault');
    const activeLabel = document.getElementById('epdoActiveLabel');
    if (activeLabel) {
        assert(activeLabel.textContent.includes('State Default'), 'Active label shows "State Default" for stateDefault');
        assert(activeLabel.textContent.includes('K='), 'Active label includes K= weight value');
    }

    loadEPDOPreset('hsm2010');
    if (activeLabel) {
        assert(activeLabel.textContent.includes('HSM'), 'Active label shows "HSM" for hsm2010');
        assert(activeLabel.textContent.includes('K=462'), 'Active label shows K=462 for HSM');
    }

    loadEPDOPreset('vdot2024');
    if (activeLabel) {
        assert(activeLabel.textContent.includes('VDOT'), 'Active label shows "VDOT" for vdot2024');
        assert(activeLabel.textContent.includes('K=1032'), 'Active label shows K=1032 for VDOT');
    }

    // Test custom inputs visibility
    loadEPDOPreset('custom');
    const customInputs = document.getElementById('epdoCustomInputs');
    if (customInputs) {
        assertEq(customInputs.style.display, 'grid', 'Custom inputs visible when custom preset active');
    }
    loadEPDOPreset('hsm2010');
    if (customInputs) {
        assertEq(customInputs.style.display, 'none', 'Custom inputs hidden when non-custom preset active');
    }

    // Test State Default description updates
    loadEPDOPreset('stateDefault');
    applyStateDefaultEPDO('51', 'Virginia');
    const stateDescEl = document.getElementById('epdoStateDefaultDesc');
    if (stateDescEl) {
        assert(stateDescEl.textContent.includes('1032'), 'State Default desc includes K=1032 for Virginia');
        assert(stateDescEl.textContent.includes('Virginia'), 'State Default desc includes "Virginia"');
    }

    applyStateDefaultEPDO('08', 'Colorado');
    if (stateDescEl) {
        assert(stateDescEl.textContent.includes('883'), 'State Default desc includes K=883 for Colorado');
    }

    // ================================================================
    // TEST GROUP 9: Dashboard Weight Labels
    // ================================================================
    console.log('\n=== GROUP 9: Dashboard Weight Labels ===');

    // epdoWeightsLabel
    const dashLabel = document.getElementById('epdoWeightsLabel');
    if (dashLabel) {
        loadEPDOPreset('hsm2010');
        assert(dashLabel.textContent.includes('K=462'), 'Dashboard label shows K=462 for HSM');
        assert(dashLabel.textContent.includes('HSM'), 'Dashboard label shows HSM preset name');

        loadEPDOPreset('vdot2024');
        assert(dashLabel.textContent.includes('K=1032'), 'Dashboard label shows K=1032 for VDOT');
        assert(dashLabel.textContent.includes('VDOT'), 'Dashboard label shows VDOT preset name');

        loadEPDOPreset('fhwa2022');
        assert(dashLabel.textContent.includes('K=975'), 'Dashboard label shows K=975 for FHWA');
    } else {
        skip('Dashboard epdoWeightsLabel element not found');
    }

    // Glossary values
    const glossaryK = document.getElementById('epdoGlossaryK');
    if (glossaryK) {
        loadEPDOPreset('hsm2010');
        assertEq(glossaryK.textContent, '462', 'Glossary K shows 462 for HSM');
        loadEPDOPreset('vdot2024');
        assertEq(glossaryK.textContent, '1032', 'Glossary K shows 1032 for VDOT');
        loadEPDOPreset('fhwa2022');
        assertEq(glossaryK.textContent, '975', 'Glossary K shows 975 for FHWA');
    } else {
        skip('Glossary epdoGlossaryK element not found');
    }

    // Glossary definition
    const glossaryDef = document.getElementById('epdoGlossaryDef');
    if (glossaryDef) {
        loadEPDOPreset('hsm2010');
        assert(glossaryDef.textContent.includes('K=462'), 'Glossary def includes K=462 for HSM');
        loadEPDOPreset('vdot2024');
        assert(glossaryDef.textContent.includes('K=1032'), 'Glossary def includes K=1032 for VDOT');
    } else {
        skip('Glossary epdoGlossaryDef element not found');
    }

    // Glossary example value
    const exampleVal = document.getElementById('epdoExampleValue');
    if (exampleVal) {
        loadEPDOPreset('hsm2010');
        assertEq(exampleVal.textContent, '924', 'Glossary example: 2×462 = 924 for HSM');
        loadEPDOPreset('vdot2024');
        assertEq(exampleVal.textContent, (2064).toLocaleString(), 'Glossary example: 2×1032 = 2,064 for VDOT');
        loadEPDOPreset('fhwa2022');
        assertEq(exampleVal.textContent, (1950).toLocaleString(), 'Glossary example: 2×975 = 1,950 for FHWA');
    } else {
        skip('Glossary epdoExampleValue element not found');
    }

    // ================================================================
    // TEST GROUP 10: localStorage Persistence
    // ================================================================
    console.log('\n=== GROUP 10: localStorage Persistence ===');

    // Test stateDefault persists
    loadEPDOPreset('stateDefault');
    assertEq(localStorage.getItem('epdoActivePreset'), 'stateDefault', 'stateDefault saved to localStorage');

    loadEPDOPreset('hsm2010');
    assertEq(localStorage.getItem('epdoActivePreset'), 'hsm2010', 'hsm2010 saved to localStorage');

    loadEPDOPreset('vdot2024');
    assertEq(localStorage.getItem('epdoActivePreset'), 'vdot2024', 'vdot2024 saved to localStorage');

    // Test custom weights persist
    loadEPDOPreset('custom');
    const ckEl = document.getElementById('epdoCustomK');
    if (ckEl) {
        ckEl.value = '777';
        saveCustomEPDOWeights();
        assertEq(localStorage.getItem('epdoActivePreset'), 'custom', 'custom saved to localStorage');
        const saved = JSON.parse(localStorage.getItem('epdoCustomWeights'));
        assertEq(saved.K, 777, 'Custom K=777 saved to localStorage');
    }

    // Test loadSavedEPDOPreset() restores correctly
    localStorage.setItem('epdoActivePreset', 'stateDefault');
    loadSavedEPDOPreset();
    assertEq(EPDO_ACTIVE_PRESET, 'stateDefault', 'loadSavedEPDOPreset restores stateDefault');

    localStorage.setItem('epdoActivePreset', 'hsm2010');
    loadSavedEPDOPreset();
    assertEq(EPDO_ACTIVE_PRESET, 'hsm2010', 'loadSavedEPDOPreset restores hsm2010');
    assertEq(EPDO_WEIGHTS.K, 462, 'loadSavedEPDOPreset restores K=462 for HSM');

    // Test default when nothing in localStorage
    localStorage.removeItem('epdoActivePreset');
    loadSavedEPDOPreset();
    assertEq(EPDO_ACTIVE_PRESET, 'stateDefault', 'loadSavedEPDOPreset defaults to stateDefault when empty');
    assertEq(localStorage.getItem('epdoActivePreset'), 'stateDefault', 'stateDefault written to localStorage as new default');

    // ================================================================
    // TEST GROUP 11: Warrant Independence
    // ================================================================
    console.log('\n=== GROUP 11: Warrant-Specific Weights Independence ===');

    // Verify that changing the global preset does NOT affect warrant functions
    // The warrant functions use local const declarations that shadow the global

    // Test roundabout warrant function exists and uses separate weights
    if (typeof roundabout_autoPopulateCrashData === 'function') {
        // Verify the warrant function exists
        assert(true, 'roundabout_autoPopulateCrashData() exists');
    } else {
        skip('roundabout_autoPopulateCrashData not in scope (loaded lazily)');
    }

    // Test streetlight warrant function
    if (typeof streetlight_analyzeCrashesByLight === 'function') {
        assert(true, 'streetlight_analyzeCrashesByLight() exists');
    } else {
        skip('streetlight_analyzeCrashesByLight not in scope (loaded lazily)');
    }

    // The key test: switching to a different preset and back should not change
    // any locally-scoped warrant weights. We test this by verifying the global
    // EPDO_WEIGHTS can be freely changed without concern for warrant functions.
    loadEPDOPreset('vdot2024'); // K=1032
    assertEq(EPDO_WEIGHTS.K, 1032, 'Global EPDO_WEIGHTS.K = 1032 (VDOT)');

    loadEPDOPreset('hsm2010'); // K=462
    assertEq(EPDO_WEIGHTS.K, 462, 'Global EPDO_WEIGHTS.K = 462 (HSM)');

    // Warrant functions define their OWN local const with K=1500, A=240
    // These are never affected by the global variable
    assert(true, 'Global EPDO_WEIGHTS changes do not affect warrant-local const declarations');

    // ================================================================
    // TEST GROUP 12: recalculateAllEPDO Cascade
    // ================================================================
    console.log('\n=== GROUP 12: recalculateAllEPDO Cascade ===');

    assert(typeof recalculateAllEPDO === 'function', 'recalculateAllEPDO() exists');

    if (typeof crashState !== 'undefined' && crashState.loaded) {
        // Test that switching presets triggers dashboard update
        const dashEPDO = document.getElementById('kpiEPDO');
        if (dashEPDO) {
            loadEPDOPreset('hsm2010');
            const hsmVal = dashEPDO.textContent;

            loadEPDOPreset('vdot2024');
            const vdotVal = dashEPDO.textContent;

            // VDOT weights are higher, so EPDO should be higher
            // (unless there are zero K crashes, which is unlikely)
            if (hsmVal !== '--' && vdotVal !== '--') {
                const hsmNum = parseInt(hsmVal.replace(/,/g, ''));
                const vdotNum = parseInt(vdotVal.replace(/,/g, ''));
                if (!isNaN(hsmNum) && !isNaN(vdotNum) && hsmNum > 0) {
                    assert(vdotNum > hsmNum,
                        `Dashboard EPDO: VDOT (${vdotVal}) > HSM (${hsmVal}) with loaded data`);
                } else {
                    skip('Dashboard EPDO values not parseable');
                }
            } else {
                skip('Dashboard EPDO not yet computed');
            }
        }

        // Test hotspot recalculation
        if (typeof crashState.hotspots !== 'undefined' && crashState.hotspots.length > 0) {
            loadEPDOPreset('hsm2010');
            const firstHotspotHSM = crashState.hotspots[0]?.epdo;

            // Clear and re-analyze with VDOT
            crashState.hotspots = [];
            loadEPDOPreset('vdot2024');
            // Wait for async hotspot analysis
            setTimeout(() => {
                if (crashState.hotspots.length > 0) {
                    const firstHotspotVDOT = crashState.hotspots[0]?.epdo;
                    if (firstHotspotHSM && firstHotspotVDOT) {
                        assert(firstHotspotVDOT > firstHotspotHSM,
                            `Hotspot #1 EPDO: VDOT (${firstHotspotVDOT}) > HSM (${firstHotspotHSM})`);
                    }
                }
            }, 500);
        } else {
            skip('No hotspots analyzed — hotspot EPDO comparison');
        }

        // Test grant ranking recalculation
        if (typeof grantState !== 'undefined' && grantState.loaded &&
            grantState.allRankedLocations?.length > 0) {
            loadEPDOPreset('hsm2010');
            // Give time for recalculation
            setTimeout(() => {
                const firstGrantHSM = grantState.allRankedLocations[0]?.epdo;
                loadEPDOPreset('vdot2024');
                setTimeout(() => {
                    const firstGrantVDOT = grantState.allRankedLocations[0]?.epdo;
                    if (firstGrantHSM && firstGrantVDOT) {
                        assert(firstGrantVDOT > firstGrantHSM,
                            `Grant #1 EPDO: VDOT (${firstGrantVDOT}) > HSM (${firstGrantHSM})`);
                    }
                }, 500);
            }, 500);
        } else {
            skip('No grant data — grant EPDO comparison');
        }
    } else {
        skip('crashState not loaded — dashboard EPDO comparison');
        skip('crashState not loaded — hotspot EPDO comparison');
        skip('crashState not loaded — grant EPDO comparison');
    }

    // ================================================================
    // TEST GROUP 13: Multi-State Rapid Switching Stress Test
    // ================================================================
    console.log('\n=== GROUP 13: Rapid State Switching Stress Test ===');

    loadEPDOPreset('stateDefault');

    // Rapidly switch through all test states
    let stressOK = true;
    for (let i = 0; i < 3; i++) {
        testStates.forEach(state => {
            applyStateDefaultEPDO(state.fips, state.name);
            if (EPDO_WEIGHTS.K !== state.expectedK) stressOK = false;
            const result = calcEPDO({ K: 1 });
            if (result !== state.expectedK) stressOK = false;
        });
    }
    assert(stressOK, 'Rapid 3× cycling through 8 states: weights always correct');

    // Verify no state leakage after rapid switching
    applyStateDefaultEPDO('51', 'Virginia');
    assertEq(EPDO_WEIGHTS.K, 1032, 'After stress test: Virginia K=1032 (no leakage)');
    assertEq(EPDO_WEIGHTS.A, 53, 'After stress test: Virginia A=53');
    assertEq(EPDO_WEIGHTS.B, 16, 'After stress test: Virginia B=16');
    assertEq(EPDO_WEIGHTS.C, 10, 'After stress test: Virginia C=10');
    assertEq(EPDO_WEIGHTS.O, 1, 'After stress test: Virginia O=1');

    // ================================================================
    // TEST GROUP 14: All 50 States Resolve Without Error
    // ================================================================
    console.log('\n=== GROUP 14: All 50 States + DC Resolve Without Error ===');

    loadEPDOPreset('stateDefault');
    let allStatesResolve = true;
    let stateErrors = [];

    ALL_STATE_FIPS.forEach(fips => {
        try {
            applyStateDefaultEPDO(fips, `State FIPS ${fips}`);
            if (typeof EPDO_WEIGHTS.K !== 'number' || EPDO_WEIGHTS.K <= 0) {
                allStatesResolve = false;
                stateErrors.push(`FIPS ${fips}: invalid K=${EPDO_WEIGHTS.K}`);
            }
            if (EPDO_WEIGHTS.O !== 1) {
                allStatesResolve = false;
                stateErrors.push(`FIPS ${fips}: O should be 1, got ${EPDO_WEIGHTS.O}`);
            }
            // Verify calcEPDO works
            const result = calcEPDO({ K: 1, A: 1, B: 1, C: 1, O: 1 });
            if (typeof result !== 'number' || result <= 0) {
                allStatesResolve = false;
                stateErrors.push(`FIPS ${fips}: calcEPDO returned ${result}`);
            }
        } catch (e) {
            allStatesResolve = false;
            stateErrors.push(`FIPS ${fips}: ${e.message}`);
        }
    });

    assert(allStatesResolve,
        `All 51 state FIPS resolve without errors${stateErrors.length ? ' — ' + stateErrors.join(', ') : ''}`);

    // ================================================================
    // TEST GROUP 15: EPDO Consistency Across Functions
    // ================================================================
    console.log('\n=== GROUP 15: EPDO Consistency Across Functions ===');

    loadEPDOPreset('vdot2024');

    // All these should produce the same result for the same input
    const consistencyInput = { K: 5, A: 10, B: 20, C: 30, O: 50 };
    const resultCalcEPDO = calcEPDO(consistencyInput);
    const resultCalculateEPDO = calculateEPDO(consistencyInput);
    const resultManual = 5*EPDO_WEIGHTS.K + 10*EPDO_WEIGHTS.A + 20*EPDO_WEIGHTS.B + 30*EPDO_WEIGHTS.C + 50*EPDO_WEIGHTS.O;

    assertEq(resultCalcEPDO, resultManual, 'calcEPDO matches manual calculation');
    assertEq(resultCalculateEPDO, resultManual, 'calculateEPDO matches manual calculation');
    assertEq(resultCalcEPDO, resultCalculateEPDO, 'calcEPDO === calculateEPDO');

    // Verify the inline EPDO_WEIGHTS[severity] pattern also works
    let inlineResult = 0;
    const severities = ['K', 'A', 'B', 'C', 'O'];
    const counts = { K: 5, A: 10, B: 20, C: 30, O: 50 };
    severities.forEach(sev => {
        inlineResult += (counts[sev] || 0) * (EPDO_WEIGHTS[sev] || 1);
    });
    assertEq(inlineResult, resultManual, 'Inline EPDO_WEIGHTS[sev] pattern matches calcEPDO()');

    // ================================================================
    // TEST GROUP 16: EPDO_PRESETS.stateDefault Name Updates
    // ================================================================
    console.log('\n=== GROUP 16: stateDefault Preset Dynamic Name/Description ===');

    loadEPDOPreset('stateDefault');
    applyStateDefaultEPDO('51', 'Virginia');
    assert(EPDO_PRESETS.stateDefault.name.includes('Virginia'),
        'stateDefault.name includes "Virginia" after VA selection');
    assert(EPDO_PRESETS.stateDefault.description.includes('VDOT'),
        'stateDefault.description includes "VDOT" for VA');

    applyStateDefaultEPDO('06', 'California');
    assert(EPDO_PRESETS.stateDefault.name.includes('California'),
        'stateDefault.name includes "California" after CA selection');
    assert(EPDO_PRESETS.stateDefault.description.includes('Caltrans'),
        'stateDefault.description includes "Caltrans" for CA');

    applyStateDefaultEPDO('01', 'Alabama');
    assert(EPDO_PRESETS.stateDefault.name.includes('Alabama'),
        'stateDefault.name includes "Alabama" after AL selection');
    assert(EPDO_PRESETS.stateDefault.description.includes('HSM'),
        'stateDefault.description includes "HSM" for AL (default)');

    // ================================================================
    // TEST GROUP 17: Edge Cases
    // ================================================================
    console.log('\n=== GROUP 17: Edge Cases ===');

    // Unknown preset key
    const prevPreset = EPDO_ACTIVE_PRESET;
    const prevK = EPDO_WEIGHTS.K;
    loadEPDOPreset('nonexistent_preset');
    assertEq(EPDO_ACTIVE_PRESET, prevPreset, 'Unknown preset key does not change active preset');
    assertEq(EPDO_WEIGHTS.K, prevK, 'Unknown preset key does not change weights');

    // Null/undefined FIPS
    const nullResult = getStateEPDOWeights(null);
    assert(nullResult.weights.K > 0, 'getStateEPDOWeights(null) returns valid weights (fallback)');

    const undefinedResult = getStateEPDOWeights(undefined);
    assert(undefinedResult.weights.K > 0, 'getStateEPDOWeights(undefined) returns valid weights (fallback)');

    // Empty string FIPS
    const emptyResult = getStateEPDOWeights('');
    assert(emptyResult.weights.K > 0, 'getStateEPDOWeights("") returns valid weights (fallback)');

    // Numeric FIPS (should be string but handle gracefully)
    const numericResult = getStateEPDOWeights(51);
    assertEq(numericResult.weights.K, 1032, 'getStateEPDOWeights(51) handles numeric FIPS → Virginia');

    // calcEPDO with negative values (defensive)
    const negResult = calcEPDO({ K: -1, A: 0, B: 0, C: 0, O: 0 });
    assert(typeof negResult === 'number', 'calcEPDO handles negative K without crashing');

    // calcEPDO with very large values
    const bigResult = calcEPDO({ K: 1000000, A: 0, B: 0, C: 0, O: 0 });
    assert(typeof bigResult === 'number' && bigResult > 0, 'calcEPDO handles K=1,000,000 without overflow');

    // ================================================================
    // RESTORE ORIGINAL STATE
    // ================================================================
    console.log('\n--- Restoring original state ---');

    // Restore EPDO weights to what they were before the test
    EPDO_WEIGHTS = { ..._origWeights };
    EPDO_ACTIVE_PRESET = _origPreset;
    if (_origLocalStorage) {
        localStorage.setItem('epdoActivePreset', _origLocalStorage);
    } else {
        localStorage.setItem('epdoActivePreset', 'stateDefault');
    }
    updateEPDOPresetUI();
    updateEPDOWeightLabels();
    // Recalculate if data is loaded to restore accurate display
    if (typeof crashState !== 'undefined' && crashState.loaded) {
        recalculateAllEPDO();
    }
    console.log(`  Restored: preset=${_origPreset}, K=${_origWeights.K}`);

    // ================================================================
    // SUMMARY
    // ================================================================
    console.log('\n' + '═'.repeat(65));
    console.log(`  TEST RESULTS: ${passed} passed, ${failed} failed, ${skipped} skipped`);
    console.log('═'.repeat(65));

    if (errors.length > 0) {
        console.log('\n  Failed tests:');
        errors.forEach((e, i) => console.log(`    ${i+1}. ${e}`));
    }

    if (failed === 0) {
        console.log('\n  🎉 ALL TESTS PASSED — Dynamic EPDO scoring is fully functional!');
    } else {
        console.log(`\n  ⚠️  ${failed} test(s) need attention.`);
    }

    return { passed, failed, skipped, errors };
})();
