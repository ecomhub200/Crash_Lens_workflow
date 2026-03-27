/**
 * EPDO (Equivalent Property Damage Only) Dynamic Weight System
 * ============================================================
 *
 * Reference implementation for making EPDO weights configurable
 * across multiple state/federal agencies.
 *
 * EPDO weights are derived from crash cost ratios:
 *   Weight = CrashCost(severity) / CrashCost(PDO)
 *
 * Different agencies have different crash cost data, so their
 * EPDO weights differ significantly.
 */

// ============================================================
// STEP 1: MUTABLE EPDO_WEIGHTS + PRESETS
// Replace the existing const at app/index.html:19916
// ============================================================

let EPDO_WEIGHTS = { K: 883, A: 94, B: 21, C: 11, O: 1 };
let EPDO_ACTIVE_PRESET = 'hsm2010';

const EPDO_PRESETS = {
    hsm2010: {
        name: 'HSM Standard (2010)',
        weights: { K: 462, A: 62, B: 12, C: 5, O: 1 },
        description: 'Highway Safety Manual standard weights (AASHTO/FHWA)'
    },
    vdot2024: {
        name: 'VDOT 2024',
        weights: { K: 1032, A: 53, B: 16, C: 10, O: 1 },
        description: 'Derived from VDOT 2024 crash cost ratios ($12.8M K / $12.4K O)'
    },
    fhwa2022: {
        name: 'FHWA 2022',
        weights: { K: 975, A: 48, B: 13, C: 8, O: 1 },
        description: 'Derived from FHWA 2022 crash cost ratios ($11.6M K / $11.9K O)'
    },
    custom: {
        name: 'Custom',
        weights: { K: 883, A: 94, B: 21, C: 11, O: 1 },
        description: 'User-defined custom weights'
    }
};


// ============================================================
// STEP 2: PRESET SWITCHING + RECALCULATION FUNCTIONS
// Insert after EPDO_PRESETS definition
// ============================================================

/**
 * Load an EPDO weight preset and trigger full recalculation.
 * @param {string} presetKey - Key from EPDO_PRESETS ('hsm2010', 'vdot2024', 'fhwa2022', 'custom')
 */
function loadEPDOPreset(presetKey) {
    if (presetKey === 'custom') {
        // Read from custom input fields
        EPDO_WEIGHTS = {
            K: parseInt(document.getElementById('epdoCustomK')?.value) || 462,
            A: parseInt(document.getElementById('epdoCustomA')?.value) || 62,
            B: parseInt(document.getElementById('epdoCustomB')?.value) || 12,
            C: parseInt(document.getElementById('epdoCustomC')?.value) || 5,
            O: parseInt(document.getElementById('epdoCustomO')?.value) || 1
        };
        EPDO_PRESETS.custom.weights = { ...EPDO_WEIGHTS };
    } else {
        const preset = EPDO_PRESETS[presetKey];
        if (!preset) {
            console.warn('[EPDO] Unknown preset:', presetKey);
            return;
        }
        EPDO_WEIGHTS = { ...preset.weights };
    }

    EPDO_ACTIVE_PRESET = presetKey;

    // Persist to localStorage
    localStorage.setItem('epdoActivePreset', presetKey);
    if (presetKey === 'custom') {
        localStorage.setItem('epdoCustomWeights', JSON.stringify(EPDO_WEIGHTS));
    }

    // Update UI
    updateEPDOPresetUI();
    updateEPDOWeightLabels();

    // Trigger full recalculation cascade
    recalculateAllEPDO();

    console.log(`[EPDO] Preset changed to: ${presetKey}`, EPDO_WEIGHTS);
}

/**
 * Load previously saved EPDO preset from localStorage on app startup.
 * Call this in DOMContentLoaded BEFORE first updateDashboard().
 */
function loadSavedEPDOPreset() {
    const saved = localStorage.getItem('epdoActivePreset');
    if (saved && EPDO_PRESETS[saved]) {
        if (saved === 'custom') {
            const customWeights = safeJsonParse(
                localStorage.getItem('epdoCustomWeights'),
                null
            );
            if (customWeights) {
                EPDO_PRESETS.custom.weights = customWeights;
                // Populate custom input fields
                const fields = ['K', 'A', 'B', 'C', 'O'];
                fields.forEach(f => {
                    const el = document.getElementById('epdoCustom' + f);
                    if (el) el.value = customWeights[f] || (f === 'O' ? 1 : 0);
                });
            }
        }
        loadEPDOPreset(saved);
    }
}

/**
 * Save custom EPDO weights when user modifies input fields.
 */
function saveCustomEPDOWeights() {
    if (EPDO_ACTIVE_PRESET === 'custom') {
        loadEPDOPreset('custom');
    }
}

/**
 * Update radio button states to reflect active preset.
 */
function updateEPDOPresetUI() {
    // Update radio buttons
    const radioMap = {
        hsm2010: 'epdoPresetHSM',
        vdot2024: 'epdoPresetVDOT',
        fhwa2022: 'epdoPresetFHWA',
        custom: 'epdoPresetCustom'
    };

    Object.entries(radioMap).forEach(([key, id]) => {
        const radio = document.getElementById(id);
        if (radio) radio.checked = (key === EPDO_ACTIVE_PRESET);
    });

    // Show/hide custom inputs
    const customInputs = document.getElementById('epdoCustomInputs');
    if (customInputs) {
        customInputs.style.display = EPDO_ACTIVE_PRESET === 'custom' ? 'grid' : 'none';
    }
}

/**
 * Update all dynamic EPDO weight labels in the UI.
 */
function updateEPDOWeightLabels() {
    const label = `Weights: K=${EPDO_WEIGHTS.K}, A=${EPDO_WEIGHTS.A}, B=${EPDO_WEIGHTS.B}, C=${EPDO_WEIGHTS.C}, O=${EPDO_WEIGHTS.O}`;
    const presetName = EPDO_PRESETS[EPDO_ACTIVE_PRESET]?.name || 'Custom';

    // Dashboard EPDO breakdown label
    const dashLabel = document.getElementById('epdoWeightsLabel');
    if (dashLabel) dashLabel.textContent = `${label} (${presetName})`;

    // Glossary definition
    const glossaryDef = document.getElementById('epdoGlossaryDef');
    if (glossaryDef) {
        glossaryDef.textContent = `Weighted severity score: ${label}. Prioritizes locations with severe crashes over high-volume minor crash locations. Using ${presetName} preset.`;
    }
}

/**
 * Cascade EPDO recalculation across ALL tabs.
 *
 * Key insight: calcEPDO() already reads from the global EPDO_WEIGHTS,
 * so any function that calls calcEPDO() at render-time automatically
 * picks up the new weights. This function triggers re-rendering of
 * each affected component.
 */
function recalculateAllEPDO() {
    if (!crashState.loaded) return;

    // 1. Dashboard — re-render (calls calcEPDO live)
    if (typeof updateDashboard === 'function') {
        updateDashboard();
    }

    // 2. Hotspots — re-analyze with new EPDO weights
    if (typeof crashState !== 'undefined' && crashState.hotspots?.length > 0) {
        crashState.hotspots = []; // Clear cached hotspots to force re-analysis
        if (typeof analyzeHotspots === 'function') analyzeHotspots();
    }

    // 3. Grants — invalidate ranking cache and re-rank
    if (typeof grantState !== 'undefined' && grantState.loaded) {
        if (grantState.rankingCache) {
            grantState.rankingCache = { key: null, locations: [] };
        }
        if (typeof rankLocationsForGrants === 'function') {
            rankLocationsForGrants();
        }
    }

    // 4. CMF tab — if location selected, rebuild crash profile
    if (typeof cmfState !== 'undefined' && cmfState.selectedLocation) {
        if (typeof buildCMFCrashProfile === 'function') buildCMFCrashProfile();
        if (typeof updateCMFUI === 'function') updateCMFUI();
    }

    // 5. Safety Focus — re-render active category if loaded
    if (typeof safetyState !== 'undefined' && safetyState.activeCategory) {
        if (typeof updateSafetyCategory === 'function') {
            updateSafetyCategory(safetyState.activeCategory);
        }
    }

    // 6. Before/After — if loaded, re-render
    if (typeof baState !== 'undefined' && baState.locationCrashes?.length > 0) {
        if (typeof updateBAStudy === 'function') updateBAStudy();
    }

    // 7. Map stats panel — if visible
    if (typeof updateMapStats === 'function') updateMapStats();

    // 8. AI context indicator — update to reflect new weights
    if (typeof updateAIContextIndicator === 'function') updateAIContextIndicator();

    console.log('[EPDO] Full recalculation cascade complete');
}


// ============================================================
// STEP 6: HTML FOR UPLOAD DATA TAB SELECTOR
// Insert after Road Type Filter (~line 4700 in app/index.html)
// ============================================================

/*
<!-- EPDO Weight Preset -->
<div class="filter-group" style="flex:1;min-width:280px">
<label style="font-weight:600;color:#0369a1;display:flex;align-items:center;gap:.4rem;margin-bottom:.5rem">
<span aria-hidden="true">&#9878;</span> EPDO Weight System
</label>
<div style="display:flex;flex-direction:column;gap:.5rem">
  <label class="radio-item" style="display:flex;align-items:center;gap:.5rem;cursor:pointer;padding:.4rem .6rem;background:white;border-radius:var(--radius);border:1px solid #e2e8f0">
    <input type="radio" name="epdoPreset" id="epdoPresetHSM" value="hsm2010" checked onchange="loadEPDOPreset('hsm2010')" style="accent-color:#0ea5e9">
    <span style="font-size:.85rem"><strong>HSM Standard (2010)</strong> - K=462, A=62, B=12, C=5, O=1</span>
  </label>
  <label class="radio-item" style="display:flex;align-items:center;gap:.5rem;cursor:pointer;padding:.4rem .6rem;background:white;border-radius:var(--radius);border:1px solid #e2e8f0">
    <input type="radio" name="epdoPreset" id="epdoPresetVDOT" value="vdot2024" onchange="loadEPDOPreset('vdot2024')" style="accent-color:#0ea5e9">
    <span style="font-size:.85rem"><strong>VDOT 2024</strong> - K=1032, A=53, B=16, C=10, O=1</span>
  </label>
  <label class="radio-item" style="display:flex;align-items:center;gap:.5rem;cursor:pointer;padding:.4rem .6rem;background:white;border-radius:var(--radius);border:1px solid #e2e8f0">
    <input type="radio" name="epdoPreset" id="epdoPresetFHWA" value="fhwa2022" onchange="loadEPDOPreset('fhwa2022')" style="accent-color:#0ea5e9">
    <span style="font-size:.85rem"><strong>FHWA 2022</strong> - K=975, A=48, B=13, C=8, O=1</span>
  </label>
  <label class="radio-item" style="display:flex;align-items:center;gap:.5rem;cursor:pointer;padding:.4rem .6rem;background:white;border-radius:var(--radius);border:1px solid #e2e8f0">
    <input type="radio" name="epdoPreset" id="epdoPresetCustom" value="custom" onchange="loadEPDOPreset('custom')" style="accent-color:#0ea5e9">
    <span style="font-size:.85rem"><strong>Custom</strong></span>
  </label>
  <div id="epdoCustomInputs" style="display:none;grid-template-columns:repeat(5,1fr);gap:.3rem;padding:.4rem .6rem;background:#fef3c7;border-radius:var(--radius)">
    <div style="text-align:center"><label style="font-size:.7rem;color:var(--gray)">K</label><input type="number" id="epdoCustomK" value="462" onchange="saveCustomEPDOWeights()" style="width:100%;font-size:.8rem;padding:.3rem;text-align:center;border:1px solid #e2e8f0;border-radius:4px"></div>
    <div style="text-align:center"><label style="font-size:.7rem;color:var(--gray)">A</label><input type="number" id="epdoCustomA" value="62" onchange="saveCustomEPDOWeights()" style="width:100%;font-size:.8rem;padding:.3rem;text-align:center;border:1px solid #e2e8f0;border-radius:4px"></div>
    <div style="text-align:center"><label style="font-size:.7rem;color:var(--gray)">B</label><input type="number" id="epdoCustomB" value="12" onchange="saveCustomEPDOWeights()" style="width:100%;font-size:.8rem;padding:.3rem;text-align:center;border:1px solid #e2e8f0;border-radius:4px"></div>
    <div style="text-align:center"><label style="font-size:.7rem;color:var(--gray)">C</label><input type="number" id="epdoCustomC" value="5" onchange="saveCustomEPDOWeights()" style="width:100%;font-size:.8rem;padding:.3rem;text-align:center;border:1px solid #e2e8f0;border-radius:4px"></div>
    <div style="text-align:center"><label style="font-size:.7rem;color:var(--gray)">O</label><input type="number" id="epdoCustomO" value="1" onchange="saveCustomEPDOWeights()" style="width:100%;font-size:.8rem;padding:.3rem;text-align:center;border:1px solid #e2e8f0;border-radius:4px"></div>
  </div>
</div>
<p style="font-size:.7rem;color:#64748b;margin-top:.35rem">EPDO weights affect severity scoring across all analysis tabs</p>
</div>
*/


// ============================================================
// STEP 7: INITIALIZATION
// Add to a DOMContentLoaded handler, BEFORE first updateDashboard()
// ============================================================

// document.addEventListener('DOMContentLoaded', function() {
//     loadSavedEPDOPreset();
// });
