/**
 * CRASH LENS - Multi-State Data Adapter
 *
 * Auto-detects which state's crash data is being loaded by analyzing CSV column headers,
 * then normalizes the data to CRASH LENS internal format.
 *
 * USAGE:
 *   1. Include this file in index.html BEFORE the main app logic
 *   2. After CSV headers are parsed, call: StateAdapter.detect(headers)
 *   3. For each row, call: StateAdapter.normalizeRow(rawRow)
 *   4. The normalized row will have Virginia-compatible column names
 *      so existing app logic works WITHOUT changes
 *
 * ADDING A NEW STATE:
 *   1. Create states/{state}/config.json with column mappings
 *   2. Add a detection signature in STATE_SIGNATURES below
 *   3. Add normalization logic in NORMALIZERS below
 */

const StateAdapter = (() => {
    'use strict';

    // ─── State detection signatures ───
    // Each state has unique column names that fingerprint its data source
    const STATE_SIGNATURES = {
        colorado: {
            requiredColumns: ['CUID', 'System Code', 'Injury 00', 'Injury 04'],
            optionalColumns: ['Rd_Number', 'Rd_Section', 'MHE'],
            displayName: 'Colorado (CDOT)',
            configPath: 'data/CDOT/config.json'
        },
        virginia: {
            requiredColumns: ['Document Nbr', 'Crash Severity', 'RTE Name', 'SYSTEM'],
            optionalColumns: ['K_People', 'A_People', 'Node', 'Physical Juris Name'],
            displayName: 'Virginia (TREDS)',
            configPath: 'states/virginia/config.json'
        }
        // Add more states here:
        // texas: { requiredColumns: [...], ... }
        // california: { requiredColumns: [...], ... }
    };

    // ─── Current state ───
    let detectedState = null;
    let stateConfig = null;
    let manualStateFips = null;  // Set when user picks a state from the UI
    let dynamicGeoConfig = null; // Built from FIPSDatabase for non-hardcoded states
    let isPreNormalized = false; // True when data was already normalized by the Python pipeline

    // ─── Detection ───
    function detect(csvHeaders) {
        // Reset pre-normalized flag on each new detection
        isPreNormalized = false;

        // Normalize headers for comparison (trim whitespace)
        const normalizedHeaders = new Set(csvHeaders.map(h => h.trim()));

        // Check for normalized Colorado data: _co_* prefixed columns indicate
        // Colorado-origin data that was normalized to Virginia-compatible format
        // by the Python pipeline. This data already has Virginia-compatible column
        // names and must NOT be re-normalized by the JavaScript adapter.
        const hasColoradoProvenance = [...normalizedHeaders].some(h => h.startsWith('_co_'));
        if (hasColoradoProvenance) {
            detectedState = 'colorado';
            isPreNormalized = true;
            console.log('[StateAdapter] Detected state: Colorado (CDOT) — pre-normalized data (skipping JS normalization)');
            return 'colorado';
        }

        for (const [stateKey, signature] of Object.entries(STATE_SIGNATURES)) {
            const allRequired = signature.requiredColumns.every(col => normalizedHeaders.has(col));
            if (allRequired) {
                detectedState = stateKey;
                console.log(`[StateAdapter] Detected state: ${signature.displayName}`);
                console.log(`[StateAdapter] Matched columns: ${signature.requiredColumns.join(', ')}`);
                return stateKey;
            }
        }

        // Fallback: check partial matches
        let bestMatch = null;
        let bestScore = 0;
        for (const [stateKey, signature] of Object.entries(STATE_SIGNATURES)) {
            const allCols = [...signature.requiredColumns, ...signature.optionalColumns];
            const score = allCols.filter(col => normalizedHeaders.has(col)).length / allCols.length;
            if (score > bestScore) {
                bestScore = score;
                bestMatch = stateKey;
            }
        }

        if (bestScore > 0.5) {
            detectedState = bestMatch;
            console.warn(`[StateAdapter] Partial match (${Math.round(bestScore * 100)}%): ${STATE_SIGNATURES[bestMatch].displayName}`);
            return bestMatch;
        }

        console.warn('[StateAdapter] Could not detect state from CSV headers. Assuming Virginia format.');
        detectedState = 'virginia';
        return 'virginia';
    }

    // ─── Colorado Normalizer ───
    // Transforms a Colorado CDOT row into Virginia-compatible format
    const COLORADO_NORMALIZER = {

        normalizeRow(row) {
            const normalized = {};

            // ── ID ──
            normalized['Document Nbr'] = row['CUID'] || '';

            // ── Date/Time ──
            const rawDate = (row['Crash Date'] || '').trim();
            normalized['Crash Date'] = rawDate;
            normalized['Crash Year'] = this._extractYear(rawDate);
            normalized['Crash Military Time'] = (row['Crash Time'] || '').replace(/:/g, '').substring(0, 4);

            // ── Severity (DERIVED from injury counts) ──
            const injK = parseInt(row['Injury 04']) || 0;
            const injA = parseInt(row['Injury 03']) || 0;
            const injB = parseInt(row['Injury 02']) || 0;
            const injC = parseInt(row['Injury 01']) || 0;
            const injO = parseInt(row['Injury 00']) || 0;

            if (injK > 0) normalized['Crash Severity'] = 'K';
            else if (injA > 0) normalized['Crash Severity'] = 'A';
            else if (injB > 0) normalized['Crash Severity'] = 'B';
            else if (injC > 0) normalized['Crash Severity'] = 'C';
            else normalized['Crash Severity'] = 'O';

            normalized['K_People'] = injK;
            normalized['A_People'] = injA;
            normalized['B_People'] = injB;
            normalized['C_People'] = injC;

            // ── Collision Type (mapped from Crash Type / MHE) ──
            normalized['Collision Type'] = this._mapCollisionType(row['Crash Type'] || row['MHE'] || '');

            // ── Conditions ──
            normalized['Weather Condition'] = row['Weather Condition'] || '';
            normalized['Light Condition'] = row['Lighting Conditions'] || '';
            normalized['Roadway Surface Condition'] = row['Road Condition'] || '';
            normalized['Roadway Alignment'] = this._mapAlignment(row['Road Contour Curves'] || '', row['Road Contour Grade'] || '');

            // ── Road Description / Intersection ──
            normalized['Roadway Description'] = row['Road Description'] || '';
            normalized['Intersection Type'] = this._mapIntersectionType(row['Road Description'] || '');

            // ── Route & Location ──
            normalized['RTE Name'] = this._buildRouteName(row);
            normalized['SYSTEM'] = this._mapRoadSystem(row['System Code'] || '');
            normalized['Node'] = this._buildNodeId(row);
            normalized['RNS MP'] = (row['System Code'] === 'State Highway' || row['System Code'] === 'Interstate Highway')
                ? row['Rd_Section'] || '' : '';

            // ── Coordinates ──
            normalized['x'] = parseFloat(row['Longitude']) || 0;
            normalized['y'] = parseFloat(row['Latitude']) || 0;

            // ── Jurisdiction ──
            normalized['Physical Juris Name'] = row['County'] || '';

            // ── Boolean flags (DERIVED) ──
            // Check NM Type AND Crash Type/MHE (NM Type alone misses many)
            normalized['Pedestrian?'] = this._checkPedestrian(row) ? 'Y' : 'N';
            normalized['Bike?'] = this._checkBicycle(row) ? 'Y' : 'N';
            normalized['Alcohol?'] = this._checkAlcohol(row) ? 'Y' : 'N';
            normalized['Speed?'] = this._checkSpeed(row) ? 'Y' : 'N';
            normalized['Hitrun?'] = (row['TU-1 Hit And Run'] === 'TRUE' || row['TU-2 Hit And Run'] === 'TRUE') ? 'Y' : 'N';
            normalized['Motorcycle?'] = this._checkVehicleType(row, 'Motorcycle') ? 'Y' : 'N';
            normalized['Night?'] = this._isNighttime(row['Lighting Conditions'] || '') ? 'Y' : 'N';
            normalized['Distracted?'] = this._checkDistracted(row) ? 'Y' : 'N';
            normalized['Drowsy?'] = this._checkDrowsy(row) ? 'Y' : 'N';
            normalized['Drug Related?'] = this._checkDrugs(row) ? 'Y' : 'N';
            normalized['Young?'] = this._checkAge(row, 16, 20) ? 'Y' : 'N';
            normalized['Senior?'] = this._checkAge(row, 65, 999) ? 'Y' : 'N';
            normalized['Unrestrained?'] = this._checkUnrestrained(row) ? 'Y' : 'N';
            normalized['School Zone'] = (row['School Zone'] === 'TRUE' || row['School Zone'] === 'True') ? 'Y' : 'N';
            normalized['Work Zone Related'] = (row['Construction Zone'] === 'TRUE' || row['Construction Zone'] === 'True') ? 'Y' : 'N';

            // ── Derived safety fields (for Safety Focus tab) ──
            normalized['Animal Related?'] = this._checkAnimal(row) ? 'Yes' : 'No';
            normalized['Guardrail Related?'] = this._checkGuardrail(row) ? 'Yes' : 'No';
            normalized['Lgtruck?'] = this._checkLargeTruck(row) ? 'Yes' : 'No';
            normalized['RoadDeparture Type'] = this._deriveRoadDepartureType(row);
            normalized['Intersection Analysis'] = this._deriveIntersectionAnalysis(row);
            normalized['Max Speed Diff'] = this._calcSpeedDiff(row);

            // ── Traffic Control (not directly available in CO data - mark as N/A) ──
            normalized['Traffic Control Type'] = 'N/A';
            normalized['Traffic Control Status'] = 'N/A';

            // ── Functional Class (not in CO data) ──
            normalized['Functional Class'] = '';
            normalized['Area Type'] = '';
            normalized['Facility Type'] = '';
            normalized['Ownership'] = '';

            // ── First Harmful Event ──
            normalized['First Harmful Event'] = row['First HE'] || '';
            normalized['First Harmful Event Loc'] = row['Location'] || '';

            // ── Keep original CO fields for reference ──
            normalized['_co_system_code'] = row['System Code'] || '';
            normalized['_co_rd_number'] = row['Rd_Number'] || '';
            normalized['_co_location1'] = row['Location 1'] || '';
            normalized['_co_location2'] = row['Location 2'] || '';
            normalized['_co_link'] = row['Link'] || '';
            normalized['_co_crash_type'] = row['Crash Type'] || '';
            normalized['_co_mhe'] = row['MHE'] || '';
            normalized['_co_agency'] = row['Agency Id'] || '';
            normalized['_co_city'] = row['City'] || '';
            normalized['_co_tu1_speed_limit'] = row['TU-1 Speed Limit'] || '';
            normalized['_co_tu1_estimated_speed'] = row['TU-1 Estimated Speed'] || '';
            normalized['_co_tu1_driver_action'] = row['TU-1 Driver Action'] || '';
            normalized['_co_tu1_human_factor'] = row['TU-1 Human Contributing Factor'] || '';

            return normalized;
        },

        // ── Helper: Extract year from M/D/YYYY ──
        _extractYear(dateStr) {
            if (!dateStr) return '';
            const parts = dateStr.split('/');
            if (parts.length === 3) return parts[2];
            return '';
        },

        // ── Helper: Map Colorado crash type to standardized collision type ──
        _mapCollisionType(crashType) {
            const ct = (crashType || '').trim();
            const mapping = {
                // Vehicle-to-vehicle
                'Rear-End': 'Rear End',
                'Front to Rear': 'Rear End',
                'Broadside': 'Angle',
                'Front to Side': 'Angle',
                'Rear to Side': 'Angle',
                'Approach Turn': 'Angle',
                'Overtaking Turn': 'Angle',
                'Head-On': 'Head On',
                'Front to Front': 'Head On',
                'Sideswipe Same Direction': 'Sideswipe - Same Direction',
                'Side to Side-Same Direction': 'Sideswipe - Same Direction',
                'Sideswipe Opposite Direction': 'Sideswipe - Opposite Direction',
                'Side to Side-Opposite Direction': 'Sideswipe - Opposite Direction',
                // Non-collision / rollover
                'Overturning/Rollover': 'Non-Collision',
                'Other Non-Collision': 'Non-Collision',
                'Fell from Motor Vehicle': 'Non-Collision',
                'Ground': 'Non-Collision',
                // Vulnerable road users
                'Pedestrian': 'Pedestrian',
                'School Age To/From School': 'Pedestrian',
                'Bicycle/Motorized Bicycle': 'Bicyclist',
                // Animals
                'Wild Animal': 'Other Animal',
                'Domestic Animal': 'Other Animal',
                // Fixed objects - off road
                'Light Pole/Utility Pole': 'Fixed Object - Off Road',
                'Traffic Signal Pole': 'Fixed Object - Off Road',
                'Concrete Highway Barrier': 'Fixed Object - Off Road',
                'Guardrail Face': 'Fixed Object - Off Road',
                'Guardrail End': 'Fixed Object - Off Road',
                'Cable Rail': 'Fixed Object - Off Road',
                'Tree': 'Fixed Object - Off Road',
                'Fence': 'Fixed Object - Off Road',
                'Sign': 'Fixed Object - Off Road',
                'Curb': 'Fixed Object - Off Road',
                'Embankment': 'Fixed Object - Off Road',
                'Ditch': 'Fixed Object - Off Road',
                'Large Rocks or Boulder': 'Fixed Object - Off Road',
                'Electrical/Utility Box': 'Fixed Object - Off Road',
                'Crash Cushion/Traffic Barrel': 'Fixed Object - Off Road',
                'Mailbox': 'Fixed Object - Off Road',
                'Delineator/Milepost': 'Fixed Object - Off Road',
                'Culvert or Headwall': 'Fixed Object - Off Road',
                'Wall or Building': 'Fixed Object - Off Road',
                'Barricade': 'Fixed Object - Off Road',
                'Bridge Structure (Not Overhead)': 'Fixed Object - Off Road',
                'Overhead Structure (Not Bridge)': 'Fixed Object - Off Road',
                'Railroad Crossing Equipment': 'Fixed Object - Off Road',
                'Other Fixed Object (Describe in Narrative)': 'Fixed Object - Off Road',
                // Fixed objects - in road
                'Vehicle Debris or Cargo': 'Fixed Object in Road',
                // Other
                'Parked Motor Vehicle': 'Other',
                'Other Non-Fixed Object (Describe in Narrative)': 'Other',
                'Other Non-Fixed Object Describe in Narrative)': 'Other'
            };
            return mapping[ct] || ct || 'Unknown';
        },

        // ── Helper: Map Road Description to intersection type ──
        _mapIntersectionType(roadDesc) {
            const rd = (roadDesc || '').trim();
            const mapping = {
                'Non-Intersection': 'Non-Intersection',
                'At Intersection': 'Intersection',
                'Intersection Related': 'Intersection',
                'Driveway Access Related': 'Driveway',
                'Ramp': 'Ramp',
                'Ramp-related': 'Ramp',
                'Roundabout': 'Roundabout',
                'Express/Managed/HOV Lane': 'Non-Intersection',
                'Crossover-Related ': 'Intersection',
                'Auxiliary Lane': 'Non-Intersection',
                'Alley Related': 'Intersection',
                'Railroad Crossing Related': 'Railroad Crossing',
                'Mid-Block Crosswalk': 'Intersection'
            };
            return mapping[rd] || rd || 'Unknown';
        },

        // ── Helper: Build route name from Colorado fields ──
        _buildRouteName(row) {
            const system = (row['System Code'] || '').trim();
            const rdNum = (row['Rd_Number'] || '').trim();
            const loc1 = (row['Location 1'] || '').trim();

            if (system === 'Interstate Highway') {
                // Extract interstate number from Rd_Number (e.g., "025A" → "I-25")
                const num = rdNum.replace(/^0+/, '').replace(/[A-Z]$/i, '');
                return `I-${num}`;
            }
            if (system === 'State Highway') {
                // State Highway: use Location 1 name if available, else CO-{number}
                if (loc1) return loc1;
                const num = rdNum.replace(/^0+/, '').replace(/[A-Z]$/i, '');
                return `CO-${num}`;
            }
            if (system === 'Frontage Road') {
                if (loc1) return `${loc1} (Frontage)`;
                return `Frontage Rd ${rdNum}`;
            }
            // City Street or County Road: use Location 1
            return loc1 || `Road ${rdNum}`;
        },

        // ── Helper: Map System Code to Virginia-style SYSTEM values ──
        // This enables the existing filter logic to work
        _mapRoadSystem(systemCode) {
            const sc = (systemCode || '').trim();
            const mapping = {
                'City Street': 'Non-DOT secondary',
                'County Road': 'Non-DOT secondary',
                'State Highway': 'Primary',
                'Interstate Highway': 'Interstate',
                'Frontage Road': 'Secondary',
                'Non Crash': 'Non-DOT secondary'
            };
            return mapping[sc] || 'Non-DOT secondary';
        },

        // ── Helper: Build node ID from intersection data ──
        _buildNodeId(row) {
            const rd = (row['Road Description'] || '').trim();
            const loc1 = (row['Location 1'] || '').trim();
            const loc2 = (row['Location 2'] || '').trim();
            const link = (row['Link'] || '').trim();

            // Only create node for intersection crashes
            if (rd === 'At Intersection' || rd === 'Intersection Related' || rd === 'Roundabout') {
                if (loc1 && loc2) {
                    // Create a stable node ID from the two road names
                    const roads = [loc1, loc2].sort();
                    return `${roads[0]} & ${roads[1]}`;
                }
            }
            return '';
        },

        // ── Helper: Map alignment from curves + grade ──
        _mapAlignment(curves, grade) {
            const parts = [];
            if (curves && curves !== 'Straight') parts.push(curves);
            if (grade && grade !== 'Level') parts.push(grade);
            return parts.length > 0 ? parts.join(', ') : 'Straight/Level';
        },

        // ── Helper: Check non-motorist type ──
        _checkNonMotoristType(row, type) {
            const tu1 = (row['TU-1 NM Type'] || '').trim();
            const tu2 = (row['TU-2 NM Type'] || '').trim();
            return tu1.includes(type) || tu2.includes(type);
        },

        // ── Helper: Check pedestrian (NM Type + Crash Type + MHE) ──
        _checkPedestrian(row) {
            if (this._checkNonMotoristType(row, 'Pedestrian')) return true;
            const ct = (row['Crash Type'] || '').trim();
            const mhe = (row['MHE'] || '').trim();
            return ct === 'Pedestrian' || mhe === 'Pedestrian'
                || ct === 'School Age To/From School' || mhe === 'School Age To/From School';
        },

        // ── Helper: Check bicycle (NM Type + Crash Type + MHE) ──
        _checkBicycle(row) {
            if (this._checkNonMotoristType(row, 'Bicycle')) return true;
            const ct = (row['Crash Type'] || '').trim();
            const mhe = (row['MHE'] || '').trim();
            return ct.includes('Bicycle') || mhe.includes('Bicycle');
        },

        // ── Helper: Check vehicle type ──
        _checkVehicleType(row, type) {
            const tu1 = (row['TU-1 Type'] || '').trim();
            const tu2 = (row['TU-2 Type'] || '').trim();
            return tu1.includes(type) || tu2.includes(type);
        },

        // ── Helper: Check alcohol ──
        _checkAlcohol(row) {
            const positiveValues = ['Yes - SFST', 'Yes - BAC', 'Yes - Both', 'Yes - Observation'];
            const tu1 = (row['TU-1 Alcohol Suspected'] || '').trim();
            const tu2 = (row['TU-2 Alcohol Suspected'] || '').trim();
            return positiveValues.includes(tu1) || positiveValues.includes(tu2);
        },

        // ── Helper: Check speed ──
        _checkSpeed(row) {
            const speedActions = ['Too Fast for Conditions', 'Exceeded Speed Limit', 'Exceeded Safe/Posted Speed', 'Speeding'];
            const tu1a = (row['TU-1 Driver Action'] || '').trim();
            const tu2a = (row['TU-2 Driver Action'] || '').trim();
            const tu1h = (row['TU-1 Human Contributing Factor'] || '').trim();
            const tu2h = (row['TU-2 Human Contributing Factor'] || '').trim();
            return speedActions.includes(tu1a) || speedActions.includes(tu2a)
                || speedActions.includes(tu1h) || speedActions.includes(tu2h);
        },

        // ── Helper: Check distracted ──
        _checkDistracted(row) {
            const distractedValues = [
                'Distracted', 'Cell Phone', 'Inattention',
                'Distracted - Cell Phone/Electronic Device',
                'Distracted - Other', 'Inattentive/Distracted'
            ];
            const fields = [
                row['TU-1 Driver Action'], row['TU-2 Driver Action'],
                row['TU-1 Human Contributing Factor'], row['TU-2 Human Contributing Factor']
            ];
            return fields.some(f => {
                const v = (f || '').trim();
                return distractedValues.some(d => v.includes(d));
            });
        },

        // ── Helper: Check drowsy ──
        _checkDrowsy(row) {
            const drowsyValues = ['Asleep or Fatigued', 'Asleep/Fatigued', 'Fatigued/Asleep', 'Ill/Asleep/Fatigued', 'Fatigued', 'Asleep'];
            const tu1 = (row['TU-1 Human Contributing Factor'] || '').trim();
            const tu2 = (row['TU-2 Human Contributing Factor'] || '').trim();
            return drowsyValues.some(v => tu1.includes(v) || tu2.includes(v));
        },

        // ── Helper: Check drugs ──
        _checkDrugs(row) {
            const positiveValues = ['Yes - Observation', 'Yes - SFST', 'Yes - Both', 'Yes - Test Results'];
            const fields = [
                row['TU-1  Marijuana Suspected'], row['TU-2 Marijuana Suspected'],
                row['TU-1 Other Drugs Suspected '], row['TU-2 Other Drugs Suspected ']
            ];
            return fields.some(f => positiveValues.includes((f || '').trim()));
        },

        // ── Helper: Check age range ──
        _checkAge(row, minAge, maxAge) {
            const tu1 = parseInt(row['TU-1 Age']) || 0;
            const tu2 = parseInt(row['TU-2 Age']) || 0;
            return (tu1 >= minAge && tu1 <= maxAge) || (tu2 >= minAge && tu2 <= maxAge);
        },

        // ── Helper: Check unrestrained ──
        _checkUnrestrained(row) {
            const unrestrained = ['Not Used', 'Improperly Used'];
            const tu1 = (row['TU-1 Safety restraint Use'] || '').trim();
            const tu2 = (row['TU-2 Safety restraint Use'] || '').trim();
            return unrestrained.includes(tu1) || unrestrained.includes(tu2);
        },

        // ── Helper: Check nighttime ──
        _isNighttime(lighting) {
            const night = ['Dark – Lighted', 'Dark – Unlighted', 'Dark - Lighted', 'Dark - Unlighted'];
            return night.includes((lighting || '').trim());
        },

        // ── Helper: Check animal involvement ──
        _checkAnimal(row) {
            const wildAnimal = (row['Wild Animal'] || '').trim();
            if (wildAnimal !== '') return true;
            const ct = (row['Crash Type'] || row['MHE'] || '').trim();
            return ct === 'Wild Animal' || ct === 'Domestic Animal';
        },

        // ── Helper: Check guardrail involvement ──
        _checkGuardrail(row) {
            const fields = [row['MHE'] || '', row['Crash Type'] || '', row['First HE'] || ''];
            return fields.some(f => f.includes('Guardrail'));
        },

        // ── Helper: Check large truck involvement ──
        _checkLargeTruck(row) {
            const truckTypes = ['Medium/Heavy Truck', 'Truck/Tractor', 'Truck Tractor',
                'Semi-Trailer', 'Bus', 'Working Vehicle', 'Farm Equipment'];
            const tu1 = (row['TU-1 Type'] || '').trim();
            const tu2 = (row['TU-2 Type'] || '').trim();
            return truckTypes.some(t => tu1.includes(t) || tu2.includes(t));
        },

        // ── Helper: Derive road departure type from crash data ──
        _deriveRoadDepartureType(row) {
            const mhe = (row['MHE'] || '').trim();
            const firstHE = (row['First HE'] || '').trim();
            const rdIndicators = ['Tree', 'Utility Pole', 'Guard Rail', 'Guardrail', 'Fence',
                'Embankment', 'Ditch', 'Concrete Highway Barrier', 'Cable Rail', 'Culvert',
                'Overturning', 'Rollover', 'Large Rocks', 'Sign', 'Mailbox',
                'Crash Cushion', 'Wall or Building', 'Barricade', 'Bridge Structure'];
            if (rdIndicators.some(ind => mhe.includes(ind) || firstHE.includes(ind))) {
                return 'RD_UNKNOWN';
            }
            return 'NOT_RD';
        },

        // ── Helper: Derive intersection analysis from road description ──
        _deriveIntersectionAnalysis(row) {
            const rd = (row['Road Description'] || '').trim();
            if (rd === 'At Intersection' || rd === 'Intersection Related' || rd === 'Roundabout' ||
                rd === 'Alley Related' || rd === 'Mid-Block Crosswalk') {
                return 'Urban Intersection';
            }
            return 'Not Intersection';
        },

        // ── Helper: Calculate speed differential ──
        _calcSpeedDiff(row) {
            const limit = parseInt(row['TU-1 Speed Limit']) || 0;
            const est = parseInt(row['TU-1 Estimated Speed']) || 0;
            if (limit > 0 && est > 0) return String(est - limit);
            return '';
        }
    };

    // ─── Virginia Normalizer ───
    // Virginia data already matches internal format, just pass through
    const VIRGINIA_NORMALIZER = {
        normalizeRow(row) {
            return row; // Virginia format IS the internal format
        }
    };

    // ─── Normalizer Registry ───
    const NORMALIZERS = {
        colorado: COLORADO_NORMALIZER,
        virginia: VIRGINIA_NORMALIZER
    };

    // ─── Public API ───
    return {
        /**
         * Detect which state's data is in the CSV by examining headers.
         * @param {string[]} headers - Array of CSV column header names
         * @returns {string} State key (e.g., 'colorado', 'virginia')
         */
        detect(headers) {
            return detect(headers);
        },

        /**
         * Normalize a single CSV row from the detected state's format
         * into the CRASH LENS internal format (Virginia-compatible).
         * @param {Object} row - Raw CSV row object (column name → value)
         * @returns {Object} Normalized row with Virginia-compatible column names
         */
        normalizeRow(row) {
            const state = detectedState || ((typeof appConfig !== 'undefined' && appConfig?.defaultState) || 'colorado');
            const normalizer = NORMALIZERS[state];
            if (!normalizer) {
                console.error(`[StateAdapter] No normalizer for state: ${state}`);
                return row;
            }
            return normalizer.normalizeRow(row);
        },

        /**
         * Get the currently detected state.
         * @returns {string|null} State key or null if not detected
         */
        getDetectedState() {
            return detectedState;
        },

        /**
         * Check if the loaded data was pre-normalized by the Python pipeline.
         * @returns {boolean}
         */
        isDataPreNormalized() {
            return isPreNormalized;
        },

        /**
         * Get display name for detected/selected state.
         * @returns {string}
         */
        getStateName() {
            // Check FIPS database first (for manual selections)
            if (manualStateFips && typeof FIPSDatabase !== 'undefined') {
                const state = FIPSDatabase.getState(manualStateFips);
                if (state) return `${state.name} (${state.dotName})`;
            }
            if (!detectedState) return 'Unknown';
            return STATE_SIGNATURES[detectedState]?.displayName || detectedState;
        },

        /**
         * Check if data normalization is needed.
         * Returns false for Virginia data (already in internal format) and for
         * pre-normalized data (processed by the Python pipeline with _co_* columns).
         * @returns {boolean}
         */
        needsNormalization() {
            if (isPreNormalized) return false;
            return detectedState !== null && detectedState !== 'virginia';
        },

        /**
         * Check if a non-default state has been selected (either detected or manual).
         * Used by applyStateAdapterConfig to decide whether to apply overrides.
         * Reads default state from appConfig.defaultState instead of hardcoding.
         * @returns {boolean}
         */
        hasStateOverride() {
            const defaultState = (typeof appConfig !== 'undefined' && appConfig?.defaultState) || 'colorado';
            const defaultFips = (typeof appConfig !== 'undefined' && appConfig?.states?.[defaultState]?.fips) || '08';
            return (manualStateFips !== null && manualStateFips !== defaultFips) ||
                   (detectedState !== null && detectedState !== defaultState);
        },

        /**
         * Get the road system filter profiles for the detected state.
         * @returns {Object} Filter profiles
         */
        getFilterProfiles() {
            // Try to load filter profiles from state config (loaded at detection time)
            if (stateConfig?.roadSystems?.filterProfiles) {
                return stateConfig.roadSystems.filterProfiles;
            }
            // Fallback: hardcoded profiles for states without config loaded
            if (detectedState === 'colorado') {
                return {
                    countyOnly: {
                        name: "County Roads Only",
                        systemValues: ["Non-DOT secondary"]
                    },
                    cityOnly: {
                        name: "City Roads Only",
                        systemCodeValues: ["City Street"]
                    },
                    countyPlusVDOT: {
                        name: "All Roads (No Interstate)",
                        systemValues: ["Non-DOT secondary", "Primary", "Secondary"]
                    },
                    allRoads: {
                        name: "All Roads (Including Interstate)",
                        systemValues: ["Non-DOT secondary", "Primary", "Secondary", "Interstate"]
                    }
                };
            }
            // Default: return null (app uses config.json filterProfiles)
            return null;
        },

        /**
         * Get available state signatures (states with CSV normalizers).
         * @returns {Object} State signatures
         */
        getAvailableStates() {
            return { ...STATE_SIGNATURES };
        },

        /**
         * Get all states from FIPS database (for the state selector UI).
         * Returns all 50 states + DC with FIPS, name, abbreviation.
         * @returns {Object[]|null} Array of state objects, or null if FIPSDatabase not loaded
         */
        getAllSelectableStates() {
            if (typeof FIPSDatabase !== 'undefined') {
                return FIPSDatabase.getAllStates();
            }
            // Fallback: build from appConfig.states registry if available, else signatures
            if (typeof appConfig !== 'undefined' && appConfig?.states) {
                return Object.entries(appConfig.states).map(([key, st]) => ({
                    fips: st.fips || '',
                    name: st.name || key,
                    abbr: st.abbreviation || ''
                }));
            }
            return Object.entries(STATE_SIGNATURES).map(([key, sig]) => ({
                fips: key === 'colorado' ? '08' : key === 'virginia' ? '51' : '',
                name: sig.displayName,
                abbr: key === 'colorado' ? 'CO' : key === 'virginia' ? 'VA' : ''
            }));
        },

        /**
         * Manually set the state (skip auto-detection).
         * @param {string} stateKey - State key (e.g., 'colorado')
         */
        setState(stateKey) {
            if (STATE_SIGNATURES[stateKey]) {
                detectedState = stateKey;
                manualStateFips = null;
                dynamicGeoConfig = null;
                console.log(`[StateAdapter] State manually set to: ${STATE_SIGNATURES[stateKey].displayName}`);
            } else {
                console.error(`[StateAdapter] Unknown state key: ${stateKey}. Use setStateByFips() for arbitrary states.`);
            }
        },

        /**
         * Set state by FIPS code (for the state selector UI).
         * Works for ANY US state, not just those with CSV normalizers.
         * Uses FIPSDatabase to auto-configure map, FIPS, jurisdictions.
         * @param {string} stateFips - Two-digit state FIPS (e.g., '08', '51', '48')
         * @returns {Promise<Object|null>} The generated geoConfig, or null on failure
         */
        async setStateByFips(stateFips) {
            if (typeof FIPSDatabase === 'undefined') {
                console.error('[StateAdapter] FIPSDatabase not loaded. Cannot set state by FIPS.');
                return null;
            }

            const padded = String(stateFips).padStart(2, '0');
            const stateInfo = FIPSDatabase.getState(padded);
            if (!stateInfo) {
                console.error(`[StateAdapter] Unknown state FIPS: ${padded}`);
                return null;
            }

            // Check if this maps to a known normalizer state (config-driven)
            let fipsToKey = { '08': 'colorado', '51': 'virginia' }; // fallback
            if (typeof appConfig !== 'undefined' && appConfig?.states) {
                fipsToKey = {};
                for (const [key, st] of Object.entries(appConfig.states)) {
                    if (st.fips) fipsToKey[st.fips] = key;
                }
            }
            if (fipsToKey[padded]) {
                detectedState = fipsToKey[padded];
            }

            manualStateFips = padded;
            console.log(`[StateAdapter] State set by FIPS: ${stateInfo.name} (${padded})`);

            // For the default state: use the cached config.json jurisdictions (rich data with bbox, education, jurisCode)
            // These are cached at startup in window._defaultStateConfigJurisdictions to survive state switching
            const defaultStateFips = (typeof appConfig !== 'undefined' && appConfig?.states?.[appConfig?.defaultState]?.fips) || '08';
            if (padded === defaultStateFips) {
                const cachedJurisdictions = (typeof window !== 'undefined' && (window._defaultStateConfigJurisdictions || window._virginiaConfigJurisdictions))
                    ? (window._defaultStateConfigJurisdictions || window._virginiaConfigJurisdictions)
                    : null;
                if (cachedJurisdictions && Object.keys(cachedJurisdictions).length > 0) {
                    // Filter cached jurisdictions to only include entries for the target state.
                    // config.json may contain jurisdictions for multiple states (e.g., VA + CO).
                    // We must not pass all of them to buildGeoConfig or downstream code will
                    // see accumulated counties from every state in the config.
                    const targetAbbr = stateInfo.abbr;
                    const hasTaggedForTarget = Object.values(cachedJurisdictions).some(j => j.state === targetAbbr);
                    let filteredJurisdictions;
                    if (hasTaggedForTarget) {
                        // Entries explicitly tagged for this state exist — keep only those
                        filteredJurisdictions = {};
                        for (const [key, val] of Object.entries(cachedJurisdictions)) {
                            if (val.state === targetAbbr) {
                                filteredJurisdictions[key] = val;
                            }
                        }
                    } else {
                        // No entries tagged for this state — untagged entries belong to it
                        filteredJurisdictions = {};
                        for (const [key, val] of Object.entries(cachedJurisdictions)) {
                            if (!val.state) {
                                filteredJurisdictions[key] = val;
                            }
                        }
                    }
                    console.log(`[StateAdapter] Using cached config.json jurisdictions for ${stateInfo.name} (${Object.keys(filteredJurisdictions).length} of ${Object.keys(cachedJurisdictions).length} entries after filtering)`);
                    dynamicGeoConfig = FIPSDatabase.buildGeoConfig(padded, filteredJurisdictions);
                    // Override defaultJurisdiction with the config-driven value (e.g., 'henrico' for Virginia)
                    // buildGeoConfig uses Object.keys()[0] which gives alphabetical first (e.g., 'accomack'),
                    // but the state config knows which jurisdiction has data available
                    const stateKey = fipsToKey[padded];
                    const configDefault = stateKey && appConfig?.states?.[stateKey]?.defaultJurisdiction;
                    if (configDefault && filteredJurisdictions[configDefault]) {
                        dynamicGeoConfig.defaultJurisdiction = configDefault;
                        console.log(`[StateAdapter] Default jurisdiction overridden to config value: ${configDefault}`);
                    }
                    try { localStorage.setItem('selectedStateFips', padded); } catch(e) {}
                    return dynamicGeoConfig;
                }
            }

            // Load counties from embedded DB or TIGERweb
            const counties = await FIPSDatabase.getCounties(padded);

            // For Colorado, merge with bundled detailed jurisdictions if available
            if (padded === '08' && counties) {
                // Preserve the detailed Douglas County config from the hardcoded data
                const hardcodedConfig = this._getHardcodedColoradoJurisdictions();
                for (const [key, val] of Object.entries(hardcodedConfig)) {
                    counties[key] = val; // Overwrite with richer data
                }
            }

            // Build dynamic geo config
            dynamicGeoConfig = FIPSDatabase.buildGeoConfig(padded, counties);

            // Override defaultJurisdiction with the config-driven value for all states
            const stateKeyForDefault = fipsToKey[padded];
            const configDefaultJurisdiction = stateKeyForDefault && appConfig?.states?.[stateKeyForDefault]?.defaultJurisdiction;
            if (configDefaultJurisdiction && dynamicGeoConfig.jurisdictions?.[configDefaultJurisdiction]) {
                dynamicGeoConfig.defaultJurisdiction = configDefaultJurisdiction;
                console.log(`[StateAdapter] Default jurisdiction overridden to config value: ${configDefaultJurisdiction}`);
            }

            // Save selection
            try { localStorage.setItem('selectedStateFips', padded); } catch(e) {}

            return dynamicGeoConfig;
        },

        /**
         * Get the manually selected state FIPS (from UI dropdown).
         * @returns {string|null}
         */
        getManualStateFips() {
            return manualStateFips;
        },

        /**
         * Get geographic configuration for the current state.
         * Priority: dynamic config (from setStateByFips) > hardcoded > null
         * @returns {Object|null} Geographic config object, or null if Virginia defaults
         */
        getGeoConfig() {
            // If dynamic config was built from FIPSDatabase, use it
            if (dynamicGeoConfig) {
                return dynamicGeoConfig;
            }

            // Fallback to hardcoded Colorado config
            if (detectedState === 'colorado') {
                return {
                    stateFips: '08',
                    stateName: 'Colorado',
                    stateAbbr: 'CO',
                    coordinateBounds: {
                        latMin: 36.9, latMax: 41.1,
                        lonMin: -109.1, lonMax: -101.9
                    },
                    defaultJurisdiction: 'douglas',
                    defaultMapCenter: [39.3298, -104.9253],
                    defaultMapZoom: 11,
                    appSubtitle: 'Colorado Crash Analysis Tool',
                    districtLabel: 'Census Subdivisions',
                    jurisdictions: this._getHardcodedColoradoJurisdictions()
                };
            }
            // Virginia: return null (app uses existing config.json defaults)
            return null;
        },

        /**
         * Internal: Get hardcoded Colorado jurisdictions (detailed data).
         * @private
         */
        _getHardcodedColoradoJurisdictions() {
            return {
                douglas: {
                    name: "Douglas County", type: "county", fips: "035",
                    stateCountyFips: "08035",
                    namePatterns: ["DOUGLAS", "Douglas", "Douglas County"],
                    mapCenter: [39.3298, -104.9253], mapZoom: 11,
                    bbox: [-105.0543, 39.1298, -104.6014, 39.5624],
                    maintainsOwnRoads: true
                },
                arapahoe: {
                    name: "Arapahoe County", type: "county", fips: "005",
                    stateCountyFips: "08005",
                    namePatterns: ["ARAPAHOE", "Arapahoe", "Arapahoe County"],
                    mapCenter: [39.6498, -104.3389], mapZoom: 10,
                    bbox: [-105.0534, 39.5638, -103.7064, 39.7404],
                    maintainsOwnRoads: true
                },
                jefferson: {
                    name: "Jefferson County", type: "county", fips: "059",
                    stateCountyFips: "08059",
                    namePatterns: ["JEFFERSON", "Jefferson", "Jefferson County"],
                    mapCenter: [39.5866, -105.2508], mapZoom: 10,
                    bbox: [-105.6798, 39.3677, -105.0534, 39.8282],
                    maintainsOwnRoads: true
                },
                elpaso: {
                    name: "El Paso County", type: "county", fips: "041",
                    stateCountyFips: "08041",
                    namePatterns: ["EL PASO", "El Paso", "El Paso County"],
                    mapCenter: [38.8339, -104.7581], mapZoom: 10,
                    bbox: [-105.0286, 38.5157, -104.0534, 39.1298],
                    maintainsOwnRoads: true
                },
                denver: {
                    name: "Denver County", type: "county", fips: "031",
                    stateCountyFips: "08031",
                    namePatterns: ["DENVER", "Denver", "Denver County"],
                    mapCenter: [39.7392, -104.9903], mapZoom: 12,
                    bbox: [-105.1098, 39.6144, -104.5996, 39.9142],
                    maintainsOwnRoads: true
                },
                adams: {
                    name: "Adams County", type: "county", fips: "001",
                    stateCountyFips: "08001",
                    namePatterns: ["ADAMS", "Adams", "Adams County"],
                    mapCenter: [39.8737, -104.7624], mapZoom: 10,
                    bbox: [-105.0534, 39.7404, -104.4610, 40.0015],
                    maintainsOwnRoads: true
                }
            };
        },

        /**
         * Get Colorado-specific road system info for a row.
         * Useful for displaying original classification in UI.
         * @param {Object} normalizedRow - A normalized row
         * @returns {Object} Original road system details
         */
        getOriginalRoadSystem(normalizedRow) {
            if (detectedState !== 'colorado') return null;
            return {
                systemCode: normalizedRow['_co_system_code'] || '',
                rdNumber: normalizedRow['_co_rd_number'] || '',
                location1: normalizedRow['_co_location1'] || '',
                location2: normalizedRow['_co_location2'] || '',
                link: normalizedRow['_co_link'] || '',
                city: normalizedRow['_co_city'] || '',
                agency: normalizedRow['_co_agency'] || ''
            };
        },

        /**
         * Get the data subdirectory for the current state.
         * States with their own data folder (e.g., Colorado → "CDOT") return that folder name.
         * Virginia and unknown states return null (data lives in the root data/ dir).
         * @returns {string|null} Subdirectory name or null for root
         */
        getDataDir() {
            // Try appConfig.states registry first (config-driven)
            if (typeof appConfig !== 'undefined' && appConfig?.states) {
                if (detectedState && appConfig.states[detectedState]?.dataDir) {
                    return appConfig.states[detectedState].dataDir;
                }
                // Check manual FIPS against states registry
                if (manualStateFips) {
                    for (const [key, st] of Object.entries(appConfig.states)) {
                        if (st.fips === manualStateFips && st.dataDir) return st.dataDir;
                    }
                }
            }
            // Fallback to hardcoded mapping for backward compatibility
            const STATE_DATA_DIRS = { 'colorado': 'CDOT' };
            if (detectedState && STATE_DATA_DIRS[detectedState]) {
                return STATE_DATA_DIRS[detectedState];
            }
            const fipsToKey = { '08': 'colorado' };
            if (manualStateFips && fipsToKey[manualStateFips]) {
                return STATE_DATA_DIRS[fipsToKey[manualStateFips]] || null;
            }
            return null;
        }
    };
})();

// Export for Node.js testing (ignored in browser)
if (typeof module !== 'undefined' && module.exports) {
    module.exports = StateAdapter;
}
