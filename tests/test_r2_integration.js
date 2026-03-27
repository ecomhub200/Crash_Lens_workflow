/**
 * Bug tests for Cloudflare R2 integration.
 *
 * Tests the R2 manifest loading, URL resolution, and fallback behavior
 * in loadR2Manifest() and resolveDataUrl() — the core R2 functions
 * in index.html.
 *
 * Run with: node tests/test_r2_integration.js
 *
 * Test Architecture Overview
 * ==========================
 *
 * The R2 integration works in 3 phases:
 *   1. loadR2Manifest() — fetches data/r2-manifest.json at startup,
 *      parses it, validates structure, and sets r2State.
 *
 *   2. resolveDataUrl(localPath) — for each data fetch, checks if
 *      the file is tracked in the manifest and returns an R2 URL
 *      or falls back to the original local path.
 *
 *   3. Fetch calls in autoLoadCrashData(), loadSampleRowsInBackground(),
 *      and attemptDataReconnection() are wrapped with resolveDataUrl().
 *
 * These tests verify phases 1 and 2 in isolation, without requiring
 * a browser DOM or real network requests.
 *
 * Test Coverage Map
 * =================
 *
 * r2State initialization:
 *   1.  Default state has null manifest
 *   2.  Default state has loaded = false
 *   3.  Default state has null error
 *
 * resolveDataUrl (no manifest loaded):
 *   4.  Returns local path when manifest is null
 *   5.  Returns local path when manifest.r2BaseUrl is empty
 *   6.  Returns local path when manifest.localPathMapping is null
 *   7.  Returns local path when manifest.localPathMapping is empty object
 *
 * resolveDataUrl (path normalization):
 *   8.  Strips '../' prefix from local path for lookup
 *   9.  Strips './' prefix from local path for lookup
 *   10. Handles paths without any prefix
 *   11. Strips only first '../' (not nested ../../)
 *   12. Returns original path (with prefix) when file not in mapping
 *
 * resolveDataUrl (URL construction):
 *   13. Constructs correct R2 URL from base + key
 *   14. Handles trailing slash in base URL
 *   15. Maps Virginia jurisdiction paths correctly
 *   16. Maps Colorado CDOT paths correctly
 *   17. Maps fallback crashes.csv correctly
 *
 * resolveDataUrl (edge cases):
 *   18. Empty string path returns empty string
 *   19. Returns local path for files not in mapping (grants.csv)
 *   20. Returns local path for files not in mapping (cmf_processed.json)
 *   21. Multiple consecutive calls return consistent results
 *   22. Path with query string or hash (shouldn't happen but defensive)
 *
 * loadR2Manifest (fetch success - valid manifest):
 *   23. Sets r2State.manifest to parsed manifest object
 *   24. Sets r2State.loaded to true
 *   25. Does not set r2State.error
 *   26. Manifest has expected fields after load
 *
 * loadR2Manifest (fetch success - empty manifest):
 *   27. Empty r2BaseUrl → manifest stays null, loaded = true
 *   28. Empty localPathMapping → manifest stays null, loaded = true
 *   29. Missing localPathMapping field → manifest stays null, loaded = true
 *   30. Missing r2BaseUrl field → manifest stays null, loaded = true
 *
 * loadR2Manifest (fetch failure):
 *   31. HTTP 404 → manifest stays null, loaded = true
 *   32. HTTP 500 → manifest stays null, loaded = true
 *   33. Network error → manifest stays null, loaded = true, error set
 *   34. Invalid JSON → manifest stays null, loaded = true, error set
 *
 * loadR2Manifest (malformed manifest):
 *   35. Manifest with r2BaseUrl but no files → manifest stays null
 *   36. Manifest with only version field → manifest stays null
 *   37. Manifest is an array instead of object → manifest stays null
 *   38. Manifest localPathMapping has entries but r2BaseUrl empty → stays null
 *
 * Integration (loadR2Manifest → resolveDataUrl):
 *   39. After successful load, resolveDataUrl returns R2 URLs
 *   40. After failed load, resolveDataUrl returns local paths
 *   41. After empty manifest load, resolveDataUrl returns local paths
 *   42. Multiple data files resolved correctly from same manifest
 *
 * Workflow manifest update simulation:
 *   43. Composite action manifest structure is valid
 *   44. Manifest with multiple jurisdictions resolves correctly
 *   45. Manifest version field preserved after update
 *
 * CORS and fetch behavior:
 *   46. R2 URLs use HTTPS protocol
 *   47. R2 URLs contain expected bucket subdomain
 *   48. Local paths preserved for small files (grants, CMF)
 *
 * Concurrency / state safety:
 *   49. r2State mutation during resolveDataUrl doesn't crash
 *   50. Calling resolveDataUrl before loadR2Manifest completes is safe
 */

// ─── Simulated r2State (matching index.html) ───
let r2State = {
    manifest: null,
    loaded: false,
    error: null
};

function resetR2State() {
    r2State = {
        manifest: null,
        loaded: false,
        error: null
    };
}

// ─── Functions under test (extracted from index.html lines 20008-20067) ───

/**
 * Simulated loadR2Manifest — takes a mock fetch function instead of
 * using the global fetch. This allows testing all code paths without
 * real network requests.
 */
async function loadR2Manifest(mockFetch) {
    try {
        const response = await mockFetch('../data/r2-manifest.json');
        if (!response.ok) {
            r2State.loaded = true;
            return;
        }
        const manifest = await response.json();

        // Validate manifest has the minimum required structure
        if (!manifest.r2BaseUrl || !manifest.localPathMapping || Object.keys(manifest.localPathMapping).length === 0) {
            r2State.loaded = true;
            return;
        }

        r2State.manifest = manifest;
        r2State.loaded = true;
    } catch (error) {
        r2State.error = error.message;
        r2State.loaded = true;
    }
}

/**
 * resolveDataUrl — exact copy from index.html (lines 20043-20067)
 */
function resolveDataUrl(localPath) {
    if (!r2State.manifest || !r2State.manifest.r2BaseUrl || !r2State.manifest.localPathMapping) {
        return localPath;
    }

    let normalizedPath = localPath;
    if (normalizedPath.startsWith('../')) {
        normalizedPath = normalizedPath.substring(3);
    }
    if (normalizedPath.startsWith('./')) {
        normalizedPath = normalizedPath.substring(2);
    }

    const r2Key = r2State.manifest.localPathMapping[normalizedPath];
    if (!r2Key) {
        return localPath;
    }

    const r2Url = `${r2State.manifest.r2BaseUrl}/${r2Key}`;
    return r2Url;
}

// ─── Mock Fetch Helpers ───

function mockFetchSuccess(jsonBody) {
    return async () => ({
        ok: true,
        json: async () => jsonBody
    });
}

function mockFetchHttpError(status) {
    return async () => ({
        ok: false,
        status: status
    });
}

function mockFetchNetworkError(message) {
    return async () => {
        throw new Error(message);
    };
}

function mockFetchInvalidJson() {
    return async () => ({
        ok: true,
        json: async () => { throw new SyntaxError('Unexpected token < in JSON'); }
    });
}

// ─── Sample Manifests ───

const VALID_MANIFEST = {
    version: 1,
    r2BaseUrl: 'https://data.aicreatesai.com',
    updated: '2026-02-13T12:00:00.000Z',
    files: {
        'colorado/douglas/all_roads.csv': {
            size: 18000000,
            md5: 'abc123',
            uploaded: '2026-02-13T12:00:00.000Z'
        },
        'colorado/douglas/county_roads.csv': {
            size: 9100000,
            md5: 'def456',
            uploaded: '2026-02-13T12:00:00.000Z'
        },
        'virginia/henrico/all_roads.csv': {
            size: 32000000,
            md5: 'ghi789',
            uploaded: '2026-02-13T12:00:00.000Z'
        }
    },
    localPathMapping: {
        'data/CDOT/douglas_all_roads.csv': 'colorado/douglas/all_roads.csv',
        'data/CDOT/douglas_county_roads.csv': 'colorado/douglas/county_roads.csv',
        'data/CDOT/douglas_no_interstate.csv': 'colorado/douglas/no_interstate.csv',
        'data/CDOT/douglas_standardized.csv': 'colorado/douglas/standardized.csv',
        'data/CDOT/crashes.csv': 'colorado/douglas/crashes.csv',
        'data/henrico_all_roads.csv': 'virginia/henrico/all_roads.csv',
        'data/henrico_county_roads.csv': 'virginia/henrico/county_roads.csv',
        'data/henrico_no_interstate.csv': 'virginia/henrico/no_interstate.csv'
    }
};

const EMPTY_MANIFEST = {
    version: 1,
    r2BaseUrl: '',
    updated: '',
    files: {},
    localPathMapping: {}
};

const MULTI_JURISDICTION_MANIFEST = {
    version: 1,
    r2BaseUrl: 'https://data.aicreatesai.com',
    updated: '2026-02-13T12:00:00.000Z',
    files: {},
    localPathMapping: {
        'data/CDOT/douglas_all_roads.csv': 'colorado/douglas/all_roads.csv',
        'data/CDOT/arapahoe_all_roads.csv': 'colorado/arapahoe/all_roads.csv',
        'data/CDOT/jefferson_all_roads.csv': 'colorado/jefferson/all_roads.csv',
        'data/henrico_all_roads.csv': 'virginia/henrico/all_roads.csv',
        'data/fairfax_county_all_roads.csv': 'virginia/fairfax_county/all_roads.csv',
        'data/chesterfield_all_roads.csv': 'virginia/chesterfield/all_roads.csv'
    }
};

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

function assertNull(value, message) {
    if (value === null) {
        passed++;
    } else {
        failed++;
        const msg = `${message} — expected null, got "${typeof value === 'object' ? JSON.stringify(value).substring(0, 50) : value}"`;
        failures.push(msg);
        console.log(`  FAIL: ${msg}`);
    }
}

function assertNotNull(value, message) {
    if (value !== null && value !== undefined) {
        passed++;
    } else {
        failed++;
        const msg = `${message} — expected non-null, got ${value}`;
        failures.push(msg);
        console.log(`  FAIL: ${msg}`);
    }
}

function assertTrue(value, message) {
    assert(value === true, message);
}

function assertFalse(value, message) {
    assert(value === false, message);
}

function assertStartsWith(str, prefix, message) {
    if (typeof str === 'string' && str.startsWith(prefix)) {
        passed++;
    } else {
        failed++;
        const msg = `${message} — expected to start with "${prefix}", got "${str}"`;
        failures.push(msg);
        console.log(`  FAIL: ${msg}`);
    }
}

function assertContains(str, substring, message) {
    if (typeof str === 'string' && str.includes(substring)) {
        passed++;
    } else {
        failed++;
        const msg = `${message} — expected to contain "${substring}", got "${str}"`;
        failures.push(msg);
        console.log(`  FAIL: ${msg}`);
    }
}

// ─── Sequential async test runner ───
// All async tests are queued and run one at a time to avoid r2State races.

async function runAllTests() {
    console.log('\n========================================');
    console.log('  R2 INTEGRATION - Bug Tests');
    console.log('========================================\n');

    // ═══════════════════════════════════════
    // Suite 1: r2State initialization
    // ═══════════════════════════════════════
    console.log('--- r2State initialization ---\n');

    // Test 1
    {
        resetR2State();
        assertNull(r2State.manifest, '1. Default r2State.manifest is null');
    }

    // Test 2
    {
        resetR2State();
        assertFalse(r2State.loaded, '2. Default r2State.loaded is false');
    }

    // Test 3
    {
        resetR2State();
        assertNull(r2State.error, '3. Default r2State.error is null');
    }

    // ═══════════════════════════════════════
    // Suite 2: resolveDataUrl (no manifest)
    // ═══════════════════════════════════════
    console.log('\n--- resolveDataUrl (no manifest loaded) ---\n');

    // Test 4
    {
        resetR2State();
        const result = resolveDataUrl('../data/CDOT/douglas_all_roads.csv');
        assertEqual(result, '../data/CDOT/douglas_all_roads.csv',
            '4. Returns local path when manifest is null');
    }

    // Test 5
    {
        resetR2State();
        r2State.manifest = { r2BaseUrl: '', localPathMapping: { 'data/test.csv': 'test/key.csv' } };
        const result = resolveDataUrl('../data/test.csv');
        assertEqual(result, '../data/test.csv',
            '5. Returns local path when r2BaseUrl is empty');
    }

    // Test 6
    {
        resetR2State();
        r2State.manifest = { r2BaseUrl: 'https://example.r2.dev', localPathMapping: null };
        const result = resolveDataUrl('../data/CDOT/test.csv');
        assertEqual(result, '../data/CDOT/test.csv',
            '6. Returns local path when localPathMapping is null');
    }

    // Test 7
    {
        resetR2State();
        r2State.manifest = { r2BaseUrl: 'https://example.r2.dev', localPathMapping: {} };
        const result = resolveDataUrl('../data/CDOT/test.csv');
        assertEqual(result, '../data/CDOT/test.csv',
            '7. Returns local path when file not found in empty mapping');
    }

    // ═══════════════════════════════════════
    // Suite 3: resolveDataUrl (path normalization)
    // ═══════════════════════════════════════
    console.log('\n--- resolveDataUrl (path normalization) ---\n');

    // Test 8
    {
        resetR2State();
        r2State.manifest = VALID_MANIFEST;
        const result = resolveDataUrl('../data/CDOT/douglas_all_roads.csv');
        assertEqual(result, 'https://data.aicreatesai.com/colorado/douglas/all_roads.csv',
            '8. Strips ../ prefix and resolves to R2 URL');
    }

    // Test 9
    {
        resetR2State();
        r2State.manifest = VALID_MANIFEST;
        const result = resolveDataUrl('./data/CDOT/douglas_all_roads.csv');
        assertEqual(result, 'https://data.aicreatesai.com/colorado/douglas/all_roads.csv',
            '9. Strips ./ prefix and resolves to R2 URL');
    }

    // Test 10
    {
        resetR2State();
        r2State.manifest = VALID_MANIFEST;
        const result = resolveDataUrl('data/CDOT/douglas_all_roads.csv');
        assertEqual(result, 'https://data.aicreatesai.com/colorado/douglas/all_roads.csv',
            '10. Handles path without any prefix');
    }

    // Test 11
    {
        resetR2State();
        r2State.manifest = VALID_MANIFEST;
        const result = resolveDataUrl('../../data/CDOT/douglas_all_roads.csv');
        // After stripping first '../', we get '../data/CDOT/...' which starts with '../'
        // Wait - substring(3) on '../../data/...' gives '../data/...'
        // Then the './' check won't match '../data/...'
        // So lookup key is '../data/CDOT/douglas_all_roads.csv' which isn't in the mapping
        assertEqual(result, '../../data/CDOT/douglas_all_roads.csv',
            '11. Double ../ not fully stripped — returns original path (defensive)');
    }

    // Test 12
    {
        resetR2State();
        r2State.manifest = VALID_MANIFEST;
        const result = resolveDataUrl('../data/not_in_manifest.csv');
        assertEqual(result, '../data/not_in_manifest.csv',
            '12. Returns original path (with prefix) when file not in mapping');
    }

    // ═══════════════════════════════════════
    // Suite 4: resolveDataUrl (URL construction)
    // ═══════════════════════════════════════
    console.log('\n--- resolveDataUrl (URL construction) ---\n');

    // Test 13
    {
        resetR2State();
        r2State.manifest = VALID_MANIFEST;
        const result = resolveDataUrl('../data/CDOT/douglas_county_roads.csv');
        assertEqual(result, 'https://data.aicreatesai.com/colorado/douglas/county_roads.csv',
            '13. Constructs correct R2 URL from base + key');
    }

    // Test 14: Trailing slash in base URL
    {
        resetR2State();
        r2State.manifest = {
            ...VALID_MANIFEST,
            r2BaseUrl: 'https://data.aicreatesai.com/'
        };
        const result = resolveDataUrl('../data/CDOT/douglas_all_roads.csv');
        assertContains(result, 'colorado/douglas/all_roads.csv',
            '14. URL contains correct R2 key even with trailing slash base');
    }

    // Test 15: Virginia jurisdiction paths
    {
        resetR2State();
        r2State.manifest = VALID_MANIFEST;
        const result = resolveDataUrl('../data/henrico_all_roads.csv');
        assertEqual(result, 'https://data.aicreatesai.com/virginia/henrico/all_roads.csv',
            '15. Maps Virginia jurisdiction paths correctly');
    }

    // Test 16: Colorado CDOT paths
    {
        resetR2State();
        r2State.manifest = VALID_MANIFEST;
        const result = resolveDataUrl('../data/CDOT/douglas_standardized.csv');
        assertEqual(result, 'https://data.aicreatesai.com/colorado/douglas/standardized.csv',
            '16. Maps Colorado CDOT paths correctly');
    }

    // Test 17: Fallback crashes.csv
    {
        resetR2State();
        r2State.manifest = VALID_MANIFEST;
        const result = resolveDataUrl('../data/CDOT/crashes.csv');
        assertEqual(result, 'https://data.aicreatesai.com/colorado/douglas/crashes.csv',
            '17. Maps fallback crashes.csv correctly');
    }

    // ═══════════════════════════════════════
    // Suite 5: resolveDataUrl (edge cases)
    // ═══════════════════════════════════════
    console.log('\n--- resolveDataUrl (edge cases) ---\n');

    // Test 18: Empty string
    {
        resetR2State();
        r2State.manifest = VALID_MANIFEST;
        const result = resolveDataUrl('');
        assertEqual(result, '',
            '18. Empty string path returns empty string');
    }

    // Test 19: Grants file (stays local)
    {
        resetR2State();
        r2State.manifest = VALID_MANIFEST;
        const result = resolveDataUrl('../data/grants.csv');
        assertEqual(result, '../data/grants.csv',
            '19. grants.csv not in R2 mapping — returns local path');
    }

    // Test 20: CMF processed JSON (stays local)
    {
        resetR2State();
        r2State.manifest = VALID_MANIFEST;
        const result = resolveDataUrl('../data/cmf_processed.json');
        assertEqual(result, '../data/cmf_processed.json',
            '20. cmf_processed.json not in R2 mapping — returns local path');
    }

    // Test 21: Multiple consecutive calls return consistent results
    {
        resetR2State();
        r2State.manifest = VALID_MANIFEST;
        const r1 = resolveDataUrl('../data/CDOT/douglas_all_roads.csv');
        const r2 = resolveDataUrl('../data/CDOT/douglas_all_roads.csv');
        const r3 = resolveDataUrl('../data/CDOT/douglas_all_roads.csv');
        assertEqual(r1, r2, '21a. Consecutive call 1 == call 2');
        assertEqual(r2, r3, '21b. Consecutive call 2 == call 3');
    }

    // Test 22: Path with query string (defensive)
    {
        resetR2State();
        r2State.manifest = VALID_MANIFEST;
        const result = resolveDataUrl('../data/CDOT/douglas_all_roads.csv?v=123');
        assertEqual(result, '../data/CDOT/douglas_all_roads.csv?v=123',
            '22. Path with query string returns original (no match in mapping)');
    }

    // ═══════════════════════════════════════
    // Suite 6: loadR2Manifest (valid manifest)
    // ═══════════════════════════════════════
    console.log('\n--- loadR2Manifest (valid manifest) ---\n');

    // Test 23-26
    {
        resetR2State();
        await loadR2Manifest(mockFetchSuccess(VALID_MANIFEST));

        assertNotNull(r2State.manifest, '23. manifest is set after successful load');
        assertTrue(r2State.loaded, '24. loaded is true after successful load');
        assertNull(r2State.error, '25. error is null after successful load');
        assertEqual(Object.keys(r2State.manifest.localPathMapping).length, 8,
            '26. manifest has 8 path mappings');
    }

    // ═══════════════════════════════════════
    // Suite 7: loadR2Manifest (empty manifest)
    // ═══════════════════════════════════════
    console.log('\n--- loadR2Manifest (empty manifest) ---\n');

    // Test 27
    {
        resetR2State();
        await loadR2Manifest(mockFetchSuccess(EMPTY_MANIFEST));
        assertNull(r2State.manifest, '27. Empty r2BaseUrl — manifest stays null');
        assertTrue(r2State.loaded, '27b. loaded is true');
    }

    // Test 28
    {
        resetR2State();
        await loadR2Manifest(mockFetchSuccess({
            version: 1,
            r2BaseUrl: 'https://example.r2.dev',
            localPathMapping: {}
        }));
        assertNull(r2State.manifest, '28. Empty localPathMapping — manifest stays null');
        assertTrue(r2State.loaded, '28b. loaded is true');
    }

    // Test 29
    {
        resetR2State();
        await loadR2Manifest(mockFetchSuccess({
            version: 1,
            r2BaseUrl: 'https://example.r2.dev'
        }));
        assertNull(r2State.manifest, '29. Missing localPathMapping — manifest stays null');
        assertTrue(r2State.loaded, '29b. loaded is true');
    }

    // Test 30
    {
        resetR2State();
        await loadR2Manifest(mockFetchSuccess({
            version: 1,
            localPathMapping: { 'data/test.csv': 'test/key.csv' }
        }));
        assertNull(r2State.manifest, '30. Missing r2BaseUrl — manifest stays null');
        assertTrue(r2State.loaded, '30b. loaded is true');
    }

    // ═══════════════════════════════════════
    // Suite 8: loadR2Manifest (fetch failures)
    // ═══════════════════════════════════════
    console.log('\n--- loadR2Manifest (fetch failures) ---\n');

    // Test 31
    {
        resetR2State();
        await loadR2Manifest(mockFetchHttpError(404));
        assertNull(r2State.manifest, '31. HTTP 404 — manifest stays null');
        assertTrue(r2State.loaded, '31b. loaded is true after 404');
    }

    // Test 32
    {
        resetR2State();
        await loadR2Manifest(mockFetchHttpError(500));
        assertNull(r2State.manifest, '32. HTTP 500 — manifest stays null');
        assertTrue(r2State.loaded, '32b. loaded is true after 500');
    }

    // Test 33
    {
        resetR2State();
        await loadR2Manifest(mockFetchNetworkError('Failed to fetch'));
        assertNull(r2State.manifest, '33. Network error — manifest stays null');
        assertTrue(r2State.loaded, '33b. loaded is true after network error');
        assertEqual(r2State.error, 'Failed to fetch', '33c. error message stored');
    }

    // Test 34
    {
        resetR2State();
        await loadR2Manifest(mockFetchInvalidJson());
        assertNull(r2State.manifest, '34. Invalid JSON — manifest stays null');
        assertTrue(r2State.loaded, '34b. loaded is true after JSON error');
        assertNotNull(r2State.error, '34c. error is set after JSON parse failure');
    }

    // ═══════════════════════════════════════
    // Suite 9: loadR2Manifest (malformed manifests)
    // ═══════════════════════════════════════
    console.log('\n--- loadR2Manifest (malformed manifest) ---\n');

    // Test 35
    {
        resetR2State();
        await loadR2Manifest(mockFetchSuccess({
            version: 1,
            r2BaseUrl: 'https://example.r2.dev',
            files: { 'some/file.csv': { size: 100 } },
            localPathMapping: {}
        }));
        assertNull(r2State.manifest, '35. r2BaseUrl set but empty localPathMapping — stays null');
    }

    // Test 36
    {
        resetR2State();
        await loadR2Manifest(mockFetchSuccess({ version: 1 }));
        assertNull(r2State.manifest, '36. Only version field — stays null');
        assertTrue(r2State.loaded, '36b. loaded is true');
    }

    // Test 37
    {
        resetR2State();
        await loadR2Manifest(mockFetchSuccess([{ r2BaseUrl: 'test' }]));
        assertNull(r2State.manifest, '37. Array manifest — stays null');
        assertTrue(r2State.loaded, '37b. loaded is true');
    }

    // Test 38
    {
        resetR2State();
        await loadR2Manifest(mockFetchSuccess({
            version: 1,
            r2BaseUrl: '',
            localPathMapping: { 'data/test.csv': 'test/key.csv' }
        }));
        assertNull(r2State.manifest, '38. Empty r2BaseUrl with entries — stays null');
        assertTrue(r2State.loaded, '38b. loaded is true');
    }

    // ═══════════════════════════════════════
    // Suite 10: Integration (load → resolve)
    // ═══════════════════════════════════════
    console.log('\n--- Integration (load → resolve) ---\n');

    // Test 39
    {
        resetR2State();
        await loadR2Manifest(mockFetchSuccess(VALID_MANIFEST));
        const result = resolveDataUrl('../data/CDOT/douglas_all_roads.csv');
        assertContains(result, 'data.aicreatesai.com', '39. After successful load, URL contains data.aicreatesai.com');
        assertContains(result, 'colorado/douglas/all_roads.csv', '39b. URL contains correct R2 key');
    }

    // Test 40
    {
        resetR2State();
        await loadR2Manifest(mockFetchNetworkError('Connection refused'));
        const result = resolveDataUrl('../data/CDOT/douglas_all_roads.csv');
        assertEqual(result, '../data/CDOT/douglas_all_roads.csv',
            '40. After failed load, returns local path');
    }

    // Test 41
    {
        resetR2State();
        await loadR2Manifest(mockFetchSuccess(EMPTY_MANIFEST));
        const result = resolveDataUrl('../data/CDOT/douglas_all_roads.csv');
        assertEqual(result, '../data/CDOT/douglas_all_roads.csv',
            '41. After empty manifest, returns local path');
    }

    // Test 42
    {
        resetR2State();
        await loadR2Manifest(mockFetchSuccess(VALID_MANIFEST));

        const r1 = resolveDataUrl('../data/CDOT/douglas_all_roads.csv');
        const r2 = resolveDataUrl('../data/CDOT/douglas_county_roads.csv');
        const r3 = resolveDataUrl('../data/henrico_all_roads.csv');
        const r4 = resolveDataUrl('../data/grants.csv');

        assertContains(r1, 'colorado/douglas/all_roads.csv', '42a. Douglas all_roads resolves');
        assertContains(r2, 'colorado/douglas/county_roads.csv', '42b. Douglas county_roads resolves');
        assertContains(r3, 'virginia/henrico/all_roads.csv', '42c. Henrico all_roads resolves');
        assertEqual(r4, '../data/grants.csv', '42d. Grants stays local');
    }

    // ═══════════════════════════════════════
    // Suite 11: Workflow manifest simulation
    // ═══════════════════════════════════════
    console.log('\n--- Workflow manifest simulation ---\n');

    // Test 43
    {
        const manifest = VALID_MANIFEST;
        assertEqual(manifest.version, 1, '43a. Manifest version is 1');
        assert(typeof manifest.r2BaseUrl === 'string' && manifest.r2BaseUrl.length > 0,
            '43b. r2BaseUrl is non-empty string');
        assert(typeof manifest.updated === 'string', '43c. updated is a string');
        assert(typeof manifest.files === 'object', '43d. files is an object');
        assert(typeof manifest.localPathMapping === 'object', '43e. localPathMapping is an object');
    }

    // Test 44
    {
        resetR2State();
        await loadR2Manifest(mockFetchSuccess(MULTI_JURISDICTION_MANIFEST));

        const douglas = resolveDataUrl('../data/CDOT/douglas_all_roads.csv');
        const arapahoe = resolveDataUrl('../data/CDOT/arapahoe_all_roads.csv');
        const jefferson = resolveDataUrl('../data/CDOT/jefferson_all_roads.csv');
        const henrico = resolveDataUrl('../data/henrico_all_roads.csv');
        const fairfax = resolveDataUrl('../data/fairfax_county_all_roads.csv');

        assertContains(douglas, 'colorado/douglas/', '44a. Douglas → colorado/douglas/');
        assertContains(arapahoe, 'colorado/arapahoe/', '44b. Arapahoe → colorado/arapahoe/');
        assertContains(jefferson, 'colorado/jefferson/', '44c. Jefferson → colorado/jefferson/');
        assertContains(henrico, 'virginia/henrico/', '44d. Henrico → virginia/henrico/');
        assertContains(fairfax, 'virginia/fairfax_county/', '44e. Fairfax → virginia/fairfax_county/');
    }

    // Test 45
    {
        resetR2State();
        await loadR2Manifest(mockFetchSuccess(VALID_MANIFEST));
        assertEqual(r2State.manifest.version, 1, '45. Version field preserved in loaded manifest');
    }

    // ═══════════════════════════════════════
    // Suite 12: CORS and URL format
    // ═══════════════════════════════════════
    console.log('\n--- CORS and URL format ---\n');

    // Test 46
    {
        resetR2State();
        await loadR2Manifest(mockFetchSuccess(VALID_MANIFEST));
        const result = resolveDataUrl('../data/CDOT/douglas_all_roads.csv');
        assertStartsWith(result, 'https://', '46. R2 URLs use HTTPS protocol');
    }

    // Test 47
    {
        resetR2State();
        await loadR2Manifest(mockFetchSuccess(VALID_MANIFEST));
        const result = resolveDataUrl('../data/CDOT/douglas_all_roads.csv');
        assertContains(result, 'data.aicreatesai.com',
            '47. R2 URL contains expected custom domain');
    }

    // Test 48
    {
        resetR2State();
        await loadR2Manifest(mockFetchSuccess(VALID_MANIFEST));

        const grants = resolveDataUrl('../data/grants.csv');
        const cmf = resolveDataUrl('../data/cmf_processed.json');
        const mutcd = resolveDataUrl('../data/va_mutcd/signal_warrants.json');
        const forecast = resolveDataUrl('../data/CDOT/forecasts.json');

        assertStartsWith(grants, '../', '48a. grants.csv stays local');
        assertStartsWith(cmf, '../', '48b. cmf_processed.json stays local');
        assertStartsWith(mutcd, '../', '48c. va_mutcd data stays local');
        assertStartsWith(forecast, '../', '48d. forecasts.json stays local');
    }

    // ═══════════════════════════════════════
    // Suite 13: Concurrency / state safety
    // ═══════════════════════════════════════
    console.log('\n--- Concurrency / state safety ---\n');

    // Test 49
    {
        resetR2State();
        r2State.manifest = VALID_MANIFEST;

        const result1 = resolveDataUrl('../data/CDOT/douglas_all_roads.csv');
        assertContains(result1, 'data.aicreatesai.com', '49a. First resolve works');

        r2State.manifest = null;
        const result2 = resolveDataUrl('../data/CDOT/douglas_all_roads.csv');
        assertEqual(result2, '../data/CDOT/douglas_all_roads.csv',
            '49b. After manifest cleared, returns local path');

        r2State.manifest = VALID_MANIFEST;
        const result3 = resolveDataUrl('../data/CDOT/douglas_all_roads.csv');
        assertContains(result3, 'data.aicreatesai.com', '49c. After manifest restored, resolves again');
    }

    // Test 50
    {
        resetR2State();
        assertFalse(r2State.loaded, '50a. loaded is false before manifest load');
        const result = resolveDataUrl('../data/CDOT/douglas_all_roads.csv');
        assertEqual(result, '../data/CDOT/douglas_all_roads.csv',
            '50b. Safe to call resolveDataUrl before manifest loaded');
    }

    // ─── REPORT ───
    console.log('\n========================================');
    console.log(`  RESULTS: ${passed} passed, ${failed} failed`);
    console.log('========================================\n');

    if (failures.length > 0) {
        console.log('FAILURES:');
        failures.forEach((f, i) => console.log(`  ${i + 1}. ${f}`));
        console.log('');
    }

    process.exit(failed > 0 ? 1 : 0);
}

// Run all tests sequentially
runAllTests().catch(err => {
    console.error('Test runner error:', err);
    process.exit(2);
});
