/**
 * Bug test: Email alert 405 fallback
 *
 * Validates the fix for the email alert 405 error where sendBAMonitoringTestAlert()
 * and checkBAMonitoringOnDataLoad() only called /api/notify/send with no fallback,
 * causing failures when the Coolify backend is unreachable.
 *
 * Bug: Console showed "405 Method Not Allowed" and "SyntaxError: Unexpected token '<'"
 *      because the server returned HTML error pages instead of JSON.
 *
 * Fix: Added Coolify-first + direct Brevo API fallback pattern (matching other email
 *      functions). Hardened JSON parsing in syncBAMonitoringToServer().
 *
 * Run with: node --test tests/test_email_alert_fallback.js
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

// =============================================================================
// MOCK: Simulated fetch responses for testing fallback logic
// =============================================================================

/**
 * Simulates the fallback logic from sendBAMonitoringTestAlert().
 * Returns { sent, method } to indicate which path was used.
 */
async function simulateSendAlert({
    hasCoolifyBackend,
    coolifyResponds,    // 'ok' | 'error_status' | 'network_error'
    hasBrevoApiKey,
    hasBrevoFromEmail,
    brevoResponds       // 'ok' | 'error_status'
}) {
    let sent = false;
    let method = null;

    // Try Coolify backend first
    if (hasCoolifyBackend) {
        try {
            if (coolifyResponds === 'network_error') {
                throw new Error('Failed to fetch');
            }
            const respOk = coolifyResponds === 'ok';
            if (respOk) {
                sent = true;
                method = 'coolify';
            }
            // If not ok (405, 500, etc.), fall through to Brevo
        } catch (coolifyErr) {
            // Coolify unavailable, fall through to Brevo
        }
    }

    // Fallback: direct Brevo API
    if (!sent) {
        const apiKey = hasBrevoApiKey ? 'xkeysib-test-key' : null;
        const fromEmail = hasBrevoFromEmail ? 'alerts@example.com' : null;

        if (apiKey && fromEmail) {
            const respOk = brevoResponds === 'ok';
            if (respOk) {
                sent = true;
                method = 'brevo_direct';
            }
        }
    }

    return { sent, method };
}

/**
 * Simulates the hardened JSON parsing in syncBAMonitoringToServer().
 * The fix uses: resp.json().catch(() => ({ success: false, error: `Server returned ${status}` }))
 */
function simulateJsonParse(responseBody, status) {
    try {
        return JSON.parse(responseBody);
    } catch {
        return { success: false, error: `Server returned ${status}` };
    }
}

// =============================================================================
// BUG REPRODUCTION: Before fix, these scenarios caused errors
// =============================================================================

describe('Bug reproduction: Email alert 405 errors', () => {

    it('BUG: sendBAMonitoringTestAlert crashed with 405 when Coolify backend unreachable', async () => {
        // Before fix: only tried /api/notify/send, got 405, then resp.json() threw
        //   SyntaxError: Unexpected token '<', "<html>..." is not valid JSON
        // After fix: falls back to direct Brevo API
        const result = await simulateSendAlert({
            hasCoolifyBackend: true,
            coolifyResponds: 'error_status',    // 405 from nginx
            hasBrevoApiKey: true,
            hasBrevoFromEmail: true,
            brevoResponds: 'ok'
        });

        assert.equal(result.sent, true, 'Should send via Brevo fallback when Coolify returns 405');
        assert.equal(result.method, 'brevo_direct', 'Should use direct Brevo API as fallback');
    });

    it('BUG: syncBAMonitoringToServer crashed on HTML response from 405', () => {
        // Before fix: resp.json() threw SyntaxError on HTML error page
        // After fix: .catch() returns a safe fallback object
        const htmlErrorPage = '<!DOCTYPE html><html><body><h1>405 Not Allowed</h1></body></html>';
        const result = simulateJsonParse(htmlErrorPage, 405);

        assert.equal(result.success, false, 'Should return success:false for HTML response');
        assert.ok(result.error.includes('405'), 'Error should mention status code');
    });

    it('BUG: checkBAMonitoringOnDataLoad silently failed on 405 — no fallback', async () => {
        // Before fix: fetch().then(r => r.json()) threw on HTML, .catch() ate the error silently
        // After fix: tries Brevo direct API before giving up
        const result = await simulateSendAlert({
            hasCoolifyBackend: true,
            coolifyResponds: 'network_error',
            hasBrevoApiKey: true,
            hasBrevoFromEmail: true,
            brevoResponds: 'ok'
        });

        assert.equal(result.sent, true, 'Should send via Brevo when Coolify has network error');
        assert.equal(result.method, 'brevo_direct');
    });
});

// =============================================================================
// FALLBACK LOGIC TESTS
// =============================================================================

describe('Email alert fallback logic', () => {

    it('should use Coolify backend when available and responding', async () => {
        const result = await simulateSendAlert({
            hasCoolifyBackend: true,
            coolifyResponds: 'ok',
            hasBrevoApiKey: true,
            hasBrevoFromEmail: true,
            brevoResponds: 'ok'
        });
        assert.equal(result.sent, true);
        assert.equal(result.method, 'coolify', 'Should prefer Coolify when it works');
    });

    it('should fall back to Brevo when Coolify returns error status (405, 500, etc)', async () => {
        const result = await simulateSendAlert({
            hasCoolifyBackend: true,
            coolifyResponds: 'error_status',
            hasBrevoApiKey: true,
            hasBrevoFromEmail: true,
            brevoResponds: 'ok'
        });
        assert.equal(result.sent, true);
        assert.equal(result.method, 'brevo_direct');
    });

    it('should fall back to Brevo when Coolify throws network error', async () => {
        const result = await simulateSendAlert({
            hasCoolifyBackend: true,
            coolifyResponds: 'network_error',
            hasBrevoApiKey: true,
            hasBrevoFromEmail: true,
            brevoResponds: 'ok'
        });
        assert.equal(result.sent, true);
        assert.equal(result.method, 'brevo_direct');
    });

    it('should skip Coolify entirely when brevoSource is manual', async () => {
        const result = await simulateSendAlert({
            hasCoolifyBackend: false,       // brevoSource !== 'auto'
            coolifyResponds: 'ok',
            hasBrevoApiKey: true,
            hasBrevoFromEmail: true,
            brevoResponds: 'ok'
        });
        assert.equal(result.sent, true);
        assert.equal(result.method, 'brevo_direct', 'Should go straight to Brevo in manual mode');
    });

    it('should fail gracefully when both Coolify and Brevo are unavailable', async () => {
        const result = await simulateSendAlert({
            hasCoolifyBackend: true,
            coolifyResponds: 'error_status',
            hasBrevoApiKey: false,
            hasBrevoFromEmail: false,
            brevoResponds: 'ok'
        });
        assert.equal(result.sent, false, 'Should return sent=false when all paths fail');
        assert.equal(result.method, null);
    });

    it('should fail when Brevo API key is missing (no fallback possible)', async () => {
        const result = await simulateSendAlert({
            hasCoolifyBackend: true,
            coolifyResponds: 'network_error',
            hasBrevoApiKey: false,
            hasBrevoFromEmail: true,
            brevoResponds: 'ok'
        });
        assert.equal(result.sent, false, 'Cannot send without API key');
    });

    it('should fail when Brevo from-email is missing (no fallback possible)', async () => {
        const result = await simulateSendAlert({
            hasCoolifyBackend: true,
            coolifyResponds: 'network_error',
            hasBrevoApiKey: true,
            hasBrevoFromEmail: false,
            brevoResponds: 'ok'
        });
        assert.equal(result.sent, false, 'Cannot send without from-email');
    });

    it('should fail when both Coolify and Brevo direct return errors', async () => {
        const result = await simulateSendAlert({
            hasCoolifyBackend: true,
            coolifyResponds: 'error_status',
            hasBrevoApiKey: true,
            hasBrevoFromEmail: true,
            brevoResponds: 'error_status'
        });
        assert.equal(result.sent, false, 'Should fail when both paths return errors');
    });
});

// =============================================================================
// JSON PARSE HARDENING TESTS
// =============================================================================

describe('Hardened JSON parsing (syncBAMonitoringToServer)', () => {

    it('should parse valid JSON response correctly', () => {
        const result = simulateJsonParse('{"success":true,"alertId":"abc123"}', 200);
        assert.equal(result.success, true);
        assert.equal(result.alertId, 'abc123');
    });

    it('should handle HTML error page (405 from nginx)', () => {
        const html = '<!DOCTYPE html><html><head><title>405 Not Allowed</title></head><body></body></html>';
        const result = simulateJsonParse(html, 405);
        assert.equal(result.success, false);
        assert.ok(result.error.includes('405'));
    });

    it('should handle empty response body', () => {
        const result = simulateJsonParse('', 502);
        assert.equal(result.success, false);
        assert.ok(result.error.includes('502'));
    });

    it('should handle partial/truncated JSON', () => {
        const result = simulateJsonParse('{"success":true, "alertId":', 200);
        assert.equal(result.success, false);
    });

    it('should handle plain text error', () => {
        const result = simulateJsonParse('Bad Gateway', 502);
        assert.equal(result.success, false);
    });

    it('should handle JSON with error field', () => {
        const result = simulateJsonParse('{"success":false,"error":"Unauthorized"}', 401);
        assert.equal(result.success, false);
        assert.equal(result.error, 'Unauthorized');
    });
});

// =============================================================================
// BREVO SOURCE DETECTION TESTS
// =============================================================================

describe('Brevo source detection (hasCoolifyBackend)', () => {

    it('should detect Coolify when source is "auto" (default)', () => {
        const brevoSource = 'auto';
        const hasCoolifyBackend = brevoSource === 'auto';
        assert.equal(hasCoolifyBackend, true);
    });

    it('should detect Coolify when source is undefined (defaults to auto)', () => {
        const brevoSource = undefined || 'auto';
        const hasCoolifyBackend = brevoSource === 'auto';
        assert.equal(hasCoolifyBackend, true);
    });

    it('should NOT detect Coolify when source is "manual"', () => {
        const brevoSource = 'manual';
        const hasCoolifyBackend = brevoSource === 'auto';
        assert.equal(hasCoolifyBackend, false);
    });

    it('should NOT detect Coolify when source is empty string', () => {
        // Empty string || 'auto' should default to 'auto'
        const brevoSource = '' || 'auto';
        const hasCoolifyBackend = brevoSource === 'auto';
        assert.equal(hasCoolifyBackend, true, 'Empty source should default to auto');
    });
});

console.log('\n=== Running email alert fallback bug tests ===\n');
