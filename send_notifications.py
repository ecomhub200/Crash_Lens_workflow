#!/usr/bin/env python3
"""
CRASH LENS Email Notification System
Sends scheduled reports and grant alerts via Brevo.

Brevo supports two modes:
  - API mode (recommended): Set BREVO_API_KEY env var
  - SMTP mode (fallback):   Set BREVO_SMTP_LOGIN + BREVO_SMTP_PASSWORD env vars

Usage:
    python send_notifications.py --type reports     # Send scheduled reports
    python send_notifications.py --type grants      # Send grant alerts
    python send_notifications.py --type digest      # Send weekly digest
    python send_notifications.py --type test --email user@example.com  # Test email
"""

import os
import sys
import json
import argparse
import smtplib
import uuid
import urllib.request
import urllib.error
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from pathlib import Path

# =============================================================================
# CONFIGURATION
# =============================================================================

FROM_EMAIL = os.environ.get('NOTIFICATION_FROM_EMAIL')
CHARSET = 'UTF-8'

# Brevo configuration
BREVO_API_KEY = os.environ.get('BREVO_API_KEY')
BREVO_SMTP_SERVER = 'smtp-relay.brevo.com'
BREVO_SMTP_PORT = 587
BREVO_SMTP_LOGIN = os.environ.get('BREVO_SMTP_LOGIN')
BREVO_SMTP_PASSWORD = os.environ.get('BREVO_SMTP_PASSWORD')

def validate_config():
    """Validate required Brevo configuration before sending emails."""
    if not FROM_EMAIL:
        print("[ERROR] NOTIFICATION_FROM_EMAIL environment variable not set")
        print("  Set this to your verified Brevo sender address")
        print("  Example: notifications@aicreatesai.com")
        sys.exit(1)

    # Prefer SMTP mode if credentials are available (most reliable)
    if BREVO_SMTP_LOGIN and BREVO_SMTP_PASSWORD:
        print(f"[CONFIG] Provider: Brevo (SMTP mode)")
        print(f"[CONFIG] From: {FROM_EMAIL}")
        print(f"[CONFIG] SMTP Login: {BREVO_SMTP_LOGIN}")
        return

    if BREVO_API_KEY:
        key = BREVO_API_KEY.strip()
        if not key.startswith('xkeysib-'):
            print("[WARN] BREVO_API_KEY does not start with 'xkeysib-'")
            print(f"  Your key starts with: {key[:8]}...")
            print("  This looks like an SMTP password, NOT an API key.")
            print("  If you have SMTP credentials, use BREVO_SMTP_LOGIN + BREVO_SMTP_PASSWORD instead.")
            print("  For the v3 API key: Brevo > profile icon (top-right) > SMTP & API > API Keys > Generate")
        print(f"[CONFIG] Provider: Brevo (API mode)")
        print(f"[CONFIG] From: {FROM_EMAIL}")
        print(f"[CONFIG] API Key: {key[:8]}...{key[-4:]}")
        return

    print("[ERROR] No Brevo credentials configured. Set one of:")
    print("")
    print("  Option 1 (easiest): BREVO_SMTP_LOGIN + BREVO_SMTP_PASSWORD")
    print("    - Get from: Brevo Dashboard > SMTP & API > SMTP tab")
    print("    - BREVO_SMTP_LOGIN = your login email (e.g. 8xxxx@smtp-brevo.com)")
    print("    - BREVO_SMTP_PASSWORD = the SMTP key shown on that page")
    print("")
    print("  Option 2 (API mode): BREVO_API_KEY")
    print("    - Get from: Brevo > profile icon (top-right) > SMTP & API > API Keys")
    print("    - Key starts with: xkeysib-")
    sys.exit(1)

# Paths
BASE_DIR = Path(__file__).parent
SUBSCRIBERS_FILE = BASE_DIR / 'data' / 'subscribers.json'
GRANTS_FILE = BASE_DIR / 'data' / 'grants.csv'
CRASHES_FILE = BASE_DIR / 'data' / 'CDOT' / 'crashes.csv'

# =============================================================================
# BREVO EMAIL SENDING
# =============================================================================

def send_via_brevo_api(to_email, subject, html_body, text_body):
    """Send email via Brevo HTTP API."""
    api_key = BREVO_API_KEY.strip()

    payload = json.dumps({
        "sender": {"email": FROM_EMAIL, "name": "CRASH LENS"},
        "to": [{"email": to_email}],
        "subject": subject,
        "htmlContent": html_body,
        "textContent": text_body,
        "tags": ["crash-lens", "transactional"],
        "headers": {
            "X-Entity-Ref-ID": str(uuid.uuid4())
        }
    })

    req = urllib.request.Request(
        'https://api.brevo.com/v3/smtp/email',
        data=payload.encode('utf-8'),
        headers={
            'accept': 'application/json',
            'content-type': 'application/json',
            'api-key': api_key
        },
        method='POST'
    )

    try:
        with urllib.request.urlopen(req) as response:
            result = json.loads(response.read().decode())
            msg_id = result.get('messageId', 'unknown')
            print(f"[SUCCESS] Email sent to {to_email} (MessageId: {msg_id})")
            return True
    except urllib.error.HTTPError as e:
        error_body = e.read().decode()
        print(f"[ERROR] Brevo API error for {to_email}: {e.code} - {error_body}")
        if e.code == 401:
            print("[HINT] 401 = Invalid API key. Check these:")
            print("  1. Copy the FULL key from Brevo Dashboard > SMTP & API > API Keys")
            print("  2. Key must start with 'xkeysib-'")
            print("  3. In GitHub Secrets: no extra spaces, quotes, or line breaks")
            print(f"  4. Your key starts with: {api_key[:12]}...")
        elif e.code == 400:
            print("[HINT] 400 = Bad request. Check that sender email is verified in Brevo")
        return False
    except Exception as e:
        print(f"[ERROR] Failed to send via Brevo API to {to_email}: {e}")
        return False

def send_via_brevo_smtp(to_email, subject, html_body, text_body):
    """Send email via Brevo SMTP relay."""
    msg = MIMEMultipart('alternative')
    msg['From'] = f"CRASH LENS <{FROM_EMAIL}>"
    msg['To'] = to_email
    msg['Subject'] = subject

    msg.attach(MIMEText(text_body, 'plain', CHARSET))
    msg.attach(MIMEText(html_body, 'html', CHARSET))

    try:
        with smtplib.SMTP(BREVO_SMTP_SERVER, BREVO_SMTP_PORT) as server:
            server.starttls()
            server.login(BREVO_SMTP_LOGIN, BREVO_SMTP_PASSWORD)
            server.send_message(msg)
        print(f"[SUCCESS] Email sent to {to_email} via Brevo SMTP")
        return True
    except smtplib.SMTPAuthenticationError as e:
        print(f"[ERROR] Brevo SMTP auth failed: {e}")
        print("[HINT] Check BREVO_SMTP_LOGIN (your Brevo account email) and BREVO_SMTP_PASSWORD (SMTP key from dashboard)")
        return False
    except smtplib.SMTPException as e:
        print(f"[ERROR] Brevo SMTP error for {to_email}: {e}")
        return False
    except Exception as e:
        print(f"[ERROR] Failed to send via Brevo SMTP to {to_email}: {e}")
        return False

def send_email(to_email, subject, html_body, text_body):
    """Send email via Brevo. Prefers SMTP mode (most reliable), falls back to API."""
    if BREVO_SMTP_LOGIN and BREVO_SMTP_PASSWORD:
        return send_via_brevo_smtp(to_email, subject, html_body, text_body)
    elif BREVO_API_KEY:
        return send_via_brevo_api(to_email, subject, html_body, text_body)
    else:
        print("[ERROR] No Brevo credentials available")
        return False

# =============================================================================
# SUBSCRIBER MANAGEMENT
# =============================================================================

def load_subscribers():
    """Load subscribers from JSON file."""
    if not SUBSCRIBERS_FILE.exists():
        return {'subscribers': []}

    try:
        with open(SUBSCRIBERS_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"[ERROR] Failed to load subscribers: {e}")
        return {'subscribers': []}

def get_report_subscribers():
    """Get subscribers with report notifications enabled."""
    data = load_subscribers()
    return [
        s for s in data.get('subscribers', [])
        if s.get('reports', {}).get('enabled', False)
        and s.get('verified', False)
        and s.get('email')
    ]

def get_grant_subscribers():
    """Get subscribers with grant notifications enabled."""
    data = load_subscribers()
    return [
        s for s in data.get('subscribers', [])
        if s.get('grants', {}).get('enabled', False)
        and s.get('verified', False)
        and s.get('email')
    ]

# =============================================================================
# DATA LOADING
# =============================================================================

def load_crash_summary():
    """Load crash data summary for reports."""
    if not CRASHES_FILE.exists():
        return None

    try:
        import csv
        with open(CRASHES_FILE, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        total = len(rows)
        severity_counts = {'K': 0, 'A': 0, 'B': 0, 'C': 0, 'O': 0}

        for row in rows:
            sev = row.get('CRASH_SEVERITY', row.get('Severity', 'O'))
            if sev in severity_counts:
                severity_counts[sev] += 1

        def _load_epdo_weights():
            default = {'K': 462, 'A': 62, 'B': 12, 'C': 5, 'O': 1}
            try:
                config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'CDOT', 'config.json')
                with open(config_path, 'r') as f:
                    return json.load(f).get('epdoWeights', default)
            except Exception:
                return default
        epdo_weights = _load_epdo_weights()
        epdo = sum(severity_counts[s] * epdo_weights[s] for s in severity_counts)

        return {
            'total': total,
            'fatal': severity_counts['K'],
            'serious_injury': severity_counts['A'],
            'moderate_injury': severity_counts['B'],
            'minor_injury': severity_counts['C'],
            'pdo': severity_counts['O'],
            'epdo': epdo,
            'data_file': str(CRASHES_FILE),
            'generated_at': datetime.now().isoformat()
        }
    except Exception as e:
        print(f"[ERROR] Failed to load crash data: {e}")
        return None

def load_grants_with_deadlines():
    """Load grants and identify upcoming deadlines."""
    if not GRANTS_FILE.exists():
        return []

    try:
        import csv
        with open(GRANTS_FILE, 'r') as f:
            reader = csv.DictReader(f)
            grants = list(reader)

        today = datetime.now().date()
        upcoming = []

        for grant in grants:
            deadline_str = grant.get('deadline', grant.get('Deadline', ''))
            if not deadline_str or deadline_str.lower() in ['ongoing', 'rolling', 'tbd', '']:
                continue

            try:
                for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%B %d, %Y']:
                    try:
                        deadline = datetime.strptime(deadline_str, fmt).date()
                        break
                    except ValueError:
                        continue
                else:
                    continue

                days_until = (deadline - today).days

                if 0 <= days_until <= 60:
                    upcoming.append({
                        'name': grant.get('name', grant.get('Name', 'Unknown Grant')),
                        'agency': grant.get('agency', grant.get('Agency', '')),
                        'deadline': deadline_str,
                        'days_until': days_until,
                        'funding': grant.get('funding', grant.get('Funding', '')),
                        'url': grant.get('url', grant.get('URL', ''))
                    })
            except Exception:
                continue

        upcoming.sort(key=lambda x: x['days_until'])
        return upcoming

    except Exception as e:
        print(f"[ERROR] Failed to load grants: {e}")
        return []

# =============================================================================
# EMAIL TEMPLATES
# =============================================================================

def generate_report_email(subscriber, crash_summary):
    """Generate scheduled report email content."""
    name = subscriber.get('name', 'Traffic Safety Professional')
    frequency = subscriber.get('reports', {}).get('frequency', 'monthly')

    if not crash_summary:
        crash_section = "Crash data is currently being updated. Please check back soon."
    else:
        crash_section = f"""
        <table style="width:100%;border-collapse:collapse;margin:20px 0;">
            <tr style="background:#1e3a5f;color:white;">
                <th style="padding:12px;text-align:left;">Metric</th>
                <th style="padding:12px;text-align:right;">Count</th>
            </tr>
            <tr style="background:#f8fafc;">
                <td style="padding:10px;border-bottom:1px solid #e2e8f0;">Total Crashes</td>
                <td style="padding:10px;text-align:right;font-weight:bold;border-bottom:1px solid #e2e8f0;">{crash_summary['total']:,}</td>
            </tr>
            <tr>
                <td style="padding:10px;border-bottom:1px solid #e2e8f0;color:#dc2626;">Fatal (K)</td>
                <td style="padding:10px;text-align:right;font-weight:bold;border-bottom:1px solid #e2e8f0;color:#dc2626;">{crash_summary['fatal']}</td>
            </tr>
            <tr style="background:#f8fafc;">
                <td style="padding:10px;border-bottom:1px solid #e2e8f0;color:#ea580c;">Serious Injury (A)</td>
                <td style="padding:10px;text-align:right;font-weight:bold;border-bottom:1px solid #e2e8f0;color:#ea580c;">{crash_summary['serious_injury']}</td>
            </tr>
            <tr>
                <td style="padding:10px;border-bottom:1px solid #e2e8f0;">Moderate Injury (B)</td>
                <td style="padding:10px;text-align:right;border-bottom:1px solid #e2e8f0;">{crash_summary['moderate_injury']}</td>
            </tr>
            <tr style="background:#f8fafc;">
                <td style="padding:10px;border-bottom:1px solid #e2e8f0;">Minor Injury (C)</td>
                <td style="padding:10px;text-align:right;border-bottom:1px solid #e2e8f0;">{crash_summary['minor_injury']}</td>
            </tr>
            <tr>
                <td style="padding:10px;border-bottom:1px solid #e2e8f0;">Property Damage Only (O)</td>
                <td style="padding:10px;text-align:right;border-bottom:1px solid #e2e8f0;">{crash_summary['pdo']}</td>
            </tr>
            <tr style="background:#dbeafe;">
                <td style="padding:10px;font-weight:bold;">EPDO Score</td>
                <td style="padding:10px;text-align:right;font-weight:bold;color:#1e40af;">{crash_summary['epdo']:,}</td>
            </tr>
        </table>
        """

    html_body = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
    </head>
    <body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;line-height:1.6;color:#1e293b;max-width:600px;margin:0 auto;padding:20px;">

        <div style="background:linear-gradient(135deg,#1e3a5f 0%,#1e40af 100%);padding:30px;border-radius:12px 12px 0 0;text-align:center;">
            <h1 style="color:white;margin:0;font-size:24px;">CRASH LENS</h1>
            <p style="color:rgba(255,255,255,0.9);margin:5px 0 0 0;font-size:14px;">Virginia Traffic Safety Analysis</p>
        </div>

        <div style="background:white;padding:30px;border:1px solid #e2e8f0;border-top:none;">
            <h2 style="color:#1e3a5f;margin-top:0;">Your {frequency.title()} Crash Report</h2>
            <p>Hello {name},</p>
            <p>Here is your scheduled {frequency} crash analysis summary:</p>
            {crash_section}
            <div style="background:#f0f9ff;border:1px solid #bae6fd;border-radius:8px;padding:15px;margin:20px 0;">
                <p style="margin:0;font-size:14px;">
                    <strong>View Full Analysis:</strong> Log in to CRASH LENS to access detailed reports,
                    interactive maps, and AI-powered insights.
                </p>
            </div>
            <a href="https://ecomhub200.github.io/Virginia/"
               style="display:inline-block;background:#1e40af;color:white;padding:12px 24px;text-decoration:none;border-radius:8px;font-weight:500;margin-top:10px;">
                Open CRASH LENS
            </a>
        </div>

        <div style="background:#f8fafc;padding:20px;border:1px solid #e2e8f0;border-top:none;border-radius:0 0 12px 12px;text-align:center;font-size:12px;color:#64748b;">
            <p style="margin:0 0 10px 0;">
                CRASH LENS - Virginia Crash Analysis Tool<br>
                Data Source: Virginia DMV Crash Records
            </p>
            <p style="margin:0;font-size:11px;">
                You received this email because you subscribed to {frequency} reports.<br>
                <a href="#" style="color:#3b82f6;">Unsubscribe</a> | <a href="#" style="color:#3b82f6;">Manage Preferences</a>
            </p>
        </div>

    </body>
    </html>
    """

    text_body = f"""
CRASH LENS - Your {frequency.title()} Crash Report

Hello {name},

Here is your scheduled {frequency} crash analysis summary:

Total Crashes: {crash_summary['total'] if crash_summary else 'N/A'}
Fatal (K): {crash_summary['fatal'] if crash_summary else 'N/A'}
Serious Injury (A): {crash_summary['serious_injury'] if crash_summary else 'N/A'}
EPDO Score: {crash_summary['epdo'] if crash_summary else 'N/A'}

View full analysis at: https://ecomhub200.github.io/Virginia/

---
CRASH LENS - Virginia Traffic Safety Analysis
Data Source: Virginia DMV Crash Records
    """

    return {
        'subject': f"CRASH LENS - Your {frequency.title()} Crash Report - {datetime.now().strftime('%B %Y')}",
        'html': html_body,
        'text': text_body
    }

def generate_grant_alert_email(subscriber, upcoming_grants):
    """Generate grant deadline alert email."""
    name = subscriber.get('name', 'Traffic Safety Professional')
    alert_days = subscriber.get('grants', {}).get('daysBeforeDeadline', [7, 14, 30])

    relevant_grants = [g for g in upcoming_grants if g['days_until'] in alert_days]

    if not relevant_grants:
        return None

    grants_html = ""
    for grant in relevant_grants:
        urgency_color = '#dc2626' if grant['days_until'] <= 7 else '#f59e0b' if grant['days_until'] <= 14 else '#059669'
        grants_html += f"""
        <div style="background:white;border:1px solid #e2e8f0;border-radius:8px;padding:15px;margin-bottom:15px;">
            <div style="display:flex;justify-content:space-between;align-items:start;">
                <div>
                    <h3 style="margin:0 0 5px 0;color:#1e3a5f;font-size:16px;">{grant['name']}</h3>
                    <p style="margin:0;color:#64748b;font-size:13px;">{grant['agency']}</p>
                </div>
                <span style="background:{urgency_color};color:white;padding:4px 10px;border-radius:12px;font-size:12px;font-weight:bold;">
                    {grant['days_until']} days
                </span>
            </div>
            <div style="margin-top:10px;padding-top:10px;border-top:1px solid #f1f5f9;">
                <p style="margin:0;font-size:13px;">
                    <strong>Deadline:</strong> {grant['deadline']}<br>
                    <strong>Funding:</strong> {grant['funding'] or 'See details'}
                </p>
                {f'<a href="{grant["url"]}" style="color:#3b82f6;font-size:13px;">View Grant Details</a>' if grant.get('url') else ''}
            </div>
        </div>
        """

    html_body = f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="UTF-8"></head>
    <body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;line-height:1.6;color:#1e293b;max-width:600px;margin:0 auto;padding:20px;">

        <div style="background:linear-gradient(135deg,#059669 0%,#047857 100%);padding:30px;border-radius:12px 12px 0 0;text-align:center;">
            <h1 style="color:white;margin:0;font-size:24px;">Grant Deadline Alert</h1>
            <p style="color:rgba(255,255,255,0.9);margin:5px 0 0 0;font-size:14px;">CRASH LENS Notification</p>
        </div>

        <div style="background:#f8fafc;padding:30px;border:1px solid #e2e8f0;border-top:none;">
            <p>Hello {name},</p>
            <p>The following grant deadlines are approaching:</p>
            {grants_html}
            <div style="background:#fef3c7;border:1px solid #fde68a;border-radius:8px;padding:15px;margin:20px 0;">
                <p style="margin:0;font-size:14px;color:#92400e;">
                    <strong>Tip:</strong> Use CRASH LENS to generate grant-ready location analysis
                    and AI-assisted application content.
                </p>
            </div>
            <a href="https://ecomhub200.github.io/Virginia/"
               style="display:inline-block;background:#059669;color:white;padding:12px 24px;text-decoration:none;border-radius:8px;font-weight:500;">
                Prepare Grant Application
            </a>
        </div>

        <div style="background:white;padding:20px;border:1px solid #e2e8f0;border-top:none;border-radius:0 0 12px 12px;text-align:center;font-size:12px;color:#64748b;">
            <p style="margin:0;">
                You received this because you enabled grant deadline alerts.<br>
                <a href="#" style="color:#3b82f6;">Manage Alert Preferences</a>
            </p>
        </div>

    </body>
    </html>
    """

    return {
        'subject': f"CRASH LENS - Grant Deadline Alert - {len(relevant_grants)} Upcoming",
        'html': html_body,
        'text': f"Grant deadlines approaching: {', '.join(g['name'] for g in relevant_grants)}"
    }

def generate_grant_summary_email(subscriber, upcoming_grants, crash_summary=None):
    """Generate comprehensive grant summary email with locations and funding match."""
    name = subscriber.get('name', 'Traffic Safety Professional')
    grant_prefs = subscriber.get('grants', {})
    include_deadlines = grant_prefs.get('includeDeadlines', True)
    include_top_locations = grant_prefs.get('includeTopLocations', True)
    include_funding_match = grant_prefs.get('includeFundingMatch', True)
    frequency = grant_prefs.get('frequency', 'weekly')

    # Deadline section
    deadlines_html = ""
    if include_deadlines and upcoming_grants:
        for grant in upcoming_grants[:8]:
            urgency_color = '#dc2626' if grant['days_until'] <= 7 else '#f59e0b' if grant['days_until'] <= 14 else '#0ea5e9' if grant['days_until'] <= 30 else '#059669'
            deadlines_html += f"""
            <div style="background:white;border:1px solid #e2e8f0;border-radius:8px;padding:14px;margin-bottom:10px;border-left:4px solid {urgency_color};">
                <div style="display:flex;justify-content:space-between;align-items:start;">
                    <div>
                        <strong style="color:#1e293b;font-size:14px;">{grant['name']}</strong>
                        <div style="color:#64748b;font-size:12px;margin-top:2px;">{grant['agency']}</div>
                    </div>
                    <span style="background:{urgency_color};color:white;padding:3px 10px;border-radius:12px;font-size:11px;font-weight:bold;white-space:nowrap;">
                        {grant['days_until']} days
                    </span>
                </div>
                <div style="margin-top:8px;font-size:12px;color:#475569;">
                    <strong>Deadline:</strong> {grant['deadline']}
                    {f' &bull; <strong>Funding:</strong> {grant["funding"]}' if grant.get('funding') else ''}
                </div>
                {f'<a href="{grant["url"]}" style="color:#3b82f6;font-size:12px;margin-top:4px;display:inline-block;">View Details &rarr;</a>' if grant.get('url') else ''}
            </div>"""
        deadlines_html = f"""
            <h3 style="color:#047857;font-size:16px;margin:20px 0 10px;border-bottom:2px solid #dcfce7;padding-bottom:6px;">
                Upcoming Grant Deadlines ({len(upcoming_grants)} total)
            </h3>
            {deadlines_html}"""

    # Crash statistics section
    stats_html = ""
    if crash_summary and include_top_locations:
        stats_html = f"""
            <h3 style="color:#047857;font-size:16px;margin:20px 0 10px;border-bottom:2px solid #dcfce7;padding-bottom:6px;">
                Jurisdiction Crash Overview
            </h3>
            <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:15px;">
                <div style="background:white;border:1px solid #fecaca;border-radius:8px;padding:12px;text-align:center;">
                    <div style="font-size:22px;font-weight:bold;color:#dc2626;">{crash_summary['fatal']}</div>
                    <div style="font-size:10px;color:#991b1b;">Fatal (K)</div>
                </div>
                <div style="background:white;border:1px solid #fed7aa;border-radius:8px;padding:12px;text-align:center;">
                    <div style="font-size:22px;font-weight:bold;color:#ea580c;">{crash_summary['serious_injury']}</div>
                    <div style="font-size:10px;color:#c2410c;">Serious Injury (A)</div>
                </div>
                <div style="background:white;border:1px solid #bfdbfe;border-radius:8px;padding:12px;text-align:center;">
                    <div style="font-size:22px;font-weight:bold;color:#1e40af;">{crash_summary['epdo']:,}</div>
                    <div style="font-size:10px;color:#1e3a5f;">EPDO Score</div>
                </div>
            </div>
            <p style="font-size:12px;color:#64748b;margin:0;">
                Total crashes: {crash_summary['total']:,} &bull;
                KA crashes: {crash_summary['fatal'] + crash_summary['serious_injury']}
            </p>"""

    now_str = datetime.now().strftime('%B %d, %Y')
    html_body = f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"></head>
    <body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;line-height:1.6;color:#1e293b;max-width:650px;margin:0 auto;padding:20px;">

        <div style="background:linear-gradient(135deg,#059669 0%,#047857 50%,#065f46 100%);padding:30px;border-radius:12px 12px 0 0;text-align:center;">
            <h1 style="color:white;margin:0;font-size:22px;">Grant Opportunities Summary</h1>
            <p style="color:rgba(255,255,255,0.9);margin:6px 0 0;font-size:13px;">Your {frequency.title()} CRASH LENS Report</p>
            <p style="color:rgba(255,255,255,0.7);margin:4px 0 0;font-size:11px;">{now_str}</p>
        </div>

        <div style="background:#f8fafc;padding:25px;border:1px solid #e2e8f0;border-top:none;">
            <p>Hello {name},</p>
            <p style="font-size:14px;">Here is your {frequency} grant opportunities summary:</p>

            {deadlines_html or '<p style="color:#64748b;font-style:italic;">No upcoming grant deadlines at this time.</p>'}

            {stats_html}

            <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px;padding:15px;margin:20px 0;">
                <p style="margin:0;font-size:13px;color:#047857;">
                    <strong>Next Steps:</strong> Log in to CRASH LENS to view detailed location rankings,
                    generate AI-powered grant applications, and prepare your submissions.
                </p>
            </div>
            <div style="text-align:center;margin-top:15px;">
                <a href="https://ecomhub200.github.io/Virginia/"
                   style="display:inline-block;background:#059669;color:white;padding:12px 28px;text-decoration:none;border-radius:8px;font-weight:600;font-size:14px;">
                    Open Grants Tab
                </a>
            </div>
        </div>

        <div style="background:white;padding:18px;border:1px solid #e2e8f0;border-top:none;border-radius:0 0 12px 12px;text-align:center;font-size:11px;color:#94a3b8;">
            <p style="margin:0 0 6px;">CRASH LENS - Traffic Safety Analysis Platform</p>
            <p style="margin:0;">
                You received this because you enabled {frequency} grant notifications.<br>
                <a href="#" style="color:#3b82f6;">Manage Preferences</a>
            </p>
        </div>

    </body>
    </html>
    """

    text_body = f"""
Grant Opportunities Summary - {now_str}

Hello {name},

{"Upcoming Deadlines:" if upcoming_grants else "No upcoming deadlines."}
{chr(10).join(f"  - {g['name']} ({g['agency']}) - {g['days_until']} days until {g['deadline']}" for g in (upcoming_grants or [])[:8])}

{"Crash Summary: " + str(crash_summary['total']) + " total crashes, EPDO: " + str(crash_summary['epdo']) if crash_summary else ""}

Log in to CRASH LENS for full analysis: https://ecomhub200.github.io/Virginia/
    """

    grant_count = len(upcoming_grants) if upcoming_grants else 0
    return {
        'subject': f"CRASH LENS - {frequency.title()} Grant Summary - {grant_count} Opportunities - {datetime.now().strftime('%B %Y')}",
        'html': html_body,
        'text': text_body
    }

def generate_weekly_digest_email(subscriber, crash_summary, upcoming_grants):
    """Generate weekly digest email."""
    name = subscriber.get('name', 'Traffic Safety Professional')

    grants_section = ""
    if upcoming_grants:
        grants_rows = ""
        for g in upcoming_grants[:10]:
            urgency = "critical" if g['days_until'] <= 7 else "warning" if g['days_until'] <= 14 else "normal"
            grants_rows += f"""
            <tr>
                <td style="padding:8px;border-bottom:1px solid #e2e8f0;font-size:13px;">{g['name'][:40]}...</td>
                <td style="padding:8px;border-bottom:1px solid #e2e8f0;font-size:13px;text-align:center;">{g['deadline']}</td>
                <td style="padding:8px;border-bottom:1px solid #e2e8f0;text-align:center;">
                    <span style="background:{'#fecaca' if urgency=='critical' else '#fef3c7' if urgency=='warning' else '#dcfce7'};
                                 color:{'#dc2626' if urgency=='critical' else '#92400e' if urgency=='warning' else '#059669'};
                                 padding:2px 8px;border-radius:10px;font-size:11px;font-weight:bold;">
                        {g['days_until']}d
                    </span>
                </td>
            </tr>
            """

        grants_section = f"""
        <h3 style="color:#059669;margin-top:25px;">Upcoming Grant Deadlines</h3>
        <table style="width:100%;border-collapse:collapse;background:white;border-radius:8px;overflow:hidden;">
            <thead>
                <tr style="background:#059669;color:white;">
                    <th style="padding:10px;text-align:left;font-size:12px;">Grant</th>
                    <th style="padding:10px;text-align:center;font-size:12px;">Deadline</th>
                    <th style="padding:10px;text-align:center;font-size:12px;">Days</th>
                </tr>
            </thead>
            <tbody>{grants_rows}</tbody>
        </table>
        """
    else:
        grants_section = "<p style='color:#64748b;'>No upcoming grant deadlines in the next 60 days.</p>"

    crash_section = ""
    if crash_summary:
        crash_section = f"""
        <h3 style="color:#1e40af;">Crash Data Summary</h3>
        <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px;">
            <div style="background:#fee2e2;padding:15px;border-radius:8px;text-align:center;">
                <div style="font-size:24px;font-weight:bold;color:#dc2626;">{crash_summary['fatal']}</div>
                <div style="font-size:11px;color:#991b1b;">Fatal</div>
            </div>
            <div style="background:#ffedd5;padding:15px;border-radius:8px;text-align:center;">
                <div style="font-size:24px;font-weight:bold;color:#ea580c;">{crash_summary['serious_injury']}</div>
                <div style="font-size:11px;color:#c2410c;">Serious Injury</div>
            </div>
            <div style="background:#dbeafe;padding:15px;border-radius:8px;text-align:center;">
                <div style="font-size:24px;font-weight:bold;color:#1e40af;">{crash_summary['total']:,}</div>
                <div style="font-size:11px;color:#1e3a5f;">Total</div>
            </div>
        </div>
        """

    html_body = f"""
    <!DOCTYPE html>
    <html>
    <head><meta charset="UTF-8"></head>
    <body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;line-height:1.6;color:#1e293b;max-width:600px;margin:0 auto;padding:20px;">

        <div style="background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);padding:30px;border-radius:12px 12px 0 0;text-align:center;">
            <h1 style="color:white;margin:0;font-size:24px;">Weekly Safety Digest</h1>
            <p style="color:rgba(255,255,255,0.9);margin:5px 0 0 0;font-size:14px;">
                Week of {datetime.now().strftime('%B %d, %Y')}
            </p>
        </div>

        <div style="background:#f8fafc;padding:30px;border:1px solid #e2e8f0;border-top:none;">
            <p>Hello {name},</p>
            <p>Here's your weekly traffic safety digest:</p>
            {crash_section}
            {grants_section}
            <div style="margin-top:25px;text-align:center;">
                <a href="https://ecomhub200.github.io/Virginia/"
                   style="display:inline-block;background:#667eea;color:white;padding:12px 24px;text-decoration:none;border-radius:8px;font-weight:500;">
                    View Full Analysis
                </a>
            </div>
        </div>

        <div style="background:white;padding:15px;border:1px solid #e2e8f0;border-top:none;border-radius:0 0 12px 12px;text-align:center;font-size:11px;color:#64748b;">
            CRASH LENS Weekly Digest | <a href="#" style="color:#3b82f6;">Unsubscribe</a>
        </div>

    </body>
    </html>
    """

    return {
        'subject': f"CRASH LENS - Weekly Safety Digest - {datetime.now().strftime('%B %d')}",
        'html': html_body,
        'text': f"Weekly digest for {datetime.now().strftime('%B %d, %Y')}"
    }

def generate_test_email(email):
    """Generate test email."""
    mode = "API" if BREVO_API_KEY else "SMTP"
    return {
        'subject': "CRASH LENS - Test Notification - Configuration Verified",
        'html': f"""
        <!DOCTYPE html>
        <html>
        <body style="font-family:sans-serif;padding:20px;max-width:500px;margin:0 auto;">
            <div style="background:#059669;color:white;padding:20px;border-radius:8px 8px 0 0;text-align:center;">
                <h1 style="margin:0;">Test Successful!</h1>
            </div>
            <div style="background:white;padding:20px;border:1px solid #e2e8f0;">
                <p>Your CRASH LENS email notifications are configured correctly.</p>
                <p><strong>Email:</strong> {email}</p>
                <p><strong>Timestamp:</strong> {datetime.now().isoformat()}</p>
                <p><strong>Provider:</strong> Brevo ({mode} mode)</p>
            </div>
            <div style="background:#f8fafc;padding:15px;border:1px solid #e2e8f0;border-top:none;border-radius:0 0 8px 8px;text-align:center;font-size:12px;color:#64748b;">
                CRASH LENS - Virginia Crash Analysis Tool
            </div>
        </body>
        </html>
        """,
        'text': f"Test email successful!\nEmail: {email}\nTimestamp: {datetime.now().isoformat()}\nProvider: Brevo ({mode} mode)"
    }

# =============================================================================
# MAIN FUNCTIONS
# =============================================================================

def send_scheduled_reports():
    """Send scheduled reports to all subscribers."""
    print("\n" + "="*60)
    print("CRASH LENS - Sending Scheduled Reports")
    print("="*60)

    subscribers = get_report_subscribers()
    print(f"Found {len(subscribers)} report subscribers")

    if not subscribers:
        print("No subscribers with report notifications enabled.")
        return

    crash_summary = load_crash_summary()

    success_count = 0
    for sub in subscribers:
        subscriber_email = sub.get('email')
        if not subscriber_email:
            print(f"[WARN] Skipping subscriber with missing email: {sub.get('id', 'unknown')}")
            continue
        email_content = generate_report_email(sub, crash_summary)
        if send_email(subscriber_email, email_content['subject'],
                     email_content['html'], email_content['text']):
            success_count += 1

    print(f"\nCompleted: {success_count}/{len(subscribers)} emails sent successfully")

def send_grant_alerts():
    """Send grant deadline alerts and grant summary emails."""
    print("\n" + "="*60)
    print("CRASH LENS - Sending Grant Alerts & Summaries")
    print("="*60)

    subscribers = get_grant_subscribers()
    print(f"Found {len(subscribers)} grant subscribers")

    if not subscribers:
        print("No subscribers with grant notifications enabled.")
        return

    upcoming_grants = load_grants_with_deadlines()
    print(f"Found {len(upcoming_grants)} grants with upcoming deadlines")

    crash_summary = load_crash_summary()

    success_count = 0
    summary_count = 0
    for sub in subscribers:
        subscriber_email = sub.get('email')
        if not subscriber_email:
            print(f"[WARN] Skipping subscriber with missing email: {sub.get('id', 'unknown')}")
            continue

        grant_prefs = sub.get('grants', {})

        # Send deadline alerts (existing behavior)
        if grant_prefs.get('deadlineAlerts', False) and upcoming_grants:
            email_content = generate_grant_alert_email(sub, upcoming_grants)
            if email_content:
                if send_email(subscriber_email, email_content['subject'],
                             email_content['html'], email_content['text']):
                    success_count += 1

        # Send grant summary report (new behavior for scheduled summaries)
        if grant_prefs.get('includeDeadlines') or grant_prefs.get('includeTopLocations') or grant_prefs.get('includeFundingMatch'):
            email_content = generate_grant_summary_email(sub, upcoming_grants, crash_summary)
            if email_content:
                if send_email(subscriber_email, email_content['subject'],
                             email_content['html'], email_content['text']):
                    summary_count += 1

    print(f"\nCompleted: {success_count} deadline alerts + {summary_count} grant summaries sent")

def send_weekly_digest():
    """Send weekly digest to subscribers."""
    print("\n" + "="*60)
    print("CRASH LENS - Sending Weekly Digest")
    print("="*60)

    subscribers = [s for s in get_grant_subscribers()
                   if s.get('grants', {}).get('weeklyDigest', False)]

    print(f"Found {len(subscribers)} digest subscribers")

    if not subscribers:
        print("No subscribers with weekly digest enabled.")
        return

    crash_summary = load_crash_summary()
    upcoming_grants = load_grants_with_deadlines()

    success_count = 0
    for sub in subscribers:
        subscriber_email = sub.get('email')
        if not subscriber_email:
            print(f"[WARN] Skipping subscriber with missing email: {sub.get('id', 'unknown')}")
            continue
        email_content = generate_weekly_digest_email(sub, crash_summary, upcoming_grants)
        if send_email(subscriber_email, email_content['subject'],
                     email_content['html'], email_content['text']):
            success_count += 1

    print(f"\nCompleted: {success_count}/{len(subscribers)} digest emails sent")

def send_test(email):
    """Send test email."""
    print("\n" + "="*60)
    print(f"CRASH LENS - Sending Test Email to {email}")
    print("="*60)

    email_content = generate_test_email(email)
    if send_email(email, email_content['subject'],
                 email_content['html'], email_content['text']):
        print("\nTest email sent successfully!")
    else:
        print("\nFailed to send test email. Check your Brevo configuration.")
        sys.exit(1)

# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='CRASH LENS Email Notification System (Brevo)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Required environment variables:
  BREVO_API_KEY              Brevo API key (starts with xkeysib-)
  NOTIFICATION_FROM_EMAIL    Verified sender email address

Alternative (SMTP mode):
  BREVO_SMTP_LOGIN           Your Brevo account email
  BREVO_SMTP_PASSWORD        Brevo SMTP key
  NOTIFICATION_FROM_EMAIL    Verified sender email address

Examples:
  python send_notifications.py --type reports     Send scheduled reports
  python send_notifications.py --type grants      Send grant deadline alerts
  python send_notifications.py --type digest      Send weekly digest
  python send_notifications.py --type test --email user@example.com
        """
    )

    parser.add_argument('--type', '-t', required=True,
                       choices=['reports', 'grants', 'digest', 'test'],
                       help='Type of notification to send')
    parser.add_argument('--email', '-e',
                       help='Email address (required for test)')

    args = parser.parse_args()

    validate_config()

    if args.type == 'reports':
        send_scheduled_reports()
    elif args.type == 'grants':
        send_grant_alerts()
    elif args.type == 'digest':
        send_weekly_digest()
    elif args.type == 'test':
        if not args.email:
            print("[ERROR] --email is required for test")
            sys.exit(1)
        send_test(args.email)

if __name__ == '__main__':
    main()
