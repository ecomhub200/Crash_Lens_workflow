#!/usr/bin/env python3
"""
CRASH LENS Email Marketing System
Send marketing campaigns to government agency contacts via Brevo.

Uses Brevo's campaign API for marketing emails (separate from transactional).
This provides open/click tracking, unsubscribe management, and campaign analytics.

Usage:
    python send_marketing.py --campaign product-launch     # Send product launch campaign
    python send_marketing.py --campaign feature-update     # Send feature update announcement
    python send_marketing.py --campaign demo-invite        # Send demo invitation
    python send_marketing.py --campaign custom --subject "..." --template path/to/template.html
    python send_marketing.py --list                        # List available campaigns
    python send_marketing.py --sync-contacts               # Sync subscribers to Brevo contact list
    python send_marketing.py --preview product-launch      # Preview campaign HTML without sending
"""

import os
import sys
import json
import argparse
import urllib.request
import urllib.error
from datetime import datetime
from pathlib import Path

# =============================================================================
# CONFIGURATION
# =============================================================================

BREVO_API_KEY = os.environ.get('BREVO_API_KEY')
FROM_EMAIL = os.environ.get('NOTIFICATION_FROM_EMAIL', 'notifications@aicreatesai.com')
FROM_NAME = os.environ.get('MARKETING_FROM_NAME', 'CRASH LENS')
REPLY_TO = os.environ.get('MARKETING_REPLY_TO', FROM_EMAIL)

BASE_DIR = Path(__file__).parent
SUBSCRIBERS_FILE = BASE_DIR / 'data' / 'subscribers.json'
MARKETING_CONTACTS_FILE = BASE_DIR / 'data' / 'marketing_contacts.json'
TEMPLATES_DIR = BASE_DIR / 'email_templates'

# Brevo contact list ID for marketing (created on first sync)
BREVO_LIST_ID = os.environ.get('BREVO_MARKETING_LIST_ID')

# =============================================================================
# BREVO API HELPERS
# =============================================================================

def brevo_api_request(endpoint, method='GET', data=None):
    """Make a request to the Brevo API."""
    if not BREVO_API_KEY:
        print("[ERROR] BREVO_API_KEY environment variable not set")
        print("  Get your API key from: Brevo Dashboard > SMTP & API > API Keys")
        sys.exit(1)

    url = f'https://api.brevo.com/v3/{endpoint}'
    headers = {
        'accept': 'application/json',
        'content-type': 'application/json',
        'api-key': BREVO_API_KEY
    }

    req = urllib.request.Request(
        url,
        data=json.dumps(data).encode('utf-8') if data else None,
        headers=headers,
        method=method
    )

    try:
        with urllib.request.urlopen(req) as response:
            body = response.read().decode()
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as e:
        error_body = e.read().decode()
        print(f"[ERROR] Brevo API {method} {endpoint}: {e.code} - {error_body}")
        return None

# =============================================================================
# CONTACT MANAGEMENT
# =============================================================================

def load_marketing_contacts():
    """Load marketing contacts from JSON file."""
    # First check dedicated marketing contacts file
    if MARKETING_CONTACTS_FILE.exists():
        try:
            with open(MARKETING_CONTACTS_FILE, 'r') as f:
                data = json.load(f)
                return data.get('contacts', [])
        except Exception as e:
            print(f"[WARN] Failed to load marketing contacts: {e}")

    # Fall back to subscribers file (filter for marketing-enabled)
    if SUBSCRIBERS_FILE.exists():
        try:
            with open(SUBSCRIBERS_FILE, 'r') as f:
                data = json.load(f)
                return [
                    s for s in data.get('subscribers', [])
                    if s.get('verified', False) and s.get('email')
                ]
        except Exception as e:
            print(f"[ERROR] Failed to load subscribers: {e}")

    return []

def sync_contacts_to_brevo():
    """Sync local contacts to a Brevo contact list for campaign sending."""
    contacts = load_marketing_contacts()
    if not contacts:
        print("[WARN] No contacts found to sync")
        return

    print(f"Syncing {len(contacts)} contacts to Brevo...")

    # Create or get the marketing contact list
    list_id = BREVO_LIST_ID
    if not list_id:
        # Create a new list
        result = brevo_api_request('contacts/lists', 'POST', {
            'name': f'CRASH LENS Marketing - {datetime.now().strftime("%Y-%m")}',
            'folderId': 1  # Default folder
        })
        if result and 'id' in result:
            list_id = result['id']
            print(f"[INFO] Created Brevo list ID: {list_id}")
            print(f"  Set BREVO_MARKETING_LIST_ID={list_id} in your environment")
        else:
            print("[ERROR] Failed to create Brevo contact list")
            return

    # Batch import contacts
    success_count = 0
    for contact in contacts:
        email = contact.get('email')
        if not email:
            continue

        result = brevo_api_request('contacts', 'POST', {
            'email': email,
            'attributes': {
                'FIRSTNAME': contact.get('name', '').split()[0] if contact.get('name') else '',
                'LASTNAME': ' '.join(contact.get('name', '').split()[1:]) if contact.get('name') else '',
                'COMPANY': contact.get('agency', ''),
            },
            'listIds': [int(list_id)],
            'updateEnabled': True
        })
        if result is not None:
            success_count += 1
            print(f"  [OK] {email}")
        else:
            print(f"  [FAIL] {email}")

    print(f"\nSynced: {success_count}/{len(contacts)} contacts")

# =============================================================================
# CAMPAIGN TEMPLATES
# =============================================================================

CAMPAIGNS = {
    'product-launch': {
        'name': 'CRASH LENS Product Launch',
        'subject': 'Introducing CRASH LENS - AI-Powered Traffic Safety Analysis for Your Agency',
        'description': 'Initial outreach to government agencies about CRASH LENS platform',
    },
    'feature-update': {
        'name': 'CRASH LENS Feature Update',
        'subject': 'New in CRASH LENS: Enhanced Safety Analysis & Grant Tools',
        'description': 'Announce new features and improvements to existing users',
    },
    'demo-invite': {
        'name': 'CRASH LENS Demo Invitation',
        'subject': 'See CRASH LENS in Action - Schedule Your Agency Demo',
        'description': 'Invite prospects to schedule a product demonstration',
    },
    'grant-season': {
        'name': 'Grant Season Alert',
        'subject': 'Upcoming Traffic Safety Grant Deadlines - CRASH LENS Can Help',
        'description': 'Seasonal campaign timed with major grant application periods',
    },
}

def get_campaign_html(campaign_key):
    """Generate HTML for a marketing campaign."""
    # Check for custom template file first
    template_file = TEMPLATES_DIR / f'{campaign_key}.html'
    if template_file.exists():
        with open(template_file, 'r') as f:
            return f.read()

    # Use built-in templates
    templates = {
        'product-launch': _template_product_launch,
        'feature-update': _template_feature_update,
        'demo-invite': _template_demo_invite,
        'grant-season': _template_grant_season,
    }

    generator = templates.get(campaign_key)
    if not generator:
        print(f"[ERROR] No template found for campaign: {campaign_key}")
        return None

    return generator()

def _template_product_launch():
    return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"></head>
<body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;line-height:1.6;color:#1e293b;max-width:600px;margin:0 auto;padding:0;background:#f1f5f9;">

    <!-- Header -->
    <div style="background:linear-gradient(135deg,#1e3a5f 0%,#1e40af 100%);padding:40px 30px;text-align:center;">
        <h1 style="color:white;margin:0;font-size:28px;letter-spacing:-0.5px;">CRASH LENS</h1>
        <p style="color:rgba(255,255,255,0.9);margin:8px 0 0 0;font-size:16px;">AI-Powered Traffic Safety Analysis</p>
    </div>

    <!-- Hero -->
    <div style="background:white;padding:40px 30px;border-bottom:1px solid #e2e8f0;">
        <h2 style="color:#1e3a5f;margin:0 0 15px 0;font-size:22px;">Transform Your Agency's Crash Data Into Actionable Safety Insights</h2>
        <p style="color:#475569;font-size:15px;">
            CRASH LENS is a comprehensive traffic safety analysis platform built for Virginia transportation
            agencies. Turn complex crash data into clear, actionable insights - powered by AI.
        </p>
    </div>

    <!-- Features Grid -->
    <div style="background:#f8fafc;padding:30px;">
        <h3 style="color:#1e3a5f;margin:0 0 20px 0;text-align:center;">What CRASH LENS Does For Your Agency</h3>

        <div style="background:white;border:1px solid #e2e8f0;border-radius:8px;padding:20px;margin-bottom:15px;">
            <h4 style="color:#1e40af;margin:0 0 8px 0;">Crash Analysis Dashboard</h4>
            <p style="color:#475569;margin:0;font-size:14px;">Interactive maps, severity breakdowns, trend analysis, and collision pattern detection across your entire jurisdiction.</p>
        </div>

        <div style="background:white;border:1px solid #e2e8f0;border-radius:8px;padding:20px;margin-bottom:15px;">
            <h4 style="color:#059669;margin:0 0 8px 0;">Grant Application Support</h4>
            <p style="color:#475569;margin:0;font-size:14px;">AI-generated grant narratives, location ranking by EPDO score, and automatic deadline tracking for HSIP, SS4A, and more.</p>
        </div>

        <div style="background:white;border:1px solid #e2e8f0;border-radius:8px;padding:20px;margin-bottom:15px;">
            <h4 style="color:#7c3aed;margin:0 0 8px 0;">Countermeasure Recommendations</h4>
            <p style="color:#475569;margin:0;font-size:14px;">Evidence-based safety countermeasures with Crash Modification Factors (CMF) matched to your specific crash patterns.</p>
        </div>

        <div style="background:white;border:1px solid #e2e8f0;border-radius:8px;padding:20px;">
            <h4 style="color:#dc2626;margin:0 0 8px 0;">Automated Reports & Alerts</h4>
            <p style="color:#475569;margin:0;font-size:14px;">Scheduled safety reports delivered to your inbox. Grant deadline alerts ensure you never miss a funding opportunity.</p>
        </div>
    </div>

    <!-- CTA -->
    <div style="background:white;padding:30px;text-align:center;border-top:1px solid #e2e8f0;">
        <p style="color:#1e3a5f;font-size:16px;font-weight:600;margin:0 0 15px 0;">Ready to improve traffic safety in your jurisdiction?</p>
        <a href="https://crashlens.aicreatesai.com"
           style="display:inline-block;background:#1e40af;color:white;padding:14px 32px;text-decoration:none;border-radius:8px;font-weight:600;font-size:16px;">
            Get Started Free
        </a>
        <p style="color:#64748b;font-size:13px;margin:15px 0 0 0;">No credit card required. Free tier available for qualifying agencies.</p>
    </div>

    <!-- Footer -->
    <div style="padding:20px 30px;text-align:center;font-size:12px;color:#64748b;">
        <p style="margin:0 0 10px 0;">
            CRASH LENS by AI Creates AI<br>
            Empowering transportation agencies with data-driven safety analysis
        </p>
        <p style="margin:0;">
            <a href="{{{{unsubscribe}}}}" style="color:#3b82f6;">Unsubscribe</a> |
            <a href="https://crashlens.aicreatesai.com" style="color:#3b82f6;">Visit Website</a>
        </p>
    </div>

</body>
</html>"""

def _template_feature_update():
    return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"></head>
<body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;line-height:1.6;color:#1e293b;max-width:600px;margin:0 auto;padding:0;background:#f1f5f9;">

    <div style="background:linear-gradient(135deg,#7c3aed 0%,#4f46e5 100%);padding:30px;text-align:center;">
        <h1 style="color:white;margin:0;font-size:24px;">CRASH LENS Updates</h1>
        <p style="color:rgba(255,255,255,0.9);margin:8px 0 0 0;font-size:14px;">{datetime.now().strftime('%B %Y')} Release</p>
    </div>

    <div style="background:white;padding:30px;">
        <h2 style="color:#1e3a5f;margin:0 0 15px 0;">What's New This Month</h2>
        <p style="color:#475569;">We've been working hard to make CRASH LENS even more powerful for your agency. Here's what's new:</p>

        <div style="border-left:4px solid #1e40af;padding-left:15px;margin:20px 0;">
            <h4 style="color:#1e40af;margin:0 0 5px 0;">Enhanced AI Analysis</h4>
            <p style="color:#475569;margin:0;font-size:14px;">Our AI assistant now provides more detailed countermeasure recommendations with location-specific crash profile context.</p>
        </div>

        <div style="border-left:4px solid #059669;padding-left:15px;margin:20px 0;">
            <h4 style="color:#059669;margin:0 0 5px 0;">Improved Grant Tools</h4>
            <p style="color:#475569;margin:0;font-size:14px;">Updated grant database with SS4A and HSIP deadlines. One-click export for grant application narratives.</p>
        </div>

        <div style="border-left:4px solid #dc2626;padding-left:15px;margin:20px 0;">
            <h4 style="color:#dc2626;margin:0 0 5px 0;">Before/After Study</h4>
            <p style="color:#475569;margin:0;font-size:14px;">New before/after analysis tool to measure the effectiveness of implemented safety improvements.</p>
        </div>

        <div style="text-align:center;margin-top:25px;">
            <a href="https://crashlens.aicreatesai.com"
               style="display:inline-block;background:#4f46e5;color:white;padding:12px 28px;text-decoration:none;border-radius:8px;font-weight:600;">
                Explore New Features
            </a>
        </div>
    </div>

    <div style="padding:20px 30px;text-align:center;font-size:12px;color:#64748b;">
        <p style="margin:0;">
            <a href="{{{{unsubscribe}}}}" style="color:#3b82f6;">Unsubscribe</a> |
            <a href="https://crashlens.aicreatesai.com" style="color:#3b82f6;">CRASH LENS</a>
        </p>
    </div>

</body>
</html>"""

def _template_demo_invite():
    return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"></head>
<body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;line-height:1.6;color:#1e293b;max-width:600px;margin:0 auto;padding:0;background:#f1f5f9;">

    <div style="background:linear-gradient(135deg,#059669 0%,#047857 100%);padding:40px 30px;text-align:center;">
        <h1 style="color:white;margin:0;font-size:26px;">See CRASH LENS In Action</h1>
        <p style="color:rgba(255,255,255,0.9);margin:10px 0 0 0;font-size:15px;">Schedule a personalized demo for your agency</p>
    </div>

    <div style="background:white;padding:30px;">
        <p style="color:#475569;font-size:15px;">
            Discover how CRASH LENS can help your agency analyze crash data, identify high-priority
            safety locations, and streamline grant applications - all in one platform.
        </p>

        <h3 style="color:#1e3a5f;margin:25px 0 15px 0;">In 30 minutes, we'll show you:</h3>

        <div style="padding:8px 0;font-size:14px;color:#475569;">
            <span style="color:#059669;font-weight:bold;margin-right:8px;">1.</span>
            How to instantly analyze crash patterns at any location in your jurisdiction
        </div>
        <div style="padding:8px 0;font-size:14px;color:#475569;">
            <span style="color:#059669;font-weight:bold;margin-right:8px;">2.</span>
            AI-powered countermeasure recommendations based on your specific crash data
        </div>
        <div style="padding:8px 0;font-size:14px;color:#475569;">
            <span style="color:#059669;font-weight:bold;margin-right:8px;">3.</span>
            One-click grant application narrative generation for HSIP, SS4A, and more
        </div>
        <div style="padding:8px 0;font-size:14px;color:#475569;">
            <span style="color:#059669;font-weight:bold;margin-right:8px;">4.</span>
            Automated safety reports and grant deadline tracking
        </div>

        <div style="text-align:center;margin-top:30px;">
            <a href="https://crashlens.aicreatesai.com"
               style="display:inline-block;background:#059669;color:white;padding:14px 32px;text-decoration:none;border-radius:8px;font-weight:600;font-size:16px;">
                Schedule Your Demo
            </a>
        </div>

        <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px;padding:15px;margin-top:25px;text-align:center;">
            <p style="margin:0;font-size:14px;color:#166534;">
                <strong>Free for qualifying government agencies.</strong><br>
                No credit card or procurement process required to get started.
            </p>
        </div>
    </div>

    <div style="padding:20px 30px;text-align:center;font-size:12px;color:#64748b;">
        <p style="margin:0;">
            <a href="{{{{unsubscribe}}}}" style="color:#3b82f6;">Unsubscribe</a> |
            <a href="https://crashlens.aicreatesai.com" style="color:#3b82f6;">CRASH LENS</a>
        </p>
    </div>

</body>
</html>"""

def _template_grant_season():
    return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"></head>
<body style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;line-height:1.6;color:#1e293b;max-width:600px;margin:0 auto;padding:0;background:#f1f5f9;">

    <div style="background:linear-gradient(135deg,#f59e0b 0%,#d97706 100%);padding:30px;text-align:center;">
        <h1 style="color:white;margin:0;font-size:24px;">Grant Season Is Here</h1>
        <p style="color:rgba(255,255,255,0.9);margin:8px 0 0 0;font-size:14px;">Don't miss critical traffic safety funding deadlines</p>
    </div>

    <div style="background:white;padding:30px;">
        <h2 style="color:#1e3a5f;margin:0 0 15px 0;">Upcoming Grant Deadlines</h2>
        <p style="color:#475569;font-size:15px;">
            Major traffic safety grants are opening soon. CRASH LENS helps your agency prepare
            competitive applications with data-driven analysis and AI-generated narratives.
        </p>

        <div style="background:#fffbeb;border:1px solid #fde68a;border-radius:8px;padding:20px;margin:20px 0;">
            <h4 style="color:#92400e;margin:0 0 10px 0;">Key Programs to Watch:</h4>
            <ul style="color:#475569;font-size:14px;margin:0;padding-left:20px;">
                <li style="margin-bottom:8px;"><strong>HSIP</strong> - Highway Safety Improvement Program</li>
                <li style="margin-bottom:8px;"><strong>SS4A</strong> - Safe Streets and Roads for All</li>
                <li style="margin-bottom:8px;"><strong>Section 402</strong> - State and Community Highway Safety</li>
                <li><strong>SMART</strong> - Strengthening Mobility and Revolutionizing Transportation</li>
            </ul>
        </div>

        <h3 style="color:#1e3a5f;margin:25px 0 10px 0;">How CRASH LENS Helps You Win Grants:</h3>
        <p style="color:#475569;font-size:14px;">
            <strong>Location Ranking:</strong> Automatically rank intersections and corridors by EPDO severity score to identify the strongest grant candidates.<br><br>
            <strong>AI Narratives:</strong> Generate compelling application narratives backed by your actual crash data and proven safety countermeasures.<br><br>
            <strong>Deadline Tracking:</strong> Never miss a deadline with automated alerts sent to your inbox.
        </p>

        <div style="text-align:center;margin-top:25px;">
            <a href="https://crashlens.aicreatesai.com"
               style="display:inline-block;background:#d97706;color:white;padding:14px 28px;text-decoration:none;border-radius:8px;font-weight:600;">
                Prepare Your Grant Application
            </a>
        </div>
    </div>

    <div style="padding:20px 30px;text-align:center;font-size:12px;color:#64748b;">
        <p style="margin:0;">
            <a href="{{{{unsubscribe}}}}" style="color:#3b82f6;">Unsubscribe</a> |
            <a href="https://crashlens.aicreatesai.com" style="color:#3b82f6;">CRASH LENS</a>
        </p>
    </div>

</body>
</html>"""

# =============================================================================
# CAMPAIGN SENDING
# =============================================================================

def send_campaign(campaign_key, subject_override=None, custom_template=None):
    """Create and send a marketing campaign via Brevo."""
    campaign_info = CAMPAIGNS.get(campaign_key)
    if not campaign_info and not custom_template:
        print(f"[ERROR] Unknown campaign: {campaign_key}")
        print(f"Available campaigns: {', '.join(CAMPAIGNS.keys())}")
        sys.exit(1)

    subject = subject_override or (campaign_info or {}).get('subject', f'CRASH LENS - {campaign_key}')
    campaign_name = (campaign_info or {}).get('name', f'Custom: {campaign_key}')

    # Get HTML content
    if custom_template:
        template_path = Path(custom_template)
        if not template_path.exists():
            print(f"[ERROR] Template file not found: {custom_template}")
            sys.exit(1)
        with open(template_path, 'r') as f:
            html_content = f.read()
    else:
        html_content = get_campaign_html(campaign_key)
        if not html_content:
            sys.exit(1)

    if not BREVO_LIST_ID:
        print("[ERROR] BREVO_MARKETING_LIST_ID not set. Run --sync-contacts first to create a list.")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"CRASH LENS - Sending Marketing Campaign")
    print(f"{'='*60}")
    print(f"Campaign: {campaign_name}")
    print(f"Subject:  {subject}")
    print(f"List ID:  {BREVO_LIST_ID}")

    # Create campaign via Brevo API
    result = brevo_api_request('emailCampaigns', 'POST', {
        'name': f"{campaign_name} - {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        'subject': subject,
        'sender': {'name': FROM_NAME, 'email': FROM_EMAIL},
        'replyTo': REPLY_TO,
        'htmlContent': html_content,
        'recipients': {'listIds': [int(BREVO_LIST_ID)]},
        'inlineImageActivation': False,
    })

    if not result or 'id' not in result:
        print("[ERROR] Failed to create campaign")
        return

    campaign_id = result['id']
    print(f"[OK] Campaign created (ID: {campaign_id})")

    # Send the campaign immediately
    send_result = brevo_api_request(f'emailCampaigns/{campaign_id}/sendNow', 'POST')
    if send_result is not None:
        print(f"[SUCCESS] Campaign sent!")
        print(f"\nView campaign stats in your Brevo dashboard.")
    else:
        print(f"[WARN] Campaign created but may need manual send from Brevo dashboard")
        print(f"  Campaign ID: {campaign_id}")

def preview_campaign(campaign_key):
    """Preview campaign HTML without sending."""
    html = get_campaign_html(campaign_key)
    if html:
        preview_file = BASE_DIR / f'_preview_{campaign_key}.html'
        with open(preview_file, 'w') as f:
            f.write(html)
        print(f"[OK] Preview saved to: {preview_file}")
        print(f"  Open this file in a browser to preview the campaign email.")

def list_campaigns():
    """List available marketing campaigns."""
    print("\nAvailable Marketing Campaigns:")
    print("=" * 60)
    for key, info in CAMPAIGNS.items():
        print(f"\n  {key}")
        print(f"    Name:    {info['name']}")
        print(f"    Subject: {info['subject']}")
        print(f"    About:   {info['description']}")
    print(f"\nUsage: python send_marketing.py --campaign <name>")
    print(f"  Or create a custom template in email_templates/<name>.html")

# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='CRASH LENS Email Marketing System (via Brevo)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Required environment variables:
  BREVO_API_KEY              Your Brevo API key
  NOTIFICATION_FROM_EMAIL    Verified sender email
  BREVO_MARKETING_LIST_ID    Brevo contact list ID (created by --sync-contacts)

Examples:
  python send_marketing.py --list                           List available campaigns
  python send_marketing.py --sync-contacts                  Sync contacts to Brevo
  python send_marketing.py --preview product-launch         Preview campaign HTML
  python send_marketing.py --campaign product-launch        Send product launch campaign
  python send_marketing.py --campaign custom --subject "Subject" --template ./my-email.html
        """
    )

    parser.add_argument('--campaign', '-c',
                       help='Campaign to send (see --list for options)')
    parser.add_argument('--subject', '-s',
                       help='Override campaign subject line')
    parser.add_argument('--template',
                       help='Path to custom HTML template')
    parser.add_argument('--list', '-l', action='store_true',
                       help='List available campaigns')
    parser.add_argument('--sync-contacts', action='store_true',
                       help='Sync local contacts to Brevo contact list')
    parser.add_argument('--preview',
                       help='Preview campaign HTML without sending')

    args = parser.parse_args()

    if args.list:
        list_campaigns()
    elif args.sync_contacts:
        sync_contacts_to_brevo()
    elif args.preview:
        preview_campaign(args.preview)
    elif args.campaign:
        send_campaign(args.campaign, args.subject, args.template)
    else:
        parser.print_help()

if __name__ == '__main__':
    main()
