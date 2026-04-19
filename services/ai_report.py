import os
import json
import smtplib
from datetime import datetime, timedelta
from email.message import EmailMessage


def generate_daily_ai_report():
    """
    Global daily job (not per-shop).
    Collects 24h health data across every active shop, asks Claude Haiku
    for a plain-English summary, and emails it to REPORT_EMAIL.
    Catches all exceptions so this job never lands in RQ's failed registry.
    """
    try:
        from app import app
        with app.app_context():
            _run_report()
    except Exception as e:
        print(f"[AI Report] Suppressed top-level error: {e}")


def _run_report():
    import anthropic
    from models import db, Shop, SyncHealth, SyncLog, FailedSyncOrder, ProcessedOrder
    from utils import q_default, q_critical, log_event

    api_key      = os.getenv('ANTHROPIC_API_KEY')
    report_email = os.getenv('REPORT_EMAIL')

    if not api_key:
        log_event('AI Report', 'Warning', 'ANTHROPIC_API_KEY not set — skipping daily report.')
        return
    if not report_email:
        log_event('AI Report', 'Warning', 'REPORT_EMAIL not set — skipping daily report.')
        return

    since = datetime.utcnow() - timedelta(hours=24)

    # ── 1. Collect data ──────────────────────────────────────────────────────
    health_data = {
        'report_date': datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC'),
        'shops': [],
        'queue': {},
    }

    try:
        health_data['queue'] = {
            'pending': q_critical.count + q_default.count,
            'active':  q_critical.started_job_registry.count + q_default.started_job_registry.count,
            'rq_failed': q_critical.failed_job_registry.count + q_default.failed_job_registry.count,
        }
    except Exception:
        health_data['queue'] = {'pending': 0, 'active': 0, 'rq_failed': 0}

    active_shops = Shop.query.filter_by(is_active=True).all()

    for shop in active_shops:
        # SyncHealth per entity
        health_rows = SyncHealth.query.filter_by(shop_url=shop.shop_url).all()
        entities = [
            {
                'entity':               row.entity,
                'last_success':         row.last_success_at.strftime('%Y-%m-%d %H:%M') if row.last_success_at else 'never',
                'last_attempt':         row.last_attempt_at.strftime('%Y-%m-%d %H:%M') if row.last_attempt_at else 'never',
                'consecutive_failures': row.consecutive_failures,
                'last_error':           (row.last_error[:120] if row.last_error else None),
            }
            for row in health_rows
        ]

        # Log counts last 24h
        logs = SyncLog.query.filter(
            SyncLog.shop_url == shop.shop_url,
            SyncLog.timestamp >= since
        ).all()
        log_summary = {}
        for log in logs:
            log_summary[log.status] = log_summary.get(log.status, 0) + 1

        # Failed orders still pending retry
        failed_orders = FailedSyncOrder.query.filter_by(shop_url=shop.shop_url).count()

        # Orders successfully pushed to Odoo in last 24h
        orders_24h = ProcessedOrder.query.filter(
            ProcessedOrder.shop_url == shop.shop_url,
            ProcessedOrder.created_at >= since
        ).count()

        health_data['shops'].append({
            'shop':                       shop.shop_url,
            'orders_processed_24h':       orders_24h,
            'failed_orders_pending_retry': failed_orders,
            'log_summary_24h':            log_summary,
            'sync_entities':              entities,
        })

    # ── 2. Ask Claude ────────────────────────────────────────────────────────
    client = anthropic.Anthropic(api_key=api_key)

    prompt = f"""You are a technical operations assistant for an Odoo-Shopify connector app.

Here is the system health data for the last 24 hours across all connected shops:

{json.dumps(health_data, indent=2)}

Write a concise daily health report in plain text (no markdown, no bullet symbols, just clean paragraphs).

Structure it exactly like this:

OVERALL STATUS: one sentence.

WHAT WENT WELL: note the positives briefly.

ISSUES DETECTED: any failures, errors, or concerning patterns. Be specific — name the shop, entity, and error. If none, say "None."

ACTION NEEDED: what the team should do today. If nothing, say "No action required."

Be direct, specific, and keep the total under 200 words."""

    try:
        response = client.messages.create(
            model='claude-haiku-4-5-20251001',
            max_tokens=512,
            messages=[{'role': 'user', 'content': prompt}]
        )
        report_text = response.content[0].text
    except Exception as e:
        log_event('AI Report', 'Error', f'Claude API call failed: {e}')
        return

    # ── 3. Email the report ──────────────────────────────────────────────────
    try:
        _send_report_email(report_email, report_text, health_data['report_date'])
        log_event('AI Report', 'Success', f'Daily AI report sent to {report_email}')
    except Exception as e:
        log_event('AI Report', 'Error', f'Failed to send report email: {e}')


def _send_report_email(recipient, report_text, report_date):
    SMTP_SERVER   = 'premium74.web-hosting.com'
    SMTP_PORT     = 465
    SENDER_EMAIL  = 'hello@tripsterdevelopers.com'
    SENDER_PASSWORD = os.getenv('SMTP_PASSWORD')

    html = f"""<html>
<body style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;color:#333;">
  <div style="background:#1a1a2e;color:white;padding:20px;border-radius:8px 8px 0 0;">
    <h2 style="margin:0;">Daily AI Health Report</h2>
    <p style="margin:5px 0 0;opacity:0.7;font-size:13px;">{report_date}</p>
  </div>
  <div style="background:#f9f9f9;padding:24px;border-radius:0 0 8px 8px;
              white-space:pre-line;line-height:1.8;font-size:14px;">
{report_text}
  </div>
  <p style="font-size:11px;color:#aaa;text-align:center;margin-top:12px;">
    Generated by Claude Haiku · Odoo-Shopify Connector
  </p>
</body>
</html>"""

    msg = EmailMessage()
    msg['Subject'] = f'[Odoo Connector] Daily Health Report — {report_date}'
    msg['From']    = SENDER_EMAIL
    msg['To']      = recipient
    msg.set_content(report_text)
    msg.add_alternative(html, subtype='html')

    with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as smtp:
        smtp.login(SENDER_EMAIL, SENDER_PASSWORD)
        smtp.send_message(msg)
