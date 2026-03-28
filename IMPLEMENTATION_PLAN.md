# Yape Voucher Processing System — Simple Implementation Plan

## 1) Goal (in plain English)

Build one small Python script that:
1. Reads new Google Form responses from your existing Google Sheet.
2. Opens each voucher image from Google Drive.
3. Sends the image to OpenAI to extract key voucher data.
4. Writes the extracted data to a separate tab named **processed data**.
5. Never edits the raw responses tab.
6. Can run manually anytime and automatically every day at midnight.

---

## 2) Keep it simple: minimal architecture

Use a **single Python file** with a few helper functions (not classes). Keep configuration in a `.env` file.

### Why this is good for you
- Easy to understand.
- Easy to run.
- Easy to troubleshoot.
- Very few moving parts.

---

## 3) Recommended minimal file structure

```text
yape_voucher_data_extraction/
├─ main.py                 # All processing logic (Google Sheets + Drive + OpenAI + write results)
├─ requirements.txt        # Python dependencies
├─ .env                    # Secrets and IDs (OpenAI key, spreadsheet ID, etc.)
├─ .env.example            # Template so you know what to fill
└─ README.md               # Beginner run instructions (manual + scheduled)
```

That is enough for a solid internal system.

---

## 4) Spreadsheet design

You already have a raw responses tab with these columns:
1. `timestamp`
2. `comprobante_yape` (Google Drive link)
3. `email`

Add one tab called exactly:
- `processed data`

Recommended columns in `processed data`:
1. `submission_id` (deterministic unique ID)
2. `raw_row_number`
3. `raw_timestamp`
4. `uploader_email`
5. `voucher_drive_link`
6. `voucher_drive_file_id`
7. `extracted_operation_number`
8. `extracted_amount`
9. `extracted_currency`
10. `extracted_date`
11. `extracted_time`
12. `extracted_phone_or_recipient` (if visible)
13. `openai_model`
14. `processed_at_utc`
15. `status` (`ok` or `error`)
16. `error_message` (blank when `ok`)
17. `raw_openai_json` (optional but useful for auditing)

---

## 5) Robust “already processed” logic (deterministic)

Use a deterministic `submission_id` created from raw fields that should uniquely identify one form response:

`submission_id = sha256(timestamp + '|' + comprobante_yape + '|' + email)`

### How it works
- On each run, script loads all existing `submission_id` values from `processed data` into a Python `set`.
- For each row in raw responses:
  - Build `submission_id`.
  - If ID is already in set → skip.
  - If not in set → process and append one new row in `processed data`.

### Why this is robust
- Deterministic: same input always gives same ID.
- Resistant to accidental duplicate runs.
- Does not depend on unstable row positions.

Note: if a cashier submits the exact same data twice, it will be treated as duplicate by design. If you want to keep both, include `raw_row_number` in the hash.

---

## 6) Exact integration behavior

## A) Google Sheets interaction
- Read all rows from raw responses tab (read-only behavior).
- Read existing `submission_id` values from `processed data`.
- Append new processed rows only to `processed data`.
- Never update or delete raw response rows.

Library approach:
- `gspread` + Google service account JSON credentials.

## B) Google Drive interaction
- From `comprobante_yape` link, extract Drive `file_id`.
- Use Drive API with same service account to download image bytes.
- If permissions fail, record row with `status=error` and message.

Important setup:
- Share the Form upload folder (or files) with service account email as Viewer.
- Share spreadsheet with service account email as Editor.

## C) OpenAI interaction
- Send image bytes (or base64) to a vision-capable OpenAI model.
- Prompt model to return strict JSON only.
- Parse JSON safely in Python.
- Map fields into processed data columns.
- If parse/model error, write `status=error` and error text.

Practical prompt style:
- “Extract operation number, amount, currency, transaction date/time, recipient/phone if visible. Return strict JSON with null for missing fields.”

---

## 7) Manual and scheduled runs

## Manual run (on-demand)
Command:
```bash
python main.py
```
This processes only new submissions and exits.

## Midnight daily run
Simplest reliable option for a beginner on Linux server/VM: **cron**.

Example cron entry (run every day at 00:00):
```cron
0 0 * * * cd /path/to/yape_voucher_data_extraction && /usr/bin/python3 main.py >> logs/cron.log 2>&1
```

Notes:
- Create `logs/` folder once.
- Keep absolute paths in cron.
- Script is idempotent because of `submission_id` dedupe.

If you are on Windows, use Task Scheduler with equivalent command.

---

## 8) Software and credentials you need

1. Python 3.10+.
2. Google Cloud project (one-time setup).
3. Enable APIs:
   - Google Sheets API
   - Google Drive API
4. Create service account and download JSON key.
5. Share spreadsheet + Drive upload folder with service account email.
6. OpenAI API key.
7. `.env` values:
   - `OPENAI_API_KEY`
   - `SPREADSHEET_ID`
   - `RAW_SHEET_NAME` (e.g., `Form Responses 1`)
   - `PROCESSED_SHEET_NAME=processed data`
   - `GOOGLE_SERVICE_ACCOUNT_FILE=service_account.json`
   - `OPENAI_MODEL` (e.g., `gpt-4.1-mini` or your chosen model)

---

## 9) Beginner setup steps (exact order)

1. Install Python.
2. Create project folder and files listed above.
3. Put `service_account.json` in project folder.
4. Create virtual environment and install dependencies.
5. Fill `.env` from `.env.example`.
6. In Google Sheets, create tab `processed data` with headers.
7. Share spreadsheet and Drive folder with service account email.
8. Run manual test: `python main.py`.
9. Check `processed data` tab for appended rows.
10. Add cron job for midnight.
11. Next day, verify only new rows were appended.

---

## 10) Risks, edge cases, and tradeoffs

1. **Drive permission errors**
   - Cause: service account lacks access.
   - Mitigation: share upload folder properly.

2. **Unreadable/blurred images**
   - Result: extraction errors or missing fields.
   - Mitigation: store `status=error`, keep raw JSON/error for review.

3. **Model mistakes (OCR inaccuracies)**
   - Tradeoff: simple system vs perfect accuracy.
   - Mitigation: keep confidence checks and optional manual audit process.

4. **Duplicate handling rule**
   - Current rule may collapse exact duplicate submissions.
   - Tradeoff: strong idempotency vs storing every duplicate.

5. **API cost growth**
   - Cost scales with number/size of images.
   - Mitigation: process only new rows, optionally resize images before sending.

6. **Schema changes in form/sheet**
   - If raw columns change names/order, script can fail.
   - Mitigation: validate required columns at start and fail with clear message.

7. **No retry queue (simple design)**
   - Failed rows are logged in processed tab, not retried automatically.
   - Mitigation: allow a later “reprocess errors” mode if needed.

---

## 11) Proposed phased execution plan

## Phase 1 — Foundation (Day 1)
- Create files and dependency setup.
- Configure Google service account and permissions.
- Create `processed data` tab headers.

## Phase 2 — Core processing (Day 1–2)
- Implement read raw rows.
- Implement deterministic `submission_id` dedupe.
- Implement Drive file download.
- Implement OpenAI extraction call.
- Implement append row to `processed data`.

## Phase 3 — Reliability basics (Day 2)
- Add clear error handling per row.
- Add status/error columns.
- Add basic logging to console + file.

## Phase 4 — Run operations (Day 2)
- Validate with manual run.
- Configure midnight cron job.
- Document simple runbook in README.

## Phase 5 — First week monitoring
- Check daily outputs.
- Track error rate.
- Adjust prompt or extracted fields if needed.

---

## 12) Proposed data flow (end-to-end)

1. Cashier submits Google Form with voucher photo.
2. Response appears in raw responses tab (`timestamp`, `comprobante_yape`, `email`).
3. At manual run or midnight cron:
   - script loads raw rows
   - script loads existing `submission_id` from `processed data`
4. For each unprocessed row:
   - generate `submission_id`
   - parse Drive file ID and download image
   - send image to OpenAI for extraction
   - append structured output row to `processed data`
5. Next run skips all previously processed IDs.

System remains append-only and traceable.

---

## 13) Final recommendation

For your current needs, this single-script + one processed tab design is the **best balance of simplicity and reliability**. It avoids overengineering while still giving deterministic dedupe, auditability, and scheduled automation.
