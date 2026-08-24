#!/usr/bin/env python3
"""
Orphaned Upload Recovery Tool
Recovers and uploads failed data from old camera_ids after camera configuration changes

Usage:
    python utils/recover_orphaned_uploads.py --device-id <device_id> [options]

Examples:
    # Dry run - show what would be recovered
    python utils/recover_orphaned_uploads.py --device-id 468a2b1d-ebaf-43ed-bf7e-a0104f54bf8a --dry-run

    # Recover from specific old camera
    python utils/recover_orphaned_uploads.py --device-id 468a2b1d-ebaf-43ed-bf7e-a0104f54bf8a --old-camera-id OLD_CAMERA_ABC123

    # Recover all orphaned data
    python utils/recover_orphaned_uploads.py --device-id 468a2b1d-ebaf-43ed-bf7e-a0104f54bf8a --all

    # Rewrite camera_id to current camera
    python utils/recover_orphaned_uploads.py --device-id 468a2b1d-ebaf-43ed-bf7e-a0104f54bf8a --rewrite-camera-id NEW_CAMERA_XYZ789

    # Recover an exact date range, paced at 1 batch every 5s instead of all at once
    python utils/recover_orphaned_uploads.py --device-id 468a2b1d-ebaf-43ed-bf7e-a0104f54bf8a \\
        --old-camera-id CAMERA_ID --start-date 2026-08-10 --end-date 2026-08-12 \\
        --send-interval 5 --batch-size 20

Copyright (c) 2025 Vizo Authors. All Rights Reserved.
Author: Nguyen Cong Quy <quync@vizo.co.jp>
"""

import os
import sys
import json
import time
import argparse
import duckdb
import requests
import urllib3
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import List, Dict, Tuple, Optional
from collections import defaultdict

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


class OrphanedUploadRecovery:
    """Tool to recover and upload failed data from old camera_ids"""

    def __init__(self,
                 device_id: str,
                 config_path: str = "config/config.json",
                 zone_data_path: str = "config/zone-data.json",
                 output_base: str = "output"):

        self.device_id = device_id
        self.config_path = config_path
        self.zone_data_path = zone_data_path
        self.output_base = output_base

        # Load configuration
        self.load_config()

        # Load zone-data to get current camera_ids
        self.current_camera_ids = self.load_current_cameras()

        # Statistics
        self.stats = {
            'cameras_scanned': 0,
            'databases_checked': 0,
            'failed_records_found': 0,
            'records_uploaded': 0,
            'records_skipped_empty': 0,
            'upload_failures': 0,
            'camera_details': defaultdict(lambda: {'days': 0, 'failed_records': 0, 'uploaded': 0})
        }

    def load_config(self):
        """Load upload configuration"""
        with open(self.config_path, 'r') as f:
            config = json.load(f)

        self.api_url = config.get('base_url', '')
        self.heartbeat_url = config.get('heartbeat_url', '')
        self.glicense = config.get('glicense', '')

        upload_config = config.get('upload', {})
        self.batch_size = upload_config.get('batch_size', 20)
        self.timeout = upload_config.get('timeout', 60)
        self.max_queue_retries = upload_config.get('max_queue_retries', 5)

        print(f"✅ Loaded config from {self.config_path}")
        print(f"   API URL: {self.api_url}")

    def load_current_cameras(self) -> set:
        """Load current camera_ids from zone-data.json"""
        with open(self.zone_data_path, 'r') as f:
            zone_data = json.load(f)

        current_cameras = set()
        for device in zone_data.get('devices', []):
            if device.get('device_id') == self.device_id:
                for camera in device.get('cameras', []):
                    current_cameras.add(camera.get('camera_id'))

        print(f"✅ Current cameras in zone-data.json ({len(current_cameras)}):")
        for cam_id in current_cameras:
            print(f"   - {cam_id}")

        return current_cameras

    @staticmethod
    def _has_column(con, column_name: str) -> bool:
        """
        retry_count and send_state are added lazily by camera_data_uploader.py
        the first time the uploader touches a given day's database (ALTER TABLE
        ... ADD COLUMN). A database the uploader has never processed -- e.g.
        upload was disabled in config.json the whole time -- won't have them yet.
        """
        result = con.execute("""
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_name = 'tracking_data' AND column_name = ?
        """, [column_name]).fetchone()
        return bool(result and result[0] > 0)

    @staticmethod
    def _is_empty_record(zone_data: dict) -> bool:
        """
        True if this record has zero crossings and zero occupancy everywhere:
        a leftover from before the should_save fix in counting_service.py
        (older databases still have these), or just a genuinely uneventful
        snapshot. Either way there is nothing to send.
        """
        flow = zone_data.get('flow', {})
        if flow.get('num_in', 0) or flow.get('num_out', 0):
            return False

        zones = zone_data.get('zone', [])
        if isinstance(zones, dict):
            zone_entries = list(zones.values())
        else:
            zone_entries = [zdata for entry in zones for zdata in entry.values()]

        for zdata in zone_entries:
            if zdata.get('num_in', 0) or zdata.get('num_out', 0) or zdata.get('num_current', 0):
                return False

        return True

    @staticmethod
    def _transform_for_upload(zone_data: dict, record_id: str) -> Optional[dict]:
        """
        Mirrors camera_data_uploader.py's transform_data() exactly -- this
        script used to build its own payload straight from the raw DB row,
        which still carries `_cumulative_snapshot` (an internal bookkeeping
        field, never meant to leave the local DB) and can still have `zone`
        in the old dict format. The realtime uploader always strips/converts
        these before sending; records that skip this step are NOT in the
        format the server's dashboard actually expects, even though the API
        itself returns status=0 for them (accepted at the HTTP layer, but
        not necessarily surfaced correctly downstream).

        Returns the transformed dict, or None if the record's zone array is
        empty (startup-period record -- transform_data() would return []).
        """
        transformed = zone_data.copy()
        transformed['id'] = record_id

        if 'zone' in transformed and isinstance(transformed['zone'], dict):
            transformed['zone'] = [{k: v} for k, v in transformed['zone'].items()]

        transformed.pop('_cumulative_snapshot', None)

        if 'zone' in transformed and isinstance(transformed['zone'], list) and not transformed['zone']:
            return None

        return transformed

    def scan_orphaned_cameras(self, lookback_days: int = 30,
                             dates: Optional[List[date]] = None) -> Dict[str, List[Tuple[str, int]]]:
        """
        Scan for orphaned camera databases with failed uploads

        Args:
            lookback_days: How many days back to check (ignored if `dates` is given)
            dates: Explicit list of calendar dates to scan. Overrides
                `lookback_days` when provided (used for --start-date/--end-date).

        Returns:
            Dict[camera_id, List[(date, failed_count)]]
        """
        if dates is None:
            today = datetime.now().date()
            dates = [today - timedelta(days=d) for d in range(1, lookback_days + 1)]

        print(f"\n{'='*80}")
        print(f"SCANNING FOR ORPHANED CAMERAS")
        print(f"{'='*80}")
        print(f"Device ID: {self.device_id}")
        if len(dates) == 1:
            print(f"Date scanned: {dates[0]}")
        else:
            print(f"Date range: {min(dates)} to {max(dates)} ({len(dates)} day(s))")
        print(f"Current cameras: {len(self.current_camera_ids)}")
        print()

        orphaned_data = defaultdict(list)

        for check_date in dates:
            device_dir = (
                Path(self.output_base) / "database" /
                f"{check_date.year:04d}" /
                f"{check_date.month:02d}" /
                f"{check_date.day:02d}" /
                self.device_id
            )

            if not device_dir.exists():
                continue

            # Scan all camera directories
            for camera_dir in device_dir.iterdir():
                if not camera_dir.is_dir():
                    continue

                camera_id = camera_dir.name
                db_path = camera_dir / f"{camera_id}.db"

                if not db_path.exists():
                    continue

                self.stats['databases_checked'] += 1

                # Check for failed records
                try:
                    con = duckdb.connect(str(db_path), read_only=True)

                    has_retry_count = self._has_column(con, 'retry_count')
                    has_send_state = self._has_column(con, 'send_state')

                    # Two independent buckets:
                    #  - still pending, hasn't exhausted retries yet (status=FALSE)
                    #  - gave up after max_queue_retries (send_state='failed'), only
                    #    meaningful once the uploader has started distinguishing it
                    pending_clause = "(status = FALSE OR status = 0)"
                    if has_retry_count:
                        pending_clause += f" AND COALESCE(retry_count, 0) < {self.max_queue_retries}"

                    where_clause = pending_clause
                    if has_send_state:
                        where_clause = f"({pending_clause}) OR send_state = 'failed'"

                    result = con.execute(
                        f"SELECT COUNT(*) FROM tracking_data WHERE {where_clause}"
                    ).fetchone()

                    failed_count = result[0] if result else 0

                    if failed_count > 0:
                        orphaned_data[camera_id].append((check_date.strftime('%Y-%m-%d'), failed_count))
                        self.stats['failed_records_found'] += failed_count
                        self.stats['camera_details'][camera_id]['days'] += 1
                        self.stats['camera_details'][camera_id]['failed_records'] += failed_count

                    con.close()

                except Exception as e:
                    print(f"❌ Error checking {db_path}: {e}")

        self.stats['cameras_scanned'] = len(orphaned_data)

        return orphaned_data

    def print_scan_results(self, orphaned_data: Dict[str, List[Tuple[str, int]]]):
        """Print scan results in organized format"""
        print(f"\n{'='*80}")
        print(f"SCAN RESULTS")
        print(f"{'='*80}")
        print(f"Databases checked: {self.stats['databases_checked']}")
        print(f"Cameras with failed uploads: {self.stats['cameras_scanned']}")
        print(f"Total failed records: {self.stats['failed_records_found']}")
        print()

        if not orphaned_data:
            print("✅ No orphaned data found!")
            return

        # Categorize cameras
        orphaned_cameras = {}
        current_cameras = {}

        for camera_id, days_data in orphaned_data.items():
            if camera_id in self.current_camera_ids:
                current_cameras[camera_id] = days_data
            else:
                orphaned_cameras[camera_id] = days_data

        # Print orphaned cameras (OLD camera_ids)
        if orphaned_cameras:
            print(f"🔍 ORPHANED CAMERAS (not in zone-data.json):")
            print(f"{'─'*80}")
            for camera_id, days_data in sorted(orphaned_cameras.items()):
                total_failed = sum(count for _, count in days_data)
                print(f"\n📷 Camera: {camera_id}")
                print(f"   Status: ⚠️  ORPHANED (removed from zone-data.json)")
                print(f"   Days with failures: {len(days_data)}")
                print(f"   Total failed records: {total_failed}")
                print(f"   Date range: {days_data[-1][0]} to {days_data[0][0]}")
                print(f"   Details:")
                for date, count in sorted(days_data, reverse=True)[:5]:
                    print(f"     - {date}: {count} records")
                if len(days_data) > 5:
                    print(f"     ... and {len(days_data) - 5} more days")

        # Print current cameras with failures
        if current_cameras:
            print(f"\n{'─'*80}")
            print(f"📸 CURRENT CAMERAS (still in zone-data.json):")
            print(f"{'─'*80}")
            for camera_id, days_data in sorted(current_cameras.items()):
                total_failed = sum(count for _, count in days_data)
                print(f"\n📷 Camera: {camera_id}")
                print(f"   Status: ✅ Current (automatic upload will handle this)")
                print(f"   Days with failures: {len(days_data)}")
                print(f"   Total failed records: {total_failed}")

    def recover_camera(self, camera_id: str, dry_run: bool = True,
                      rewrite_camera_id: Optional[str] = None,
                      lookback_days: int = 30,
                      dates: Optional[List[date]] = None,
                      send_interval: float = 0.0,
                      max_batches_per_day: Optional[int] = None,
                      empty_only: bool = False) -> int:
        """
        Recover failed uploads from specific camera_id

        Args:
            camera_id: Camera ID to recover
            dry_run: If True, only simulate (don't actually upload)
            rewrite_camera_id: If provided, rewrite camera_id in zone_data before upload
            lookback_days: How many days back to check (ignored if `dates` is given)
            dates: Explicit list of calendar dates to process, oldest first.
                Overrides `lookback_days` when provided (used for --start-date/--end-date).
            send_interval: Seconds to sleep after each live batch send (paces the
                upload instead of firing every matching batch back-to-back). No
                effect in dry-run mode or when empty_only=True (no sends happen).
            max_batches_per_day: Safety cap on how many batches to send per
                database in one run. None = no cap (loop until nothing is left
                to send, bounded naturally by max_queue_retries).
            empty_only: Never sends anything -- only clears the all-zero
                backlog (see counting_service.py's should_save fix). Real
                records are left completely untouched for a later, separately
                confirmed pass. Use this to safely bulk-clean a large backlog
                before deciding whether/how to send the real records in it.

        Returns:
            Number of records successfully uploaded (always 0 when empty_only=True)
        """
        print(f"\n{'='*80}")
        print(f"RECOVERING CAMERA: {camera_id}")
        print(f"{'='*80}")
        if empty_only:
            print(f"Mode: EMPTY-ONLY CLEANUP (no records will be sent to the API)")
        else:
            print(f"Mode: {'DRY RUN (simulation)' if dry_run else 'LIVE (will upload to server)'}")
        if rewrite_camera_id:
            print(f"Camera ID rewrite: {camera_id} → {rewrite_camera_id}")
        if send_interval > 0 and not empty_only:
            print(f"Send pacing: {send_interval}s between batches of {self.batch_size} records")
        print()

        uploaded_count = 0
        empty_marked_before = self.stats['records_skipped_empty']

        if dates is None:
            today = datetime.now().date()
            dates = [today - timedelta(days=d) for d in range(1, lookback_days + 1)]

        for check_date in dates:
            db_path = (
                Path(self.output_base) / "database" /
                f"{check_date.year:04d}" /
                f"{check_date.month:02d}" /
                f"{check_date.day:02d}" /
                self.device_id / camera_id / f"{camera_id}.db"
            )

            if not db_path.exists():
                continue

            # Process this database
            day_uploaded = self.process_database(
                db_path, camera_id, check_date.strftime('%Y-%m-%d'),
                dry_run, rewrite_camera_id,
                send_interval=send_interval,
                max_batches=max_batches_per_day,
                empty_only=empty_only,
            )

            uploaded_count += day_uploaded

        print(f"\n{'─'*80}")
        print(f"✅ Recovery complete for {camera_id}")
        if empty_only:
            print(f"   Empty records marked resolved: "
                  f"{self.stats['records_skipped_empty'] - empty_marked_before}")
        else:
            print(f"   Records uploaded: {uploaded_count}")
        print(f"{'='*80}")

        return uploaded_count

    def process_database(self, db_path: Path, camera_id: str, date_label: str,
                        dry_run: bool, rewrite_camera_id: Optional[str],
                        send_interval: float = 0.0,
                        max_batches: Optional[int] = None,
                        empty_only: bool = False) -> int:
        """
        Process single database file.

        Dry-run always does a single preview batch (status never changes, so
        looping would just re-read the same rows forever). Live mode loops
        with an `id`-based cursor: each batch only looks at rows with
        id > the highest id seen so far, so a record left untouched (empty_only
        skipping a real record, or a send failing) is visited exactly once per
        run instead of being re-fetched by every subsequent batch -- without
        the cursor, untouched records pile up at the front of an
        ORDER BY ... LIMIT query and each batch wastes more and more of its
        LIMIT re-scanning them instead of reaching new rows.

        empty_only=True never sends anything: it loops purely to clear the
        empty backlog, one id-range slice at a time, until it runs off the
        end of the day's ids.
        """
        total_uploaded = 0
        total_empty_marked = 0
        batch_num = 0
        last_id = 0

        while True:
            batch_num += 1
            if max_batches is not None and batch_num > max_batches:
                print(f"   ⏸️  Reached --max-batches-per-day ({max_batches}) for {date_label}, "
                      f"stopping this day (remaining records will be picked up on the next run)")
                break

            try:
                con = duckdb.connect(str(db_path))
                self._ensure_recovery_columns(con)

                # Pending bucket: hasn't exhausted retries yet (status still FALSE).
                # id > last_id guarantees forward-only progress through the table
                # regardless of which rows in the previous batch got resolved.
                query = f"""
                    SELECT id, zone_data, COALESCE(retry_count, 0) as retry_count
                    FROM tracking_data
                    WHERE (status = FALSE OR status = 0)
                      AND COALESCE(retry_count, 0) < {self.max_queue_retries}
                      AND id > {last_id}
                    ORDER BY id ASC
                    LIMIT {self.batch_size}
                """

                rows = con.execute(query).fetchall()

                if not rows:
                    con.close()
                    break

                last_id = max(r[0] for r in rows)

                uploaded, empty_marked = self._send_batch(
                    con, camera_id, rows, dry_run, rewrite_camera_id, empty_only=empty_only,
                    date_label=date_label,
                )

                con.commit()
                con.close()

                total_uploaded += uploaded
                total_empty_marked += empty_marked

                if empty_only:
                    print(f"   {date_label} batch {batch_num}: {len(rows)} scanned, "
                          f"{empty_marked} empty {'would be ' if dry_run else ''}marked resolved "
                          f"(running total this day: {total_empty_marked})")
                    # No early-exit on empty_marked == 0: real and empty records
                    # are interleaved by time throughout the day, not grouped,
                    # so a batch with no empties doesn't mean there are none
                    # left later in the id range. The id > last_id cursor above
                    # already guarantees this loop terminates (runs off the end
                    # of the day's ids) without re-scanning untouched rows.
                else:
                    print(f"\n📅 Processing {date_label} batch {batch_num} ({len(rows)} pending records)")

            except Exception as e:
                print(f"❌ Error processing {db_path}: {e}")
                break

            if dry_run:
                # Status never changes in dry-run, so the query above would
                # return the exact same rows forever -- one preview batch only.
                break

            if send_interval > 0 and not empty_only:
                print(f"   ⏳ Waiting {send_interval}s before next batch...")
                time.sleep(send_interval)

        if empty_only:
            # Nothing left to do here: send_state='failed' records are real,
            # already-attempted data, never empty -- empty_only has nothing
            # to clear in that bucket.
            return total_uploaded

        # Second, separate pass: records the automatic uploader gave up on
        # (send_state='failed', already status=TRUE so they'd never surface
        # in the pending query above). Bounded to ONE pass -- unlike the
        # pending bucket, a record that fails here again stays 'failed'
        # forever, so re-querying it would loop infinitely.
        try:
            con = duckdb.connect(str(db_path))
            self._ensure_recovery_columns(con)

            if self._has_column(con, 'send_state'):
                rows = con.execute(f"""
                    SELECT id, zone_data, COALESCE(retry_count, 0) as retry_count
                    FROM tracking_data
                    WHERE send_state = 'failed'
                    ORDER BY tracking_time ASC
                    LIMIT {self.batch_size}
                """).fetchall()

                if rows:
                    print(f"\n📅 Processing {date_label}: {len(rows)} previously-abandoned "
                          f"record(s) (send_state='failed'), one retry each")
                    batch_uploaded, _ = self._send_batch(con, camera_id, rows, dry_run, rewrite_camera_id,
                                                          date_label=date_label)
                    total_uploaded += batch_uploaded
                    con.commit()

            con.close()
        except Exception as e:
            print(f"❌ Error processing abandoned records in {db_path}: {e}")

        return total_uploaded

    def _ensure_recovery_columns(self, con) -> None:
        """
        Mirror camera_data_uploader.py's lazy migrations: a database the
        uploader has never touched won't have these columns yet.
        """
        if not self._has_column(con, 'retry_count'):
            con.execute("ALTER TABLE tracking_data ADD COLUMN retry_count INTEGER DEFAULT 0")
            con.commit()
        if not self._has_column(con, 'send_state'):
            con.execute("ALTER TABLE tracking_data ADD COLUMN send_state VARCHAR DEFAULT NULL")
            con.commit()

    def _send_batch(self, con, camera_id: str, rows: List[Tuple],
                   dry_run: bool, rewrite_camera_id: Optional[str],
                   empty_only: bool = False, date_label: str = "") -> Tuple[int, int]:
        """
        Send (or preview) one batch of (id, zone_data, retry_count) rows.
        Records with all-zero in/out/current are skipped without sending --
        older databases still have these from before counting_service.py's
        should_save fix, and there is nothing useful to deliver.

        date_label ("YYYY-MM-DD") is folded into the id sent to the server.
        Each day's database restarts its own `id` at 1 (fresh AUTOINCREMENT
        per file), so record id=1 exists in EVERY day's DB -- sending the
        bare local id across multiple days puts the same "id" on many
        different real records. If the server keys storage by id (looked
        like it does: bulk-sending 15 days of un-prefixed ids left almost
        nothing visible on the dashboard, consistent with later ids
        overwriting earlier ones), this collision silently destroys all but
        the last record for each id value. The realtime uploader never hits
        this because it only ever sends the current day's ids in order.

        empty_only=True processes ONLY the empty records and leaves every
        real record completely untouched (no send, no DB write) -- used to
        bulk-clear known-empty backlog without risking a live send of real
        historical data before that is separately confirmed.

        Returns (uploaded_count, empty_marked_count). dry-run counts
        previewed rows as "uploaded".

        Real (non-empty) records are sent as ONE combined HTTP request per
        call -- the API accepts a JSON array of records in a single POST
        (confirmed: a 200-record array round-tripped in <1s during testing).
        Sending them one-request-per-record instead (the original
        implementation) was ~10x slower in practice: ~409K records at
        ~0.4s/request is ~45 hours, not the few hours a combined payload
        takes. Trade-off: the API returns one status for the whole request,
        so on failure none of the records in this call can be individually
        identified as the cause -- all stay pending and are retried together
        next run, rather than isolating a single bad record.
        """
        uploaded = 0
        empty_marked = 0
        has_send_state = self._has_column(con, 'send_state')
        to_send: List[Tuple[int, int, Dict]] = []  # (record_id, retry_count, zone_data)

        for row in rows:
            record_id, zone_data, retry_count = row

            if isinstance(zone_data, str):
                zone_data = json.loads(zone_data)

            if self._is_empty_record(zone_data):
                if dry_run:
                    empty_marked += 1
                else:
                    set_clause = "status = TRUE, retry_count = ?"
                    params = [retry_count + 1]
                    if has_send_state:
                        set_clause += ", send_state = 'skipped_empty'"
                    params.append(record_id)
                    con.execute(f"UPDATE tracking_data SET {set_clause} WHERE id = ?", params)
                    self.stats['records_skipped_empty'] += 1
                    empty_marked += 1
                continue

            if empty_only:
                # Real record, but this run is only clearing the empty
                # backlog -- leave it exactly as found for a later pass.
                continue

            # Rewrite camera_id if requested
            if rewrite_camera_id:
                zone_data['camera_id'] = rewrite_camera_id

            unique_id = f"{date_label.replace('-', '')}-{record_id}" if date_label else str(record_id)
            transformed = self._transform_for_upload(zone_data, unique_id)
            if transformed is None:
                # Same as camera_data_uploader.py's transform_data(): empty
                # zone array (startup-period record) -- nothing to send, but
                # this is real (not all-zero) data so don't silently drop it
                # from the pending pool either. Leave it as-is; it'll be
                # re-checked on a future run.
                continue
            to_send.append((record_id, retry_count, transformed))

        if rewrite_camera_id and to_send:
            print(f"   📝 Rewriting camera_id -> {rewrite_camera_id} for {len(to_send)} record(s)")

        if to_send:
            if dry_run:
                print(f"   🔍 Would upload {len(to_send)} record(s) in one combined request (dry run)")
                uploaded += len(to_send)
            else:
                payload = [zd for (_rid, _rc, zd) in to_send]
                id_range = f"{to_send[0][0]}-{to_send[-1][0]}"
                success = self.send_data(id_range, payload)

                if success:
                    for record_id, retry_count, _zd in to_send:
                        set_clause = "status = TRUE, retry_count = ?"
                        if has_send_state:
                            set_clause += ", send_state = 'sent'"
                        con.execute(f"UPDATE tracking_data SET {set_clause} WHERE id = ?",
                                   [retry_count + 1, record_id])
                    print(f"   ✅ Batch of {len(to_send)} record(s) (id {id_range}) uploaded successfully")
                    uploaded += len(to_send)
                    self.stats['records_uploaded'] += len(to_send)
                    self.stats['camera_details'][camera_id]['uploaded'] += len(to_send)
                else:
                    print(f"   ❌ Batch of {len(to_send)} record(s) (id {id_range}) failed to upload")
                    self.stats['upload_failures'] += len(to_send)
                    # No status/retry_count/send_state change, matching the
                    # automatic uploader: only the AUTOMATIC path (which owns
                    # the retry-count lifecycle) decides a record is
                    # permanently abandoned. A manual attempt failing again
                    # just means "still not deliverable right now" -- stays
                    # exactly as found and is retried whole next run.

        if empty_marked > 0:
            print(f"   ⏭️  {empty_marked} empty record(s) in this batch "
                  f"{'would be' if dry_run else 'were'} skipped and marked resolved")

        return uploaded, empty_marked

    def send_data(self, record_id: str, payload: List[Dict]) -> bool:
        """Send data to API server"""
        try:
            url = f"{self.api_url}?key={self.glicense}"

            res = requests.post(
                url,
                json=payload,
                timeout=self.timeout,
                verify=False
            )

            if res.status_code == 200:
                response = res.json()
                if response.get('status') == 0:
                    return True

            return False

        except Exception as e:
            print(f"   Upload error: {e}")
            return False

    def print_final_stats(self):
        """Print final statistics"""
        print(f"\n{'='*80}")
        print(f"FINAL STATISTICS")
        print(f"{'='*80}")
        print(f"Cameras scanned: {self.stats['cameras_scanned']}")
        print(f"Databases checked: {self.stats['databases_checked']}")
        print(f"Failed records found: {self.stats['failed_records_found']}")
        print(f"Records uploaded: {self.stats['records_uploaded']}")
        print(f"Upload failures: {self.stats['upload_failures']}")

        if self.stats['camera_details']:
            print(f"\nPer-camera breakdown:")
            for camera_id, details in self.stats['camera_details'].items():
                print(f"  {camera_id}:")
                print(f"    Days: {details['days']}")
                print(f"    Failed records: {details['failed_records']}")
                print(f"    Uploaded: {details['uploaded']}")


def main():
    parser = argparse.ArgumentParser(
        description='Recover orphaned upload data from old camera_ids',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    parser.add_argument('--device-id', required=True,
                       help='Device ID to recover data for')

    parser.add_argument('--old-camera-id',
                       help='Specific old camera_id to recover (otherwise scans all)')

    parser.add_argument('--rewrite-camera-id',
                       help='Rewrite camera_id in zone_data to this value before upload')

    parser.add_argument('--all', action='store_true',
                       help='Recover all orphaned cameras (non-interactive)')

    parser.add_argument('--dry-run', action='store_true',
                       help='Simulate recovery without actually uploading')

    parser.add_argument('--lookback-days', type=int, default=30,
                       help='How many days back to check (default: 30). '
                            'Ignored if --start-date/--end-date are given')

    parser.add_argument('--start-date', type=str,
                       help='Exact range start (YYYY-MM-DD, inclusive). '
                            'Requires --end-date; overrides --lookback-days')

    parser.add_argument('--end-date', type=str,
                       help='Exact range end (YYYY-MM-DD, inclusive). Requires --start-date')

    parser.add_argument('--send-interval', type=float, default=0.0,
                       help='Seconds to sleep between each live batch send (default: 0 = '
                            'no pacing, send as fast as possible). No effect with --dry-run')

    parser.add_argument('--batch-size', type=int,
                       help='Records per send. Default: upload.batch_size from config.json')

    parser.add_argument('--max-batches-per-day', type=int,
                       help='Cap batches sent per database per run (default: unbounded, '
                            'naturally limited by max_queue_retries)')

    parser.add_argument('--empty-only', action='store_true',
                       help='Never send anything to the API -- only mark all-zero '
                            '(in/out/current all 0) records resolved. Real records are '
                            'left completely untouched. Safe to run on a large backlog '
                            'before separately deciding whether/how to send the rest.')

    parser.add_argument('--config', default='config/config.json',
                       help='Path to config.json')

    parser.add_argument('--zone-data', default='config/zone-data.json',
                       help='Path to zone-data.json')

    parser.add_argument('--output', default='output',
                       help='Output base directory')

    args = parser.parse_args()

    if bool(args.start_date) != bool(args.end_date):
        print("❌ --start-date and --end-date must be given together", file=sys.stderr)
        return 1

    dates = None
    if args.start_date and args.end_date:
        try:
            start = datetime.strptime(args.start_date, '%Y-%m-%d').date()
            end = datetime.strptime(args.end_date, '%Y-%m-%d').date()
        except ValueError as e:
            print(f"❌ Invalid date: {e}", file=sys.stderr)
            return 1
        if start > end:
            print("❌ --start-date must not be after --end-date", file=sys.stderr)
            return 1
        dates = [start + timedelta(days=d) for d in range((end - start).days + 1)]

    # Initialize recovery tool
    recovery = OrphanedUploadRecovery(
        device_id=args.device_id,
        config_path=args.config,
        zone_data_path=args.zone_data,
        output_base=args.output
    )

    if args.batch_size is not None:
        recovery.batch_size = args.batch_size

    # Scan for orphaned data
    orphaned_data = recovery.scan_orphaned_cameras(lookback_days=args.lookback_days, dates=dates)
    recovery.print_scan_results(orphaned_data)

    # If no orphaned data, exit
    if not orphaned_data:
        return 0

    # Determine what to recover
    if args.old_camera_id:
        # Recover specific camera
        if args.old_camera_id not in orphaned_data:
            print(f"\n❌ Camera {args.old_camera_id} not found in orphaned data")
            return 1

        cameras_to_recover = [args.old_camera_id]

    elif args.all:
        # Recover all orphaned cameras (not in current zone-data.json)
        cameras_to_recover = [
            cam_id for cam_id in orphaned_data.keys()
            if cam_id not in recovery.current_camera_ids
        ]

        if not cameras_to_recover:
            print(f"\n✅ No orphaned cameras to recover (all are current)")
            return 0

    else:
        # Interactive mode
        print(f"\n{'='*80}")
        print("SELECT CAMERAS TO RECOVER")
        print(f"{'='*80}")

        orphaned_only = [
            cam_id for cam_id in orphaned_data.keys()
            if cam_id not in recovery.current_camera_ids
        ]

        if not orphaned_only:
            print("✅ No orphaned cameras found (all failed uploads are from current cameras)")
            print("   Automatic historical upload will handle these.")
            return 0

        print("Orphaned cameras:")
        for i, cam_id in enumerate(orphaned_only, 1):
            days_data = orphaned_data[cam_id]
            total_failed = sum(count for _, count in days_data)
            print(f"  {i}. {cam_id} ({total_failed} failed records across {len(days_data)} days)")

        print(f"\nOptions:")
        print(f"  - Enter camera numbers (comma-separated): e.g., 1,3")
        print(f"  - Enter 'all' to recover all orphaned cameras")
        print(f"  - Enter 'q' to quit")

        choice = input(f"\nYour choice: ").strip().lower()

        if choice == 'q':
            return 0
        elif choice == 'all':
            cameras_to_recover = orphaned_only
        else:
            try:
                indices = [int(x.strip()) - 1 for x in choice.split(',')]
                cameras_to_recover = [orphaned_only[i] for i in indices if 0 <= i < len(orphaned_only)]
            except (ValueError, IndexError):
                print("❌ Invalid selection")
                return 1

    # Confirm before proceeding
    if not args.dry_run and args.empty_only:
        print(f"\n{'='*80}")
        print("EMPTY-ONLY CLEANUP -- nothing will be sent to the API")
        print(f"{'='*80}")
        print(f"About to scan and mark all-zero records resolved for {len(cameras_to_recover)} "
              f"camera(s) (real records left untouched):")
        for cam_id in cameras_to_recover:
            total_failed = sum(count for _, count in orphaned_data[cam_id])
            print(f"  - {cam_id}: up to {total_failed} records to scan")

        confirm = input(f"\nProceed? (yes/no): ").strip().lower()
        if confirm != 'yes':
            print("Cancelled.")
            return 0
    elif not args.dry_run:
        print(f"\n{'='*80}")
        print("⚠️  WARNING: LIVE MODE")
        print(f"{'='*80}")
        print(f"About to upload data from {len(cameras_to_recover)} camera(s) to server:")
        for cam_id in cameras_to_recover:
            total_failed = sum(count for _, count in orphaned_data[cam_id])
            print(f"  - {cam_id}: {total_failed} records")

        if args.rewrite_camera_id:
            print(f"\n⚠️  Camera IDs will be rewritten to: {args.rewrite_camera_id}")

        confirm = input(f"\nProceed? (yes/no): ").strip().lower()
        if confirm != 'yes':
            print("Cancelled.")
            return 0

    # Recover selected cameras
    for camera_id in cameras_to_recover:
        recovery.recover_camera(
            camera_id=camera_id,
            dry_run=args.dry_run,
            rewrite_camera_id=args.rewrite_camera_id,
            lookback_days=args.lookback_days,
            dates=dates,
            send_interval=args.send_interval,
            max_batches_per_day=args.max_batches_per_day,
            empty_only=args.empty_only,
        )

    # Print final stats
    recovery.print_final_stats()

    print(f"\n{'='*80}")
    print("✅ RECOVERY COMPLETE")
    print(f"{'='*80}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
