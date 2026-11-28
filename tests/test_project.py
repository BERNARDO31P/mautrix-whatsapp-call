"""Offline regression fixtures. No Matrix, WhatsApp or Docker service is used."""
import hashlib
import json
import shutil
import stat
import struct
from zipfile import ZipFile, ZipInfo
import os
from pathlib import Path
import runpy
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest import mock

ROOT=Path(__file__).resolve().parents[1]
DEPLOY=ROOT/"deployment/mautrix-whatsapp-call"
sys.dont_write_bytecode=True
sys.path[:0]=[str(DEPLOY),str(DEPLOY/"worker")]
import yaml
import config_files
import session_files
configure=runpy.run_path(str(DEPLOY/"configure-bridge.py"))["update_config"]
merge=runpy.run_path(str(DEPLOY/"merge-synapse.py"))["merge"]
reconcile=runpy.run_path(str(DEPLOY/"reconcile-credentials.py"))["reconcile"]
automatic=runpy.run_path(str(DEPLOY/"worker/auto-provision-whatsapp-ghosts.py"))

def fixture_elf_header():
    header = bytearray(64)
    header[:7] = b"\x7fELF\x02\x01\x01"
    struct.pack_into("<HHI", header, 16, 2, 62, 1)
    struct.pack_into("<H", header, 52, 64)
    return bytes(header)

def fixture_call_bindings(model, root):
    root = Path(root)
    rtc = root / "mautrix-whatsapp-call"
    def bind(source, target, read_only=False):
        return {"type": "bind", "source": str(source), "target": target,
                "read_only": read_only}
    state = bind(rtc / "worker-state", "/var/lib/mautrix-whatsapp-call")
    phrase = bind(rtc / "generated/matrixrtc_store_passphrase",
                  "/run/secrets/matrixrtc_store_passphrase", True)
    services = model["services"]
    services["mautrix-whatsapp-call-worker"]["volumes"] += [dict(state), dict(phrase)]
    services["mautrix-whatsapp-call-provisioner"] = {"volumes": [dict(state), dict(phrase),
        bind(root / "mautrix-whatsapp/registration.yaml", "/run/secrets/mautrix-whatsapp-registration.yaml", True),
        bind(root / "mautrix-whatsapp/config.yaml", "/run/secrets/mautrix-whatsapp-config.yaml", True)]}
    services.setdefault("lk-jwt-service", {})["volumes"] = [
        bind(rtc / "generated/livekit_secret", "/run/secrets/livekit_secret", True),
        bind(rtc / "generated/matrixrtc-registration.yaml", "/run/secrets/matrixrtc-registration.yaml", True)]
    services["livekit"] = {"volumes": [bind(rtc / "generated/livekit.yaml", "/etc/livekit.yaml", True)]}
    for service, directory, image in (
            ("mautrix-whatsapp", "bridge", "mautrix-whatsapp-call-bridge:local"),
            ("mautrix-whatsapp-call-worker", "worker", "mautrix-whatsapp-call-worker:local"),
            ("mautrix-whatsapp-call-provisioner", "worker", "mautrix-whatsapp-call-worker:local")):
        services[service].update(image=image, build={"context": str(rtc / directory),
                                                      "dockerfile": "Dockerfile"})

    overlay = yaml.safe_load((DEPLOY / "compose.yml").read_text())["services"]
    for service in ("mautrix-whatsapp-call-worker", "mautrix-whatsapp-call-provisioner"):
        original = overlay[service]
        services[service]["command"] = original["command"]
        services[service]["environment"] = {**original["environment"],
                                             "MATRIXRTC_SERVER_NAME": "example.org"}
    services["mautrix-whatsapp-call-worker"]["healthcheck"] = overlay["mautrix-whatsapp-call-worker"]["healthcheck"]


class ProjectRegression(unittest.TestCase):
    def test_delayed_membership_inspector_distinguishes_missing_and_empty_journal(self):
        script = DEPLOY / "inspect-delayed-membership.py"
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            command = [sys.executable, "-B", str(script), str(root)]
            result = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(result.returncode, 2)
            self.assertIn("Cannot inspect delayed membership journal", result.stderr)
            self.assertFalse((root / "delayed-membership-operations").exists())
            journal = root / "delayed-membership-operations"
            journal.mkdir(mode=0o700)
            result = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(result.stdout), {"records": [], "errors": [], "automatic_settlement": False})
            self.assertEqual(list(journal.iterdir()), [])
            strict = subprocess.run(command + ["--require-empty"], capture_output=True, text=True)
            self.assertEqual(strict.returncode, 0, strict.stderr)
            self.assertEqual(json.loads(strict.stdout), json.loads(result.stdout))

    def test_delayed_membership_inspector_preserves_known_unknown_and_interrupted_records(self):
        script = DEPLOY / "inspect-delayed-membership.py"
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            journal = root / "delayed-membership-operations"
            journal.mkdir(mode=0o700)
            def record(token, delay_id, suffix=""):
                path = journal / (token + suffix)
                path.write_text(json.dumps({"operation": {"token": token, "room_id": "!room:example.org", "slot_id": "m.call#room", "member_id": "member", "user_id": "@ghost:example.org", "device_id": "DEVICE"}, "delay_id": delay_id}))
                path.chmod(0o600)
                return path
            first = "0" * 31 + "1"
            second = "0" * 31 + "2"
            paths = [record(first, None), record(first, "late-id", ".identified"), record(second, "known-id")]
            before = {p.name: (p.read_bytes(), p.stat().st_mode, p.stat().st_mtime_ns) for p in paths}
            result = subprocess.run([sys.executable, "-B", str(script), str(root)], capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            report = json.loads(result.stdout)
            self.assertFalse(report["automatic_settlement"])
            self.assertEqual([entry["status"] for entry in report["records"]], ["unknown", "interrupted-identification", "known"])
            self.assertEqual([entry["delay_id"] for entry in report["records"]], [None, "late-id", "known-id"])
            self.assertEqual(report["errors"], [])
            strict_command = [sys.executable, "-B", str(script), "--require-empty", str(root)]
            strict_result = subprocess.run(strict_command, capture_output=True, text=True)
            self.assertEqual(strict_result.returncode, 3, strict_result.stderr)
            self.assertEqual(json.loads(strict_result.stdout), report)
            self.assertEqual({p.name: (p.read_bytes(), p.stat().st_mode, p.stat().st_mtime_ns) for p in paths}, before)

    def test_delayed_membership_inspector_refuses_malformed_conflicting_or_linked_records(self):
        script = DEPLOY / "inspect-delayed-membership.py"
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            journal = root / "delayed-membership-operations"
            journal.mkdir(mode=0o700)
            token = "a" * 32
            operation = {"token": token, "room_id": "!room:example.org", "slot_id": "m.call#room", "member_id": "member", "user_id": "@ghost:example.org", "device_id": "DEVICE"}
            for name, data in [(token, json.dumps({"operation": operation, "delay_id": "one"})), (token + ".identified", json.dumps({"operation": operation, "delay_id": "two"})), ("b" * 32, '{"operation":{},"delay_id":null,"delay_id":"duplicate"}')]:
                path = journal / name
                path.write_text(data)
                path.chmod(0o600)
            external = root / "private"
            external.write_text("DO_NOT_READ")
            (journal / ("c" * 32)).symlink_to(external)
            before = sorted(p.name for p in journal.iterdir())
            result = subprocess.run([sys.executable, "-B", str(script), str(root)], capture_output=True, text=True)
            self.assertEqual(result.returncode, 2)
            report = json.loads(result.stdout)
            self.assertEqual(len(report["errors"]), 3)
            self.assertNotIn("DO_NOT_READ", result.stdout + result.stderr)
            self.assertEqual(sorted(p.name for p in journal.iterdir()), before)
            self.assertTrue((journal / ("c" * 32)).is_symlink())

    def test_delayed_membership_inspector_rejects_hardlinks_and_fifos(self):
        script = DEPLOY / "inspect-delayed-membership.py"
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            journal = root / "delayed-membership-operations"
            journal.mkdir(mode=0o700)
            external = root / "private"
            external.write_text("PRIVATE_FIXTURE")
            external.chmod(0o600)
            linked = journal / ("d" * 32)
            os.link(external, linked)
            fifo = journal / ("e" * 32)
            os.mkfifo(fifo, 0o600)
            result = subprocess.run([sys.executable, "-B", str(script), str(root)],
                                    capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode, 2)
            report = json.loads(result.stdout)
            self.assertEqual(report["records"], [])
            self.assertEqual(len(report["errors"]), 2)
            self.assertTrue(all("single link" in item["error"] for item in report["errors"]))
            self.assertNotIn("PRIVATE_FIXTURE", result.stdout + result.stderr)
            self.assertEqual(external.read_text(), "PRIVATE_FIXTURE")
            self.assertTrue(stat.S_ISFIFO(fifo.stat().st_mode))

    def test_delayed_membership_inspector_rejects_linked_or_public_identity(self):
        script = DEPLOY / "inspect-delayed-membership.py"
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            identity = root / "identity"
            identity.mkdir(mode=0o700)
            (identity / "delayed-membership-operations").mkdir(mode=0o700)
            alias = root / "alias"
            alias.symlink_to(identity, target_is_directory=True)
            for path in (alias, identity):
                if path == identity:
                    identity.chmod(0o755)
                result = subprocess.run([sys.executable, "-B", str(script), str(path)],
                                        capture_output=True, text=True, timeout=5)
                self.assertEqual(result.returncode, 2)
                self.assertIn("identity directory is not a private real directory", result.stderr)
                self.assertEqual(result.stdout, "")
            self.assertEqual(list((identity / "delayed-membership-operations").iterdir()), [])

    def test_delayed_membership_inspector_rejects_linked_or_public_journal(self):
        script = DEPLOY / "inspect-delayed-membership.py"
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            identity = root / "identity"
            identity.mkdir(mode=0o700)
            journal = identity / "delayed-membership-operations"
            other = root / "other"
            other.mkdir(mode=0o700)
            journal.symlink_to(other, target_is_directory=True)
            command = [sys.executable, "-B", str(script), str(identity)]
            result = subprocess.run(command, capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode, 2)
            self.assertIn("journal directory is not a private real directory", result.stderr)
            self.assertEqual(result.stdout, "")
            journal.unlink()
            journal.mkdir(mode=0o755)
            result = subprocess.run(command, capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode, 2)
            self.assertIn("journal directory is not a private real directory", result.stderr)
            self.assertEqual(result.stdout, "")

    def test_candidate_validation_is_read_only_and_precedes_pause(self):
        script = (DEPLOY / "install-or-upgrade.sh").read_text()
        self.assertLess(script.index('python3 -B "$RTC_ROOT/validate-candidate.py"'),
                        script.index("WORKER_PAUSED=1\n"))
        with TemporaryDirectory() as tmp:
            matrix = Path(tmp)
            root = matrix / "mautrix-whatsapp-call"
            shutil.copytree(DEPLOY, root, ignore=shutil.ignore_patterns("__pycache__"))
            (matrix / "synapse").mkdir()
            (matrix / "mautrix-whatsapp").mkdir()
            hs = matrix / "synapse/homeserver.yaml"
            bridge = matrix / "mautrix-whatsapp/config.yaml"
            hs.write_text("server_name: example.org\napp_service_config_files: []\n")
            bridge.write_text("network: {}\n")
            def snapshot():
                return {p.relative_to(matrix).as_posix(): (p.stat().st_mode, p.read_bytes() if p.is_file() else None)
                        for p in matrix.rglob("*")}
            def check(ok):
                before = snapshot()
                proc = subprocess.run([sys.executable, "-B", str(root / "validate-candidate.py"),
                                       "--server-name", "example.org"], capture_output=True, text=True)
                self.assertEqual(proc.returncode == 0, ok, proc.stderr)
                self.assertEqual(snapshot(), before)
                self.assertNotIn("fixture-secret", proc.stdout + proc.stderr)
            check(True)
            self.assertFalse((root / "generated").exists())
            hs.write_text("server_name: example.org\napp_service_config_files: wrong\n")
            check(False)
            hs.write_text("server_name: example.org\napp_service_config_files: []\n")
            bridge.write_text("network: false\n")
            check(False)
            bridge.write_text("network: {}\n")
            dest = root / "generated"
            dest.mkdir()
            (dest / "double_puppet_as_token").write_text("fixture-secret")
            bridge.write_text("network: {}\ndouble_puppet: {secrets: {example.org: 'as_token:other'}}\n")
            check(False)
            bridge.write_text("network: {}\ndouble_puppet: {secrets: {example.org: 'as_token:fixture-secret'}}\n")
            check(True)
            state = root / "worker-state/identities/ghost/crypto"
            state.mkdir(parents=True)
            (state / "store.db").touch()
            check(False)
            (dest / "matrixrtc_store_passphrase").write_text("fixture-secret")
            check(True)
            (dest / "livekit.yaml").mkdir()
            check(False)
            (dest / "livekit.yaml").rmdir()
            (root / "synapse-additions.yaml").write_text("invalid: [")
            check(False)

    def test_applied_configuration_readback_before_restart(self):
        script = (DEPLOY / "install-or-upgrade.sh").read_text()
        self.assertLess(script.index('merge-synapse.py" --apply'), script.index('--applied'))
        self.assertLess(script.index('--applied'), script.index('compose up -d --no-deps --force-recreate livekit'))
        with TemporaryDirectory() as tmp:
            matrix = Path(tmp)
            root = matrix / "mautrix-whatsapp-call"
            shutil.copytree(DEPLOY, root, ignore=shutil.ignore_patterns("__pycache__"))
            (matrix / "synapse").mkdir()
            (matrix / "mautrix-whatsapp").mkdir()
            hs = matrix / "synapse/homeserver.yaml"
            bridge = matrix / "mautrix-whatsapp/config.yaml"
            hs.write_text("server_name: example.org\napp_service_config_files: []\n")
            bridge.write_text("network: {}\n")
            env = {**os.environ, "MATRIXRTC_SERVER_NAME": "example.org"}
            def check(ok):
                proc = subprocess.run([sys.executable, "-B", str(root / "validate-candidate.py"),
                                       "--server-name", "example.org", "--applied"],
                                      capture_output=True, text=True, env=env)
                self.assertEqual(proc.returncode == 0, ok, proc.stderr)
                if not ok:
                    self.assertIn("refusing service restart", proc.stderr)
                    self.assertNotIn("fixture-secret", proc.stderr + proc.stdout)
            check(False)
            subprocess.run([sys.executable, str(root / "render-hostname.py"), "--server-name", "example.org"], check=True, capture_output=True, env=env)
            subprocess.run(["bash", str(root / "prepare.sh")], check=True, capture_output=True, env=env)
            check(False)
            subprocess.run([sys.executable, str(root / "configure-bridge.py"), "--config", str(bridge), "--server-name", "example.org", "--double-puppet-secret-file", str(root / "generated/double_puppet_secret"), "--apply"], check=True, capture_output=True, env=env)
            check(False)
            subprocess.run([sys.executable, str(root / "merge-synapse.py"), "--apply"], check=True, capture_output=True, env=env)
            check(True)
            for path, replacement in ((bridge, "network: {}\n"),
                    (hs, "server_name: example.org\napp_service_config_files: []\n"),
                    (root / "generated/double_puppet_secret", "fixture-secret\n"),
                    (root / "generated/livekit.yaml", "keys: {}\n"),
                    (matrix / "synapse/matrixrtc-registration.yaml", "id: incomplete\n"),
                    (root / "generated/as_token", ""),
                    (root / "generated/matrixrtc_store_passphrase", None)):
                original = path.read_bytes()
                if replacement is None:
                    path.unlink()
                else:
                    path.write_text(replacement)
                check(False)
                path.write_bytes(original)
                check(True)

    def test_live_config_rollback_before_restart(self):
        script = (DEPLOY / "install-or-upgrade.sh").read_text()
        self.assertLess(script.index("--snapshot-live"), script.index('merge-synapse.py" --apply'))
        restart = script.index('compose up -d --no-deps --force-recreate mautrix-whatsapp-call-worker', script.index('--applied'))
        self.assertLess(script.index('--mark-restart'), restart)
        self.assertLess(restart, script.index('INSTALL_COMPLETE=1'))
        self.assertLess(script.index('INSTALL_COMPLETE=1'), script.index('rm -rf -- "$LIVE_JOURNAL"', script.index('--applied')))
        with TemporaryDirectory() as tmp:
            matrix = Path(tmp)
            root = matrix / "mautrix-whatsapp-call"
            root.mkdir()
            (root / "generated").mkdir()
            (matrix / "synapse").mkdir()
            (matrix / "mautrix-whatsapp").mkdir()
            hs = matrix / "synapse/homeserver.yaml"
            bridge = matrix / "mautrix-whatsapp/config.yaml"
            reg = matrix / "synapse/matrixrtc-registration.yaml"
            new = matrix / "synapse/matrixrtc-doublepuppet-registration.yaml"
            hs.write_text("server_name: example.org\napp_service_config_files: []\n")
            hs.chmod(0o600)
            bridge.write_text("network: {}\n")
            bridge.chmod(0o604)
            reg.write_text("old registration\n")
            reg.chmod(0o640)
            (root / "synapse-additions.yaml").write_text("max_event_delay_duration: 24h\n")
            (root / "generated/double_puppet_secret").write_text("as_token:fixture-secret\n")
            for p in (reg, new):
                (root / "generated" / p.name).write_text("new registration\n")
            api = runpy.run_path(str(DEPLOY / "validate-candidate.py"))
            snapshot, restore = api["snapshot_live"], api["restore_live"]
            journal = matrix / "journal"
            journal.mkdir(mode=0o700)
            before = [(p.read_bytes(), stat.S_IMODE(p.stat().st_mode)) for p in (bridge, hs, reg)]
            # The manifest and its parent directory entry must be durable
            # before the installer can begin live writes.
            with mock.patch.dict(snapshot.__globals__, {"sync_directory": mock.Mock(side_effect=OSError("flush failed"))}):
                with self.assertRaises(OSError):
                    snapshot(root, "example.org", journal)
            self.assertTrue((journal / "manifest.json").is_file())
            self.assertEqual([(p.read_bytes(), stat.S_IMODE(p.stat().st_mode)) for p in (bridge, hs, reg)], before)
            self.assertFalse(new.exists())
            # The failed journal is deliberately retained, not reused.
            shutil.rmtree(journal)
            journal.mkdir(mode=0o700)
            real_sync = snapshot.__globals__["sync_directory"]
            def observe_sync(path):
                self.assertEqual(path, journal.parent)
                self.assertTrue((journal / "manifest.json").is_file())
                real_sync(path)
            with mock.patch.dict(snapshot.__globals__, {"sync_directory": mock.Mock(side_effect=observe_sync)}) as globals_:
                snapshot(root, "example.org", journal)
                globals_["sync_directory"].assert_called_once_with(journal.parent)
            self.assertTrue(all(stat.S_IMODE(p.stat().st_mode) == 0o600 for p in journal.iterdir()))
            # Interruption after the bridge chmod but before its content write.
            bridge.chmod(0o600)
            restore(root, journal)
            self.assertEqual(stat.S_IMODE(bridge.stat().st_mode), before[0][1])
            bridge.write_text(configure(bridge.read_text(), "example.org", "as_token:fixture-secret"))
            bridge.chmod(0o600)
            hs.write_text(merge(hs.read_text(), (root / "synapse-additions.yaml").read_text())[0])
            for p in (reg, new):
                p.write_text("new registration\n")
                p.chmod(0o600)
            applied = bridge.read_bytes()
            reg.write_text("external modification\n")
            with self.assertRaises(ValueError):
                restore(root, journal)
            self.assertEqual(bridge.read_bytes(), applied)
            self.assertTrue(new.exists())
            reg.write_text("new registration\n")
            # Content is valid, but an unrelated metadata edit must block
            # restoration of every file, including an earlier changed one.
            for path in (reg, new):
                path.chmod(0o644)
                with self.assertRaises(ValueError):
                    restore(root, journal)
                self.assertEqual(bridge.read_bytes(), applied)
                self.assertTrue(new.exists())
                path.chmod(0o600)
            manifest = journal / "manifest.json"
            valid_manifest = manifest.read_bytes()
            # Reject malformed metadata in a later entry before restoring the
            # first file, even when every content hash matches.
            for key, bad in (("mode", -1), ("uid", True), ("gid", 0xffffffff),
                             ("present", 1), ("planned", "invalid"), ("planned_metadata", [0, 0, -1])):
                entries = json.loads(valid_manifest)
                entries[2][key] = bad
                manifest.write_text(json.dumps(entries))
                with self.assertRaises(ValueError):
                    restore(root, journal)
                self.assertEqual(bridge.read_bytes(), applied)
                self.assertTrue(new.exists())
            manifest.write_bytes(valid_manifest)
            backup = journal / "2"
            backup.chmod(0o644)
            with self.assertRaises(ValueError):
                restore(root, journal)
            self.assertEqual(bridge.read_bytes(), applied)
            backup.chmod(0o600)
            backup.rename(journal / "saved-2")
            backup.symlink_to(journal / "saved-2")
            with self.assertRaises(ValueError):
                restore(root, journal)
            self.assertEqual(bridge.read_bytes(), applied)
            backup.unlink()
            (journal / "saved-2").rename(backup)
            restore(root, journal)
            self.assertEqual([(p.read_bytes(), stat.S_IMODE(p.stat().st_mode)) for p in (bridge, hs, reg)], before)
            self.assertFalse(new.exists())
            self.assertEqual((root / "generated/double_puppet_secret").read_text(), "as_token:fixture-secret\n")
            restore(root, journal)  # safe to repeat after interrupted recovery
            # Once a service may have loaded the new config, the old files
            # must not be restored underneath it, even if the bytes match.
            # A corrupt or missing backup must block service recreation even
            # if the live configuration passed the applied-state checks.
            backup_bytes = backup.read_bytes()
            backup.write_bytes(b"corrupted snapshot")
            with self.assertRaises(ValueError):
                api["mark_restart"](root, journal)
            self.assertFalse((journal / "restart-started").exists())
            backup.unlink()
            with self.assertRaises(FileNotFoundError):
                api["mark_restart"](root, journal)
            self.assertFalse((journal / "restart-started").exists())
            backup.write_bytes(backup_bytes)
            backup.chmod(0o600)
            manifest.write_text("[]")
            with self.assertRaises(ValueError):
                api["mark_restart"](root, journal)
            self.assertFalse((journal / "restart-started").exists())
            manifest.write_bytes(valid_manifest)
            # A valid journal alone is not sufficient: the live files must
            # still equal the applied candidate at the restart boundary.
            with self.assertRaises(ValueError):
                api["mark_restart"](root, journal)
            self.assertFalse((journal / "restart-started").exists())
            bridge.write_bytes(applied)
            bridge.chmod(0o600)
            hs.write_text(merge(hs.read_text(), (root / "synapse-additions.yaml").read_text())[0])
            for p in (reg, new):
                p.write_text("new registration\n")
                p.chmod(0o600)
            new.chmod(0o644)
            with self.assertRaises(ValueError):
                api["mark_restart"](root, journal)
            self.assertFalse((journal / "restart-started").exists())
            new.chmod(0o600)
            api["mark_restart"](root, journal)
            self.assertEqual(stat.S_IMODE((journal / "restart-started").stat().st_mode), 0o600)
            with self.assertRaises(ValueError):
                api["mark_restart"](root, journal)
            bridge.write_text(applied.decode())
            with self.assertRaises(ValueError):
                restore(root, journal)
            self.assertEqual(bridge.read_bytes(), applied)
            self.assertTrue((journal / "manifest.json").exists())

    def test_pending_journal_blocks_next_upgrade(self):
        script = (DEPLOY / "install-or-upgrade.sh").read_text()
        start = script.index("require_no_pending_journal() {")
        end = script.index("\nrequire_no_pending_journal || exit 2", start)
        guard = script[start:end]
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            env = {**os.environ, "MATRIX_ROOT": tmp}
            def check(ok):
                proc = subprocess.run(["bash", "-c", guard + "\nrequire_no_pending_journal"], env=env, capture_output=True, text=True)
                self.assertEqual(proc.returncode == 0, ok, proc.stderr)
            check(True)
            journal = root / ".matrixrtc-config-rollback.abandoned"
            journal.mkdir(mode=0o700)
            check(False)
            (journal / "restart-started").touch()
            check(False)
            shutil.rmtree(journal)
            journal.symlink_to(root / "missing")
            check(False)
            journal.unlink()
            check(True)


    def test_jwt_image_contract_refuses_incompatible_replacement(self):
        script=(DEPLOY/"install-or-upgrade.sh").read_text()
        start=script.index("JWT_IMAGE=")
        end=script.index("\nverify_jwt_image_contract\n", start)
        guard=script[start:end]
        pin="ghcr.io/element-hq/lk-jwt-service:0.9.0"
        self.assertEqual(yaml.safe_load((DEPLOY/"compose.yml").read_text())["services"]["lk-jwt-service"]["image"],pin)
        fixture="""compose() {
    case "$1" in
        config) printf '%s\\n' "$FIXTURE_CONFIG"; return "$CONFIG_RC" ;;
        ps) printf '%s\\n' "$FIXTURE_IDS"; return "$PS_RC" ;;
        *) return 99 ;;
    esac
}
docker() {
    case "$1:$2" in
        inspect:--format)
            [[ "$INSPECT_RC" == 0 ]] || return "$INSPECT_RC"
            case "$3" in
                '{{.Config.Image}}') printf '%s\\n' "$FIXTURE_IMAGE" ;;
                '{{.Image}}') if [[ "$4" == second ]]; then printf '%s\\n' "$SECOND_ID"; else printf '%s\\n' "$CONTAINER_ID"; fi ;;
                '{{.State.Running}}') printf '%s\\n' "$RUNNING" ;;
                *) return 99 ;;
            esac ;;
        image:inspect)
            [[ "$LOCAL_RC" == 0 || "${PULLED:-0}" == 1 ]] || return 1
            printf '%s\\n' "$LOCAL_ID" ;;
        pull:*) [[ "$ALLOW_PULL" == 1 ]] || return 98; PULLED=1 ;;
        *) return 99 ;;
    esac
}
"""
        good="sha256:"+"a"*64
        other="sha256:"+"b"*64
        defaults={"FIXTURE_IDS":"", "FIXTURE_IMAGE":pin, "CONFIG_RC":"0", "PS_RC":"0",
                  "INSPECT_RC":"0", "CONTAINER_ID":good, "SECOND_ID":good,
                  "LOCAL_ID":good, "LOCAL_RC":"0", "ALLOW_PULL":"0", "RUNNING":"true"}
        cases=[
            ("fresh local", {}, {}, True),
            ("existing pin", {}, {"FIXTURE_IDS":"container"}, True),
            ("fresh pull", {}, {"LOCAL_RC":"1", "ALLOW_PULL":"1"}, True),
            ("pull fails", {}, {"LOCAL_RC":"1"}, False),
            ("existing missing image", {}, {"FIXTURE_IDS":"container", "LOCAL_RC":"1", "ALLOW_PULL":"1"}, False),
            ("retagged", {}, {"FIXTURE_IDS":"container", "LOCAL_ID":other}, False),
            ("mixed image IDs", {}, {"FIXTURE_IDS":"container\nsecond", "SECOND_ID":other}, False),
            ("invalid image ID", {}, {"LOCAL_ID":"not-an-id"}, False),
            ("stopped newer service", {}, {"FIXTURE_IDS":"container", "FIXTURE_IMAGE":pin.replace("0.9.0","0.10.0")}, False),
            ("override", {"image":pin.replace("0.9.0","latest")}, {}, False),
            ("custom build", {"build":{"context":"."}}, {}, False),
            ("always pull", {"pull_policy":"always"}, {}, False),
            ("config error", {}, {"CONFIG_RC":"1"}, False),
            ("list error", {}, {"PS_RC":"1"}, False),
            ("inspect error", {}, {"FIXTURE_IDS":"container", "INSPECT_RC":"1"}, False),
        ]
        for label, changes, overrides, ok in cases:
            with self.subTest(label=label):
                service={"image":pin, **changes}
                env={**os.environ, **defaults, **overrides,
                     "FIXTURE_CONFIG":json.dumps({"services":{"lk-jwt-service":service}})}
                result=subprocess.run(["bash","-ec","set -o pipefail\n"+fixture+guard+"\nverify_jwt_image_contract"],
                                      env=env,capture_output=True,text=True)
                self.assertEqual(result.returncode==0,ok,result.stderr)
        env={**os.environ, **defaults, "FIXTURE_CONFIG":json.dumps({"services":{"lk-jwt-service":{"image":pin}}})}
        for commands, ok in (
            ("verify_jwt_image_contract; verify_jwt_image_contract",True),
            ("verify_jwt_image_contract; LOCAL_ID=sha256:"+"b"*64+"; verify_jwt_image_contract",False),
            ("verify_jwt_image_contract; LOCAL_RC=1; ALLOW_PULL=1; verify_jwt_image_contract",False),
            ("verify_jwt_image_contract; FIXTURE_IDS=container; verify_running_jwt_image",True),
            ("verify_jwt_image_contract; FIXTURE_IDS=container; RUNNING=false; verify_running_jwt_image",False),
            ("verify_jwt_image_contract; verify_running_jwt_image",False),
            ("verify_jwt_image_contract; FIXTURE_IDS=container; CONTAINER_ID=sha256:"+"b"*64+"; verify_running_jwt_image",False),
            ("verify_jwt_image_contract; FIXTURE_IDS=$'container\nsecond'; verify_running_jwt_image",False),
        ):
            with self.subTest(commands=commands):
                result=subprocess.run(["bash","-ec","set -o pipefail\n"+fixture+guard+"\n"+commands],
                                      env=env,capture_output=True,text=True)
                self.assertEqual(result.returncode==0,ok,result.stderr)
        self.assertIn("--pull never --force-recreate lk-jwt-service\nverify_running_jwt_image",script)
        self.assertLess(end,script.index("WORKER_PAUSED=1\n"))

    def test_worker_focus_policy_matches_deployment(self):
        compose = yaml.safe_load((DEPLOY / "compose.yml").read_text())
        services = compose["services"]
        worker = services["mautrix-whatsapp-call-worker"]["environment"]
        self.assertEqual(worker["MATRIXRTC_SERVER_NAME"], "${MATRIXRTC_SERVER_NAME:?MATRIXRTC_SERVER_NAME is required}")
        synapse = yaml.safe_load((DEPLOY / "synapse-additions.yaml").read_text())
        expected_sfu = "wss://__MATRIXRTC_SERVER_NAME__/livekit/sfu"
        expected_jwt = "https://__MATRIXRTC_SERVER_NAME__/livekit/jwt"
        def transports(value):
            if isinstance(value, dict):
                if "livekit_service_url" in value:
                    yield value
                for child in value.values():
                    yield from transports(child)
            elif isinstance(value, list):
                for child in value:
                    yield from transports(child)
        advertised = list(transports(synapse))
        self.assertTrue(advertised)
        for transport in advertised:
            self.assertEqual(transport["url"], expected_sfu)
            self.assertEqual(transport["livekit_service_url"], expected_jwt)
        self.assertEqual(services["lk-jwt-service"]["environment"]["LIVEKIT_URL"],
                         "wss://${MATRIXRTC_SERVER_NAME:?MATRIXRTC_SERVER_NAME is required}/livekit/sfu")

    def test_shared_ghost_namespace_matches_migration_and_provisioning(self):
        import re
        from ghost_namespace import resolve_metadata
        resolve = runpy.run_path(str(DEPLOY/"resolve-appservice.py"))["resolve"]
        scope = automatic["scan_once"].__globals__
        with TemporaryDirectory() as tmp:
            reg, cfg = Path(tmp)/"registration.yaml", Path(tmp)/"config.yaml"
            cfg.write_text(json.dumps({"appservice": {"username_template": "wa-custom-{{.}}",
                                                      "bot": {"username": "custombot"}}}))
            def registration(entries):
                reg.write_text(json.dumps({"as_token": "fixture", "sender_localpart": "custombot",
                                          "namespaces": {"users": entries}}))
            entries = [{"exclusive": True, "regex": r"^@wa-custom-[0-9]+:example\.org$"},
                       {"exclusive": False, "regex": r"^@.*$"}]
            registration(entries)
            metadata = resolve(reg, cfg, "example.org")
            self.assertEqual(metadata, resolve_metadata(reg, cfg, "example.org"))
            self.assertEqual(metadata, scope["resolve_metadata"](reg, cfg, "example.org"))
            patterns = metadata["ghost_regexes"]
            candidates = ["@wa-custom-123:example.org", "@wa-custom-alice:example.org",
                          "@wa-custom-123:foreign.org", "@telegram_123:example.org",
                          "@custombot:example.org", "@wa-custom-123:example.org\n"]
            expected = [True, False, False, False, False, False]
            self.assertEqual([scope["is_bridge_user"](u, [re.compile(p) for p in patterns])
                              for u in candidates], expected)
            if shutil.which("jq"):
                result = subprocess.run(["jq", "-c", "--argjson", "patterns", json.dumps(patterns),
                                         "map(. as $u | any($patterns[]; . as $rx | ($u | test($rx))))"],
                                        input=json.dumps(candidates), text=True, check=True,
                                        capture_output=True)
                self.assertEqual(json.loads(result.stdout), expected)
            for invalid in [[], entries[1:], [{"exclusive": True, "regex": "["}]]:
                registration(invalid)
                with self.assertRaises(ValueError):
                    resolve(reg, cfg, "example.org")
                with self.assertRaises(ValueError):
                    scope["resolve_metadata"](reg, cfg, "example.org")
            registration(entries)
            cfg.write_text("appservice: {username_template: x, username_template: y}")
            with self.assertRaisesRegex(ValueError, "duplicate"):
                resolve(reg, cfg, "example.org")

    def test_portal_metadata_requires_complete_agreement(self):
        import copy
        import re
        from portal_kind import portal_kind
        bot = "@whatsappbot:example.org"
        regexes = [r"^@whatsapp_[0-9]+:example\.org$"]
        patterns = [re.compile(r) for r in regexes]
        portal = {"type": "m.bridge", "sender": bot, "content": {
            "bridgebot": bot, "protocol": {"id": "whatsapp"},
            "channel": {"id": "123@s.whatsapp.net"}, "com.beeper.room_type.v2": "dm"}}
        def modified(path, value):
            result = copy.deepcopy(portal)
            node = result
            for part in path[:-1]:
                node = node[part]
            node[path[-1]] = value
            return result
        same = modified(["type"], "uk.half-shot.bridge")
        ghost = modified(["sender"], "@whatsapp_123:example.org")
        group = modified(["content", "channel", "id"], "123@g.us")
        cases = [([], "none"), ([{"type": "m.room.name"}], "none"),
                 ([portal], "dm"), ([portal, same, ghost], "dm"), ([group], "group"),
                 ([portal, group], "other"), (None, "other"), ([portal, 1], "other")]
        for path, value in [(["sender"], "@foreign:example.org"),
                            (["sender"], "@whatsapp_123:foreign.org"),
                            (["content", "bridgebot"], "@otherbot:example.org"),
                            (["content", "protocol", "id"], "telegram"),
                            (["content", "protocol"], []), (["content", "channel"], None),
                            (["content", "channel", "id"], "456@s.whatsapp.net"),
                            (["content", "channel", "id"], "@s.whatsapp.net"),
                            (["content", "channel", "id"], "123@s.whatsapp.net\n"),
                            (["content", "com.beeper.room_type.v2"], "group")]:
            cases.append(([portal, modified(path, value)], "other"))
        for state, expected in cases:
            with self.subTest(state=state):
                self.assertEqual(portal_kind(state, bot, patterns), expected)
                self.assertEqual(automatic["is_whatsapp_dm_portal"](state, bot, patterns), expected == "dm")
                result = subprocess.run([sys.executable, "-B", str(DEPLOY/"worker/portal_kind.py"),
                    bot, json.dumps(regexes)], input=json.dumps(state), text=True, capture_output=True, check=True)
                self.assertEqual(result.stdout.strip(), expected)

    def test_bridge_yaml(self):
        raw="network:\n  unknown: preserved\nmatrix: {federate_rooms: false}\nencryption: {allow: false}\n"
        out=configure(raw,"example.org","as_token:test-only")
        data=yaml.safe_load(out)
        self.assertEqual(data["network"]["unknown"],"preserved")
        self.assertIs(data["matrix"]["federate_rooms"],False)
        self.assertTrue(data["encryption"]["allow"])
        self.assertEqual(configure(out,"example.org","as_token:test-only"),out)

    def test_synapse_maps_and_inline_registration_list(self):
        raw=("experimental_features: {other: true}\n"
             "app_service_config_files: [/data/else.yaml] # /data/matrixrtc-registration.yaml\n"
             "matrix_rtc: {transports: [{type: custom, url: x}]}\n")
        extra=(DEPLOY/"synapse-additions.yaml").read_text()
        out,_=merge(raw,extra)
        data=yaml.safe_load(out)
        self.assertTrue(data["experimental_features"]["msc4143_enabled"])
        self.assertIs(data["experimental_features"]["msc4354_enabled"], True)
        self.assertTrue(data["experimental_features"]["other"])
        self.assertEqual(len(data["app_service_config_files"]),3)
        self.assertEqual(len(data["matrix_rtc"]["transports"]),3)
        self.assertEqual(merge(out,extra)[0],out)

    def test_modern_homeserver_feature_verification(self):
        # Exercise the actual versions validator from the operator script.
        import shlex
        script = (DEPLOY / "verify.sh").read_text()
        code = shlex.split(script.split("python3 -c ", 1)[1].split("\necho", 1)[0])[0]
        for features, expected in [
            ({"org.matrix.msc4143": True, "org.matrix.msc4354": True}, True),
            ({"org.matrix.msc4143.stable": True, "org.matrix.msc4354": True}, True),
            ({"org.matrix.msc4143": True}, False),
            ({"org.matrix.msc4143": True, "org.matrix.msc4354": False}, False),
            ({"org.matrix.msc4354": True}, False),
            ({"org.matrix.msc4143": True, "org.matrix.msc4354": "true"}, False),
            (None, False),
        ]:
            result = subprocess.run([sys.executable, "-c", code], text=True,
                input=json.dumps({"versions": ["v1.17"], "unstable_features": features}),
                capture_output=True)
            self.assertEqual(result.returncode == 0, expected, result.stderr)

    def test_identity_lock_busy_does_not_mutate_and_can_retry(self):
        import fcntl
        with TemporaryDirectory() as tmp:
            folder = Path(tmp)/"identity"
            folder.mkdir(mode=0o755)
            path = folder/".provision.lock"
            with path.open("w") as owner:
                fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
                with self.assertRaisesRegex(BlockingIOError, "identity is busy"):
                    with session_files.identity_lock(folder):
                        self.fail("entered locked identity")
                self.assertEqual(folder.stat().st_mode & 0o777, 0o755)
                self.assertFalse((folder/"session.json").exists())
            with session_files.identity_lock(folder):
                self.assertEqual(folder.stat().st_mode & 0o777, 0o700)
            self.assertTrue(path.is_file())

    def test_interrupted_identity_and_matching_store(self):
        with TemporaryDirectory() as tmp:
            folder=Path(tmp)/"identity"
            ident="@ghost:example.org"
            session={"user_id":ident,"device_id":"D","access_token":"fixture"}
            with session_files.identity_lock(folder):
                (folder/"session.json").write_text("{")
                with self.assertRaisesRegex(ValueError, "session.json"):
                    session_files.ensure_session(folder, ident, lambda: self.fail("unexpected login"))
                self.assertEqual((folder/"session.json").read_text(), "{")
                # Explicit operator reconciliation, not automatic replacement.
                (folder/"session.json").unlink()
                self.assertEqual(session_files.ensure_session(folder,ident,lambda:session),session)
                self.assertEqual((folder/"session.json").stat().st_mode&0o777,0o600)
                (folder/"matrixrtc-ready-v1").touch()
                self.assertFalse(session_files.is_ready(folder,ident))
                with self.assertRaisesRegex(ValueError,"crypto store"):
                    session_files.ensure_session(folder,ident,lambda:self.fail("unexpected login"))
                (folder/"crypto").mkdir()
                (folder/"crypto/matrix-sdk-crypto.sqlite3").write_bytes(b"fixture")
                self.assertFalse(session_files.is_ready(folder,ident))
                session_files.mark_ready(folder,ident)
                self.assertTrue(session_files.is_ready(folder,ident))
                (folder/"matrixrtc-ready-v1").write_text(json.dumps({"user_id":ident,"device_id":"OTHER"}))
                self.assertFalse(session_files.is_ready(folder,ident))
                session_files.mark_ready(folder,ident)
                self.assertEqual(session_files.ensure_session(folder,ident,lambda:self.fail("unexpected login")),session)
                (folder/"session.json").unlink()
                with self.assertRaisesRegex(ValueError,"session.json"):
                    session_files.ensure_session(folder,ident,lambda:self.fail("unexpected login"))

    def test_inconsistent_identity_evidence_is_preserved(self):
        ident = "@ghost:example.org"
        session = {"user_id": ident, "device_id": "D", "access_token": "fixture"}
        def no_login():
            self.fail("unexpected replacement login")
        for name, setup in [
            ("empty crypto", lambda f: (f / "crypto").mkdir()),
            ("dangling crypto", lambda f: (f / "crypto").symlink_to(f / "missing")),
            ("dangling readiness", lambda f: (f / "matrixrtc-ready-v1").symlink_to(f / "missing")),
            ("dangling recovery", lambda f: (f / "recovery-key.txt").symlink_to(f / "missing")),
            ("duplicate device", lambda f: (f / "session.json").write_text(
                '{"user_id":"@ghost:example.org","device_id":"X","device_id":"D","access_token":"fixture"}')),
            ("incomplete session", lambda f: (f / "session.json").write_text('{"device_id":"D"}')),
            ("session directory", lambda f: (f / "session.json").mkdir()),
        ]:
            with self.subTest(name=name), TemporaryDirectory() as tmp:
                folder = Path(tmp)
                setup(folder)
                before = sorted((p.name, p.lstat().st_ino) for p in folder.iterdir())
                with self.assertRaises(ValueError):
                    session_files.ensure_session(folder, ident, no_login)
                self.assertEqual(before, sorted((p.name, p.lstat().st_ino) for p in folder.iterdir()))
        with TemporaryDirectory() as tmp:
            folder = Path(tmp)
            session_files.ensure_session(folder, ident, lambda: session)
            crypto = folder / "crypto"
            crypto.mkdir()
            partial = crypto / "matrix-sdk-crypto.sqlite3"
            partial.touch()
            with self.assertRaisesRegex(ValueError, "crypto store"):
                session_files.ensure_session(folder, ident, no_login)
            partial.write_bytes(b"store")
            marker = folder / "matrixrtc-ready-v1"
            for invalid in ("{", json.dumps({"user_id": ident, "device_id": "OTHER"}),
                            json.dumps({"user_id": "@other:example.org", "device_id": "D"})):
                marker.write_text(invalid)
                with self.assertRaisesRegex(ValueError, "readiness"):
                    session_files.ensure_session(folder, ident, no_login)
                self.assertEqual(marker.read_text(), invalid)
                self.assertFalse(session_files.is_ready(folder, ident))
            marker.write_text("{}")
            self.assertEqual(session_files.ensure_session(folder, ident, no_login), session)
            self.assertEqual(partial.read_bytes(), b"store")
        self.assertEqual((DEPLOY / "worker/session_files.py").read_bytes(),
                         (ROOT / "rtc/matrixrtc-worker/session_files.py").read_bytes())

    def test_ready_marker_tracks_store_replacement_and_failed_preparation(self):
        with TemporaryDirectory() as tmp:
            folder = Path(tmp) / "identity"
            ident = "@ghost:example.org"
            session = {"user_id": ident, "device_id": "D", "access_token": "fixture"}
            with session_files.identity_lock(folder):
                session_files.ensure_session(folder, ident, lambda: session)
                crypto = folder / "crypto"
                crypto.mkdir()
                db = crypto / "matrix-sdk-crypto.sqlite3"
                db.write_bytes(b"initial")
                marker = folder / "matrixrtc-ready-v1"
                marker.write_text(json.dumps({"user_id": ident, "device_id": "D"}))
                self.assertFalse(session_files.is_ready(folder, ident))
                session_files.mark_ready(folder, ident)
                self.assertTrue(session_files.is_ready(folder, ident))
                db.write_bytes(b"normal in-place database update")
                self.assertTrue(session_files.is_ready(folder, ident))
                replacement = crypto / "replacement"
                replacement.write_bytes(b"different store")
                replacement.replace(db)
                self.assertFalse(session_files.is_ready(folder, ident))
                session_files.mark_ready(folder, ident)
                session_files.invalidate_ready(folder)
                self.assertFalse(session_files.is_ready(folder, ident))
                self.assertTrue(session_files.identity_evidence(folder))
                self.assertEqual(session_files.ensure_session(folder, ident, lambda: self.fail("unexpected login")), session)
                session_files.mark_ready(folder, ident)
                self.assertTrue(session_files.is_ready(folder, ident))
                db.rename(crypto / "other")
                db.symlink_to(crypto / "other")
                self.assertFalse(session_files.is_ready(folder, ident))

    def test_readiness_commit_rejects_identity_replacement(self):
        with TemporaryDirectory() as tmp:
            folder = Path(tmp) / "identity"
            ident = "@ghost:example.org"
            session = {"user_id": ident, "device_id": "D", "access_token": "fixture"}
            with session_files.identity_lock(folder):
                session_files.ensure_session(folder, ident, lambda: session)
                db = folder / "crypto/matrix-sdk-crypto.sqlite3"
                with session_files.prepare_ready(folder, ident):
                    db.parent.mkdir()
                    db.write_bytes(b"first store")
                self.assertTrue(session_files.is_ready(folder, ident))
                with session_files.prepare_ready(folder, ident):
                    db.write_bytes(b"ordinary update")
                self.assertTrue(session_files.is_ready(folder, ident))
                with self.assertRaisesRegex(ValueError, "crypto store changed"):
                    with session_files.prepare_ready(folder, ident):
                        replacement = db.with_suffix(".replacement")
                        replacement.write_bytes(b"replacement")
                        replacement.replace(db)
                self.assertFalse(session_files.is_ready(folder, ident))
                pending = json.loads((folder / "matrixrtc-ready-v1").read_text())
                self.assertIs(pending["ready"], False)
                with self.assertRaisesRegex(ValueError, "readiness"):
                    session_files.ensure_session(folder, ident, lambda: self.fail("unexpected login"))
                with self.assertRaisesRegex(ValueError, "readiness"):
                    with session_files.prepare_ready(folder, ident):
                        self.fail("replacement accepted")
                # Explicit operator reconciliation of the replacement store.
                session_files.mark_ready(folder, ident)
                with self.assertRaisesRegex(ValueError, "session changed"):
                    with session_files.prepare_ready(folder, ident):
                        (folder / "session.json").write_text(json.dumps({**session, "device_id": "new"}))
                self.assertFalse(session_files.is_ready(folder, ident))
                # Restore the matching session before retrying a failed preparation.
                (folder / "session.json").write_text(json.dumps(session))
                with self.assertRaisesRegex(RuntimeError, "failed"):
                    with session_files.prepare_ready(folder, ident):
                        raise RuntimeError("failed")
                self.assertFalse(session_files.is_ready(folder, ident))

    def test_pending_readiness_binding_survives_failed_retry(self):
        with TemporaryDirectory() as tmp:
            folder = Path(tmp)
            ident = "@ghost:example.org"
            session = {"user_id": ident, "device_id": "D", "access_token": "fixture"}
            no_login = lambda: self.fail("unexpected login")
            session_files.ensure_session(folder, ident, lambda: session)
            db = folder / "crypto/matrix-sdk-crypto.sqlite3"
            db.parent.mkdir()
            db.write_bytes(b"store")
            session_files.mark_ready(folder, ident)
            marker = folder / "matrixrtc-ready-v1"
            original = json.loads(marker.read_text())
            for invalid in (
                {**original, "store": {"device": True, "inode": original["store"]["inode"]}},
                {**original, "store": {"device": original["store"]["device"], "inode": 0}},
                {**original, "ready": 0},
                {**original, "ready": True},
                {**original, "unexpected": 1},
                {"user_id": ident, "device_id": "D"},
            ):
                with self.subTest(invalid=invalid):
                    marker.write_text(json.dumps(invalid))
                    self.assertFalse(session_files.is_ready(folder, ident))
                    with self.assertRaisesRegex(ValueError, "readiness"):
                        session_files.ensure_session(folder, ident, no_login)
                    self.assertEqual(json.loads(marker.read_text()), invalid)
            marker.write_text(json.dumps(original))
            with self.assertRaisesRegex(RuntimeError, "failure"):
                with session_files.prepare_ready(folder, ident):
                    raise RuntimeError("failure")
            pending = {**original, "ready": False}
            self.assertEqual(json.loads(marker.read_text()), pending)
            self.assertFalse(session_files.is_ready(folder, ident))
            db.write_bytes(b"legitimate update")
            self.assertEqual(session_files.ensure_session(folder, ident, no_login), session)
            with session_files.prepare_ready(folder, ident):
                pass
            self.assertTrue(session_files.is_ready(folder, ident))
            session_files.invalidate_ready(folder)
            old = db.with_suffix(".original")
            db.rename(old)
            db.write_bytes(b"replacement")
            with self.assertRaisesRegex(ValueError, "readiness"):
                session_files.ensure_session(folder, ident, no_login)
            self.assertEqual(json.loads(marker.read_text()), pending)
            old.replace(db)
            with session_files.prepare_ready(folder, ident):
                pass
            self.assertTrue(session_files.is_ready(folder, ident))

    def test_first_preparation_intent_and_failure_binding(self):
        ident = "@ghost:example.org"
        session = {"user_id": ident, "device_id": "D", "access_token": "fixture"}
        no_login = lambda: self.fail("unexpected replacement login")
        with TemporaryDirectory() as tmp:
            folder = Path(tmp) / "identity"
            with session_files.identity_lock(folder):
                session_files.ensure_session(folder, ident, lambda: session)
                marker = folder / "matrixrtc-ready-v1"
                expected = {"user_id": ident, "device_id": "D", "store": None, "ready": False}
                with self.assertRaisesRegex(RuntimeError, "before store"):
                    with session_files.prepare_ready(folder, ident):
                        self.assertEqual(json.loads(marker.read_text()), expected)
                        self.assertFalse(session_files.is_ready(folder, ident))
                        raise RuntimeError("before store")
                self.assertEqual(json.loads(marker.read_text()), expected)
                self.assertEqual(session_files.ensure_session(folder, ident, no_login), session)
                db = folder / "crypto/matrix-sdk-crypto.sqlite3"
                with self.assertRaisesRegex(RuntimeError, "after store"):
                    with session_files.prepare_ready(folder, ident):
                        db.parent.mkdir()
                        db.write_bytes(b"new store")
                        raise RuntimeError("after store")
                expected["store"] = session_files.store_identity(folder)
                self.assertEqual(json.loads(marker.read_text()), expected)
                self.assertFalse(session_files.is_ready(folder, ident))
                self.assertEqual(session_files.ensure_session(folder, ident, no_login), session)
                db.rename(db.with_suffix(".old"))
                db.write_bytes(b"replacement")
                with self.assertRaisesRegex(ValueError, "readiness"):
                    session_files.ensure_session(folder, ident, no_login)
                self.assertEqual(json.loads(marker.read_text()), expected)
                db.with_suffix(".old").replace(db)
                with session_files.prepare_ready(folder, ident):
                    db.write_bytes(b"normal retry update")
                self.assertTrue(session_files.is_ready(folder, ident))
                self.assertEqual((folder / "session.json").stat().st_mode & 0o777, 0o600)

    def test_first_preparation_abrupt_exit_never_adopts_unbound_store(self):
        ident = "@ghost:example.org"
        session = {"user_id": ident, "device_id": "D", "access_token": "fixture"}
        script = """
import os, sys
from pathlib import Path
import session_files
folder = Path(sys.argv[1])
with session_files.identity_lock(folder):
    with session_files.prepare_ready(folder, sys.argv[2]):
        if sys.argv[3] == "store":
            db = folder / "crypto/matrix-sdk-crypto.sqlite3"
            db.parent.mkdir()
            db.write_bytes(b"unbound store")
        os._exit(17)
"""
        for phase in ("empty", "store"):
            with self.subTest(phase=phase), TemporaryDirectory() as tmp:
                folder = Path(tmp) / "identity"
                with session_files.identity_lock(folder):
                    session_files.ensure_session(folder, ident, lambda: session)
                result = subprocess.run([sys.executable, "-c", script, str(folder), ident, phase],
                    env={**os.environ, "PYTHONPATH": str(DEPLOY / "worker")}, check=False)
                self.assertEqual(result.returncode, 17)
                marker = folder / "matrixrtc-ready-v1"
                before = marker.read_bytes()
                self.assertEqual(json.loads(before), {"user_id": ident, "device_id": "D",
                    "store": None, "ready": False})
                self.assertFalse(session_files.is_ready(folder, ident))
                with session_files.identity_lock(folder):
                    if phase == "store":
                        with self.assertRaisesRegex(ValueError, "readiness"):
                            session_files.ensure_session(folder, ident, lambda: self.fail("unexpected login"))
                        self.assertEqual((folder / "crypto/matrix-sdk-crypto.sqlite3").read_bytes(), b"unbound store")
                    else:
                        self.assertEqual(session_files.ensure_session(folder, ident, lambda: self.fail("unexpected login")), session)
                self.assertEqual(marker.read_bytes(), before)

    def test_first_preparation_intent_rejects_other_recovery_evidence(self):
        ident = "@ghost:example.org"
        session = {"user_id": ident, "device_id": "D", "access_token": "fixture"}
        for artifact in ("recovery-key.txt", "matrixrtc-ready", "crypto/partial"):
            with self.subTest(artifact=artifact), TemporaryDirectory() as tmp:
                folder = Path(tmp) / "identity"
                with session_files.identity_lock(folder):
                    session_files.ensure_session(folder, ident, lambda: session)
                    with self.assertRaises(RuntimeError):
                        with session_files.prepare_ready(folder, ident):
                            raise RuntimeError("failed")
                    marker = folder / "matrixrtc-ready-v1"
                    before = marker.read_bytes()
                    path = folder / artifact
                    path.parent.mkdir(exist_ok=True)
                    path.write_bytes(b"preserve")
                    with self.assertRaisesRegex(ValueError, "crypto store"):
                        session_files.ensure_session(folder, ident, lambda: self.fail("unexpected login"))
                    self.assertFalse(session_files.is_ready(folder, ident))
                    self.assertEqual(marker.read_bytes(), before)
                    self.assertEqual(path.read_bytes(), b"preserve")

    def test_auto_provision_readiness_failure_is_per_identity(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            bad = "@whatsapp_bad:example.org"
            good = "@whatsapp_good:example.org"
            bot = "@whatsappbot:example.org"
            scope = automatic["scan_once"].__globals__
            def folder(ghost):
                return root / "identities" / hashlib.sha256(ghost.encode()).hexdigest()
            bad_folder = folder(bad)
            with session_files.identity_lock(bad_folder):
                session_files.ensure_session(bad_folder, bad, lambda: {
                    "user_id": bad, "device_id": "D", "access_token": "fixture"})
                # A directory marker is invalid and may not be overwritten.
                (bad_folder / "matrixrtc-ready-v1").mkdir()
            self.assertFalse(session_files.is_ready(bad_folder, bad))
            with self.assertRaises(ValueError):
                session_files.ensure_session(bad_folder, bad, lambda: self.fail("unexpected login"))
            self.assertTrue((bad_folder / "matrixrtc-ready-v1").is_dir())
            processed = []
            def ensure(ghost, token):
                processed.append(ghost)
                if ghost == bad:
                    raise OSError("fixture identity unavailable")
                return ghost, True, "provisioned"
            with mock.patch.dict(scope, {
                "SERVER": "example.org", "STATE_ROOT": root,
                "resolve_metadata": lambda *args: {"as_token": "fixture", "bot_localpart": "whatsappbot", "ghost_regexes": []},
                "api": lambda *args: (200, {"user_id": bot}),
                "eligible_ghosts": lambda *args: [bad, good],
                "ensure_ghost": ensure,
                "log": mock.Mock(),
            }):
                self.assertFalse(scope["ghost_is_ready"](bad))
                self.assertEqual(scope["scan_once"](), 1)
                self.assertCountEqual(processed, [bot, bad, good])
                self.assertTrue(any("failed for " + bad in call.args[0]
                                    for call in scope["log"].call_args_list))

    def test_auto_provision_bounded_admission_prioritises_bot(self):
        scope = automatic["scan_once"].__globals__
        bot = "@whatsappbot:example.org"
        ghosts = [f"@whatsapp_{i}:example.org" for i in range(20)] + [bot]
        submitted = []
        widths = []
        real_executor = scope["ThreadPoolExecutor"]
        real_wait = scope["wait"]

        class RecordingExecutor(real_executor):
            def submit(self, fn, ghost, token):
                submitted.append(ghost)
                return super().submit(fn, ghost, token)

        def observe_wait(futures, **kwargs):
            widths.append(len(futures))
            if len(widths) == 1:
                self.assertEqual(submitted, ghosts[:3])
            return real_wait(futures, **kwargs)

        prepared = []

        def ensure(ghost, token):
            prepared.append(ghost)
            if ghost == ghosts[2]:
                raise OSError("fixture failure")
            return ghost, True, "provisioned"

        with mock.patch.dict(scope, {
            "SERVER": "example.org", "JOBS": 3,
            "resolve_metadata": lambda *args: {"as_token": "fixture", "bot_localpart": "whatsappbot", "ghost_regexes": []},
            "api": lambda *args: (200, {"user_id": bot}),
            "eligible_ghosts": lambda *args: ghosts,
            "ghost_is_ready": lambda ghost: False,
            "ensure_ghost": ensure, "log": mock.Mock(),
            "ThreadPoolExecutor": RecordingExecutor, "wait": observe_wait,
        }):
            self.assertEqual(scope["scan_once"](), 1)
        self.assertCountEqual(submitted, ghosts[:-1])
        self.assertCountEqual(prepared, ghosts)
        self.assertEqual(prepared[0], bot)
        self.assertTrue(widths)
        self.assertLessEqual(max(widths), 3)

    def test_auto_provision_bot_before_failed_discovery(self):
        scope = automatic["scan_once"].__globals__
        bot = "@whatsappbot:example.org"
        order = []

        def ensure(ghost, token):
            order.append(ghost)
            return ghost, True, "provisioned"

        def discovery(*args):
            order.append("discovery")
            raise OSError("fixture discovery failure")

        with mock.patch.dict(scope, {
            "SERVER": "example.org",
            "resolve_metadata": lambda *args: {"as_token": "fixture", "bot_localpart": "whatsappbot", "ghost_regexes": []},
            "api": lambda *args: (200, {"user_id": bot}),
            "eligible_ghosts": discovery,
            "ghost_is_ready": lambda ghost: False,
            "ensure_ghost": ensure, "log": mock.Mock(),
        }):
            self.assertEqual(scope["scan_once"](), 1)
        self.assertEqual(order, [bot, "discovery"])

    def test_auto_provision_room_read_failure_does_not_abort_scan(self):
        scope = automatic["eligible_ghosts"].__globals__
        bot = "@whatsappbot:example.org"
        ghost = "@whatsapp_good:example.org"
        bad, transient, good = "!bad:example.org", "!transient:example.org", "!good:example.org"
        from urllib import parse, error
        def api(path, token, as_user):
            if path.endswith("/joined_rooms"):
                return 200, {"joined_rooms": [bad, transient, good]}
            if parse.quote(bad, safe="") in path:
                raise error.URLError("fixture timeout")
            if path.endswith("/joined_members"):
                return 200, {"joined": {bot: {}, ghost: {}, "@alice:example.org": {}}}
            if path.endswith("/io.element.functional_members/"):
                return 200, {"service_members": [bot]}
            if path.endswith("/m.room.encryption/"):
                if parse.quote(transient, safe="") in path:
                    return 503, {}
                return 200, {"algorithm": "m.megolm.v1.aes-sha2"}
            if path.endswith("/state"):
                return 200, [{"type": "m.bridge", "sender": bot, "content": {
                    "bridgebot": bot, "protocol": {"id": "whatsapp"},
                    "channel": {"id": "123@s.whatsapp.net"}, "com.beeper.room_type": "dm"}}]
            self.fail(path)
        save = mock.Mock()
        with mock.patch.dict(scope, {"SERVER": "example.org", "api": api,
                                     "load_room_cache": lambda: {}, "save_room_cache": save,
                                     "log": mock.Mock()}):
            patterns = [scope["username_template_pattern"]("whatsapp_{{.}}", "example.org")]
            self.assertCountEqual(scope["eligible_ghosts"]("fixture", bot, patterns), [bot, ghost])
            self.assertEqual(set(save.call_args.args[0]), {good})
            save.side_effect = OSError("fixture cache unavailable")
            self.assertCountEqual(scope["eligible_ghosts"]("fixture", bot, patterns), [bot, ghost])

    def test_auto_provision_malformed_room_response_is_retryable(self):
        scope = automatic["eligible_ghosts"].__globals__
        bot = "@whatsappbot:example.org"
        ghost = "@whatsapp_good:example.org"
        from urllib import parse
        fixtures = {
            "members": ("/joined_members", {"joined": []}),
            "hints": ("/io.element.functional_members/", {"service_members": [None]}),
            "encryption": ("/m.room.encryption/", []),
            "state": ("/state", [{"type": "m.bridge", "sender": bot, "content": []}]),
            "channel": ("/state", [{"type": "m.bridge", "sender": bot, "content": {"channel": 5}}]),
        }
        rooms = {name: "!" + name + ":example.org" for name in fixtures}
        good = "!good:example.org"
        def api(path, token, as_user):
            if path.endswith("/joined_rooms"):
                return 200, {"joined_rooms": [*rooms.values(), good, good]}
            for name, (suffix, value) in fixtures.items():
                if parse.quote(rooms[name], safe="") in path and path.endswith(suffix):
                    return 200, value
            if path.endswith("/joined_members"):
                return 200, {"joined": {bot: {}, ghost: {}, "@alice:example.org": {}}}
            if path.endswith("/io.element.functional_members/"):
                return 200, {"service_members": [bot]}
            if path.endswith("/m.room.encryption/"):
                return 200, {"algorithm": "m.megolm.v1.aes-sha2"}
            if path.endswith("/state"):
                return 200, [{"type": "m.bridge", "sender": bot, "content": {
                    "bridgebot": bot, "protocol": {"id": "whatsapp"},
                    "channel": {"id": "123@s.whatsapp.net"}, "com.beeper.room_type": "dm"}}]
            self.fail(path)
        save = mock.Mock()
        call = mock.Mock(side_effect=api)
        with mock.patch.dict(scope, {"SERVER": "example.org", "api": call,
                                     "load_room_cache": lambda: {}, "save_room_cache": save,
                                     "log": mock.Mock()}):
            patterns = [scope["username_template_pattern"]("whatsapp_{{.}}", "example.org")]
            self.assertCountEqual(scope["eligible_ghosts"]("fixture", bot, patterns), [bot, ghost])
            self.assertEqual(set(save.call_args.args[0]), {good})
            self.assertEqual(sum(parse.quote(good, safe="") in c.args[0] and
                                 c.args[0].endswith("/joined_members") for c in call.call_args_list), 1)
            call.side_effect = None
            call.return_value = (200, {"joined_rooms": "invalid"})
            with self.assertRaises(scope["InvalidRoomResponse"]):
                scope["eligible_ghosts"]("fixture", bot, patterns)
            self.assertEqual(save.call_count, 1)

    def test_auto_provision_cache_rejects_invalid_and_future_timestamps(self):
        scope = automatic["eligible_ghosts"].__globals__
        bot = "@whatsappbot:example.org"
        ghost = "@whatsapp_good:example.org"
        room = "!good:example.org"
        now = 1000
        with TemporaryDirectory() as tmp:
            cache_file = Path(tmp) / "cache.json"
            cache = {room: {"checked_at": now + 3600, "ghost": ""},
                     "!valid:example.org": {"checked_at": now, "ghost": ghost}}
            for index, value in enumerate((float("nan"), float("inf"), -1, True, 1.5, "1000")):
                cache[f"!bad{index}:example.org"] = {"checked_at": value, "ghost": ""}
            cache_file.write_text(json.dumps(cache))
            def api(path, token, as_user):
                if path.endswith("/joined_rooms"):
                    return 200, {"joined_rooms": [room]}
                if path.endswith("/joined_members"):
                    return 200, {"joined": {bot: {}, ghost: {}, "@alice:example.org": {}}}
                if path.endswith("/io.element.functional_members/"):
                    return 200, {"service_members": [bot]}
                if path.endswith("/m.room.encryption/"):
                    return 200, {"algorithm": "m.megolm.v1.aes-sha2"}
                if path.endswith("/state"):
                    return 200, [{"type": "m.bridge", "sender": bot, "content": {
                        "bridgebot": bot, "protocol": {"id": "whatsapp"},
                        "channel": {"id": "123@s.whatsapp.net"}, "com.beeper.room_type": "dm"}}]
                self.fail(path)
            save = mock.Mock()
            with mock.patch.dict(scope, {"CACHE_FILE": cache_file, "SERVER": "example.org",
                                         "api": api, "save_room_cache": save}), \
                    mock.patch.object(scope["time"], "time", return_value=now):
                self.assertEqual(set(scope["load_room_cache"]()), {room, "!valid:example.org"})
                patterns = [scope["username_template_pattern"]("whatsapp_{{.}}", "example.org")]
                self.assertCountEqual(scope["eligible_ghosts"]("fixture", bot, patterns), [bot, ghost])
                self.assertEqual(save.call_args.args[0], {room: {"checked_at": now, "ghost": ghost}})
                cache_file.write_bytes(b"\xff")
                self.assertEqual(scope["load_room_cache"](), {})
                cache_file.write_text("{" + "\"nested\":" + "[" * 2000 + "]" * 2000 + "}")
                self.assertEqual(scope["load_room_cache"](), {})

    def test_surviving_credentials(self):
        with TemporaryDirectory() as tmp:
            root=Path(tmp)/"mautrix-whatsapp-call"
            dest=root/"generated";dest.mkdir(parents=True)
            (dest/"matrixrtc-registration.yaml").write_text("as_token: map-value\nhs_token: server-value\n")
            reconcile(root,"example.org")
            self.assertEqual((dest/"as_token").read_text().strip(),"map-value")
            p=dest/"matrixrtc_store_passphrase";p.unlink()
            crypto=root/"worker-state/identities/example/crypto";crypto.mkdir(parents=True)
            (crypto/"matrix-sdk-crypto.sqlite3").touch()
            with self.assertRaisesRegex(ValueError,"passphrase"):reconcile(root,"example.org")

    def test_strict_yaml(self):
        with self.assertRaisesRegex(ValueError, "duplicate"):
            config_files.load("network: {enable: true, enable: false}")
        self.assertEqual(config_files.load("base: &b {enabled: false}\nnetwork: {<<: *b, enabled: true}")["network"]["enabled"], True)
        with self.assertRaisesRegex(ValueError, "invalid YAML configuration"):
            config_files.load("secret: [unterminated")

    def test_hostname_render_is_bounded_and_idempotent(self):
        render = runpy.run_path(str(DEPLOY / "render-hostname.py"))
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name in render["TEMPLATES"]:
                shutil.copyfile(DEPLOY/name, root/name)
            for name in ("generated", "worker-state"):
                (root/name).mkdir()
                (root/name/"private.txt").write_text(render["PLACEHOLDER"])
            (root/"private.txt").write_text(render["PLACEHOLDER"])
            self.assertEqual(len(render["render"](root, "example.org")), 4)
            self.assertEqual(render["render"](root, "example.org"), [])
            for name in ("generated/private.txt", "worker-state/private.txt", "private.txt"):
                self.assertEqual((root/name).read_text(),render["PLACEHOLDER"])
            # A failed invocation cannot partially render another template.
            shutil.copyfile(DEPLOY/"synapse-additions.yaml",root/"synapse-additions.yaml")
            before = (root/"synapse-additions.yaml").read_bytes()
            with self.assertRaisesRegex(ValueError, "hostname mismatch"):
                render["render"](root,"other.example.org")
            self.assertEqual((root/"synapse-additions.yaml").read_bytes(),before)

    def test_prepare_credentials_quote_safely_and_survive_rerun(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)/"mautrix-whatsapp-call"
            root.mkdir()
            for name in ("prepare.sh", "config_files.py", "reconcile-credentials.py"):
                shutil.copyfile(DEPLOY/name,root/name)
            generated=root/"generated"
            generated.mkdir()
            special = "token" + chr(34) + chr(92) + ": #fixture"
            (generated/"as_token").write_text(special+"\n")
            (generated/"hs_token").write_text("yes\n")
            env={**os.environ,"MATRIXRTC_SERVER_NAME":"example.org","PYTHONDONTWRITEBYTECODE":"1"}
            for _ in range(2):
                subprocess.run(["bash",str(root/"prepare.sh")],env=env,check=True,capture_output=True)
                data=yaml.safe_load((generated/"matrixrtc-registration.yaml").read_text())
                self.assertEqual(data["as_token"],special)
                self.assertEqual(data["hs_token"],"yes")
                files={p.name:p.read_bytes() for p in generated.iterdir() if p.is_file()}
                if _ == 0: previous=files
                else: self.assertEqual(files,previous)
                for p in generated.iterdir(): self.assertEqual(p.stat().st_mode&0o777,0o600)
            self.assertEqual(generated.stat().st_mode&0o777,0o700)
            (generated/"as_token").unlink()
            outside=Path(tmp)/"outside"
            outside.write_text(special)
            (generated/"as_token").symlink_to(outside)
            with self.assertRaisesRegex(ValueError,"symbolic link"):
                reconcile(root,"example.org")

    def test_prepare_recovers_after_interrupted_promotion(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)/"mautrix-whatsapp-call"
            root.mkdir()
            for name in ("prepare.sh", "config_files.py", "reconcile-credentials.py"):
                shutil.copyfile(DEPLOY/name, root/name)
            generated = root/"generated"
            generated.mkdir(mode=0o700)
            fake = Path(tmp)/"fake"
            fake.mkdir()
            mover = fake/"mv"
            real_mv = shutil.which("mv")
            # Kill the actual preparation shell immediately after its first
            # promotion, bypassing EXIT cleanup as on process interruption.
            mover.write_text('#!/bin/bash\n"'+real_mv+'" "$@" || exit $?\nkill -KILL "$PPID"\n')
            mover.chmod(0o755)
            env = {**os.environ, "MATRIXRTC_SERVER_NAME":"example.org", "PYTHONDONTWRITEBYTECODE":"1"}
            interrupted = subprocess.run(["bash", str(root/"prepare.sh")],
                env={**env,"PATH":str(fake)+os.pathsep+env["PATH"]}, capture_output=True, timeout=10)
            self.assertEqual(interrupted.returncode, -9, interrupted.stderr)
            self.assertTrue(list(generated.glob(".render.*")))
            credentials = ("as_token", "hs_token", "double_puppet_as_token", "double_puppet_hs_token", "livekit_secret", "matrixrtc_store_passphrase")
            saved = {name:(generated/name).read_bytes() for name in credentials}
            self.assertTrue(all(saved.values()))
            for _ in range(2):
                subprocess.run(["bash", str(root/"prepare.sh")], env=env, check=True, capture_output=True, timeout=10)
                self.assertEqual({name:(generated/name).read_bytes() for name in credentials}, saved)
                reg = yaml.safe_load((generated/"matrixrtc-registration.yaml").read_text())
                self.assertEqual(reg["as_token"], saved["as_token"].decode().strip())
                self.assertEqual(reg["hs_token"], saved["hs_token"].decode().strip())
                self.assertEqual(yaml.safe_load((generated/"livekit.yaml").read_text())["keys"]["lk-matrixrtc"], saved["livekit_secret"].decode().strip())
                self.assertEqual((generated/"double_puppet_secret").read_text(), "as_token:"+saved["double_puppet_as_token"].decode())
            target = generated/"livekit.yaml"
            target.unlink()
            target.mkdir()
            result = subprocess.run(["bash", str(root/"prepare.sh")], env=env, capture_output=True, timeout=10)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(list(target.iterdir()), [])
            self.assertEqual({name:(generated/name).read_bytes() for name in credentials}, saved)

    def test_call_service_stop_is_bounded_and_requires_stopped_containers(self):
        script = (DEPLOY/"install-or-upgrade.sh").read_text()
        guard = script[script.index("stop_call_services()") : script.index("restore_worker()")]
        self.assertLess(script.index('stop_call_services || { echo "Cannot confirm'), script.index('bash "$RTC_ROOT/prepare.sh"'))
        fixture = r"""timeout() {
    [[ "$1" == --kill-after=5s ]] || return 97
    local limit="$2"
    shift 2
    [[ "$1" == docker ]] || return 97
    shift
    case "$*" in
        "compose -f docker-compose.yml -f mautrix-whatsapp-call/compose.yml stop mautrix-whatsapp-call-worker mautrix-whatsapp-call-provisioner")
            [[ "$limit" == 75s && "$MATRIXRTC_ENABLE_CALLS" == 0 ]] || return 97
            echo stop >> "$TRACE"; return "$STOP_RC" ;;
        "compose -f docker-compose.yml -f mautrix-whatsapp-call/compose.yml ps --all --quiet mautrix-whatsapp-call-worker mautrix-whatsapp-call-provisioner")
            [[ "$limit" == 10s && "$MATRIXRTC_ENABLE_CALLS" == 0 ]] || return 97
            echo list >> "$TRACE"; printf '%s\n' "$IDS"; return "$LIST_RC" ;;
        "inspect --format {{.State.Status}} "*)
            [[ "$limit" == 10s ]] || return 97
            echo inspect >> "$TRACE"; printf '%s\n' "$STATUSES"; return "$INSPECT_RC" ;;
        *) return 97 ;;
    esac
}
"""
        ids = "0123456789ab\nabcdef123456"
        cases = [
            ("absent", "", "", 0, 0, 0, True, "stop\nlist\n"),
            ("stopped", ids, "exited\ncreated", 0, 0, 0, True, "stop\nlist\ninspect\n"),
            ("running", ids, "exited\nrunning", 0, 0, 0, False, "stop\nlist\ninspect\n"),
            ("restarting", ids, "exited\nrestarting", 0, 0, 0, False, "stop\nlist\ninspect\n"),
            ("partial", ids, "exited", 0, 0, 0, False, "stop\nlist\ninspect\n"),
            ("timeout", ids, "exited\ncreated", 124, 0, 0, False, "stop\n"),
            ("listing timeout", ids, "exited\ncreated", 0, 124, 0, False, "stop\nlist\n"),
            ("inspect error", ids, "exited\ncreated", 0, 0, 1, False, "stop\nlist\ninspect\n"),
            ("invalid id", "--bad", "exited", 0, 0, 0, False, "stop\nlist\n"),
        ]
        with TemporaryDirectory() as tmp:
            trace = Path(tmp)/"trace"
            for label, found, statuses, stop_rc, list_rc, inspect_rc, ok, expected in cases:
                with self.subTest(label=label):
                    trace.write_text("")
                    env = {**os.environ, "TRACE":str(trace), "IDS":found, "STATUSES":statuses,
                           "STOP_RC":str(stop_rc), "LIST_RC":str(list_rc), "INSPECT_RC":str(inspect_rc)}
                    result = subprocess.run(["bash", "-ec", fixture+guard+"\nstop_call_services"],
                                            env=env, capture_output=True, timeout=5)
                    self.assertEqual(result.returncode == 0, ok, result.stderr)
                    self.assertEqual(trace.read_text(), expected)

    def test_preflight_parses_actual_yaml_not_indent(self):
        with TemporaryDirectory() as tmp:
            matrix=Path(tmp)
            root=matrix/"mautrix-whatsapp-call"
            (root/"worker").mkdir(parents=True)
            (root/"bridge").mkdir()
            (root/"generated").mkdir()
            (matrix/"mautrix-whatsapp").mkdir()
            (matrix/"docker-compose.yml").write_text("services: {}\n")
            shutil.copytree(DEPLOY,root,dirs_exist_ok=True,ignore=shutil.ignore_patterns("__pycache__"))
            for name in ("worker/mautrix-whatsapp-call-worker","worker/mautrix-whatsapp-call-provision",
                         "bridge/mautrix-whatsapp-call-bridge"):
                (root/name).write_bytes(fixture_elf_header())
            passphrase=root/"generated/matrixrtc_store_passphrase"
            passphrase.write_text("fixture")
            passphrase.chmod(0o600)
            config=matrix/"mautrix-whatsapp/config.yaml"
            config.write_text(configure("network: {}\n","example.org","as_token:fixture"))
            fake=matrix/"fake-bin";fake.mkdir(exist_ok=True)
            docker=fake/"docker"
            docker.write_text("""#!/bin/bash
[[ "$PWD" == "$FIXTURE_ROOT" && "$1" == compose ]] || exit 99
shift
if [[ "$*" == version ]]; then exit "$VERSION_RC"; fi
if [[ "$*" == "-f $FIXTURE_ROOT/docker-compose.yml config --services" ]]; then
    printf '%s\\n' "$BASE_SERVICES"
    exit "$BASE_RC"
fi
if [[ "$*" == "-f $FIXTURE_ROOT/docker-compose.yml -f $FIXTURE_ROOT/mautrix-whatsapp-call/compose.yml config --quiet" ]]; then
    exit "$MERGED_RC"
fi
exit 99
""");docker.chmod(0o755)
            env={**os.environ,"PATH":str(fake)+os.pathsep+os.environ["PATH"],
                 "FIXTURE_ROOT":str(matrix),"VERSION_RC":"0","BASE_RC":"0","MERGED_RC":"0",
                 "BASE_SERVICES":"synapse\nmautrix-whatsapp"}
            def check(ok, **overrides):
                result=subprocess.run(["bash",str(root/"preflight.sh")],cwd=fake,
                                      capture_output=True,text=True,env={**env,**overrides})
                self.assertEqual(result.returncode==0,ok,result.stdout+result.stderr)
                self.assertEqual("local preflight: PASS" in result.stdout,ok)
            check(True)
            check(False,VERSION_RC="1")
            check(False,BASE_RC="1")
            check(False,BASE_SERVICES="synapse\nmautrix-whatsapp-old")
            check(False,BASE_SERVICES="mautrix-whatsapp")
            check(False,MERGED_RC="1")
            (root/"bridge/mautrix-whatsapp-call-bridge").unlink()
            check(False)
            (root/"bridge/mautrix-whatsapp-call-bridge").write_bytes(fixture_elf_header())
            config.write_text("network: {experimental_whatsapp_voip: false}\n")
            check(False)

    def test_payload_check_covers_docker_copy_and_installer_helpers(self):
        script=(DEPLOY/"install-or-upgrade.sh").read_text()
        self.assertLess(script.index('bash "$RTC_ROOT/preflight.sh" --payload-only'),
                        script.index("WORKER_PAUSED=1\n"))
        with TemporaryDirectory() as tmp:
            root=Path(tmp)/"mautrix-whatsapp-call"
            shutil.copytree(DEPLOY,root,ignore=shutil.ignore_patterns("__pycache__"))
            inputs=[]
            for directory in ("bridge","worker"):
                for line in (root/directory/"Dockerfile").read_text().splitlines():
                    if line.startswith("COPY "):
                        inputs.append(root/directory/line.split()[1])
            for path in inputs:
                if not path.exists():
                    path.write_bytes(fixture_elf_header())
            def check(ok,path=None):
                result=subprocess.run(["bash",str(root/"preflight.sh"),"--payload-only"],
                                      cwd=tmp,capture_output=True,text=True)
                self.assertEqual(result.returncode==0,ok,result.stdout+result.stderr)
                self.assertEqual("payload presence: PASS" in result.stdout,ok)
                if path is not None:
                    self.assertIn(path.relative_to(root).as_posix(),result.stderr)
            check(True)
            self.assertFalse((root/"generated").exists())
            for path in inputs+[root/"verify-worker.sh",root/"prepare.sh",root/"render-hostname.py"]:
                with self.subTest(path=path.name):
                    original=path.read_bytes()
                    path.unlink()
                    check(False,path)
                    path.write_bytes(b"")
                    check(False,path)
                    path.write_bytes(original)
            check(True)
            for path in (root/"bridge/mautrix-whatsapp-call-bridge",
                         root/"worker/mautrix-whatsapp-call-worker",
                         root/"worker/mautrix-whatsapp-call-provision"):
                original = path.read_bytes()
                for offset, value in ((0, 0), (4, 1), (5, 2), (6, 0),
                                      (16, 1), (18, 183), (20, 0), (52, 0)):
                    invalid = bytearray(original)
                    invalid[offset] = value
                    path.write_bytes(invalid)
                    check(False, path)
                path.write_bytes(original[:20])
                check(False, path)
                path.unlink()
                target = Path(tmp)/"external-binary"
                target.write_bytes(original)
                path.symlink_to(target)
                check(False, path)
                path.unlink()
                path.write_bytes(original)
                # Both conventional executables and PIE binaries are accepted.
                pie = bytearray(original)
                pie[16] = 3
                path.write_bytes(pie)
                check(True)
                path.write_bytes(original)
            check(True)

    def test_docker_copy_inputs_are_in_context_allowlist(self):
        for directory in ("bridge", "worker"):
            context = DEPLOY / directory
            rules = (context / ".dockerignore").read_text().splitlines()
            self.assertEqual(rules[0], "**")
            # The shipped Dockerfiles use literal single-source COPY instructions.
            for line in (context / "Dockerfile").read_text().splitlines():
                if line.startswith("COPY "):
                    source = line.split()[1]
                    with self.subTest(directory=directory, source=source):
                        self.assertIn("!" + source, rules)

    def test_image_build_failure_does_not_mutate_existing_stack(self):
        script = (DEPLOY / "install-or-upgrade.sh").read_text()
        build = script.index("\ncompose build mautrix-whatsapp-call-worker")
        self.assertLess(script.index('python3 -B "$RTC_ROOT/validate-candidate.py"'), build)
        for mutation in ("\nnormalize_deployment_permissions\n", "WORKER_PAUSED=1\n",
                         'bash "$RTC_ROOT/prepare.sh"', 'python3 "$RTC_ROOT/configure-bridge.py"'):
            self.assertLess(build, script.index(mutation))
        self.assertEqual(script.count("build mautrix-whatsapp-call-worker"), 1)
        with TemporaryDirectory() as tmp:
            matrix = Path(tmp)
            root = matrix / "mautrix-whatsapp-call"
            shutil.copytree(DEPLOY, root, ignore=shutil.ignore_patterns("__pycache__"))
            for rel in ("bridge/mautrix-whatsapp-call-bridge", "worker/mautrix-whatsapp-call-worker",
                        "worker/mautrix-whatsapp-call-provision"):
                (root / rel).write_bytes(fixture_elf_header())
            (matrix / "synapse").mkdir()
            (matrix / "mautrix-whatsapp").mkdir()
            (matrix / "synapse/homeserver.yaml").write_text("server_name: example.org\napp_service_config_files: []\n")
            (matrix / "mautrix-whatsapp/config.yaml").write_text("network: {}\n")
            (matrix / "mautrix-whatsapp/registration.yaml").write_text("id: fixture\n")
            (matrix / "docker-compose.yml").write_text("services: {}\n")
            (matrix / ".env").write_text("MATRIXRTC_ENABLE_CALLS=1\n")
            volume = "mautrix-whatsapp-call-worker-socket"
            socket = {"type": "volume", "source": volume, "target": "/run/mautrix-whatsapp-call"}
            bridge_data = {"type": "bind", "source": str(matrix / "mautrix-whatsapp"), "target": "/data"}
            synapse_data = {"type": "bind", "source": str(matrix / "synapse"), "target": "/data"}
            base = {"services": {"mautrix-whatsapp": {"volumes": [bridge_data]},
                                 "synapse": {"volumes": [synapse_data]}}}
            merged = {"services": {"mautrix-whatsapp": {"volumes": [bridge_data, socket]},
                        "synapse": {"volumes": [synapse_data]},
                        "mautrix-whatsapp-call-worker": {"volumes": [socket]},
                        "lk-jwt-service": {"image": "ghcr.io/element-hq/lk-jwt-service:0.9.0"}},
                      "volumes": {volume: {}}}
            fixture_call_bindings(merged, matrix)
            docker = matrix / "docker"
            docker.write_text("""#!/bin/bash
case "$*" in
    "compose -f docker-compose.yml config --services") printf 'synapse\\nmautrix-whatsapp\\n' ;;
    "compose -f docker-compose.yml config --format json") printf '%s' "$BASE_MODEL" ;;
    "compose -f docker-compose.yml -f mautrix-whatsapp-call/compose.yml config --format json") printf '%s' "$MERGED_MODEL" ;;
    "compose -f docker-compose.yml -f mautrix-whatsapp-call/compose.yml ps --all --quiet lk-jwt-service") ;;
    "image inspect --format {{.Id}} ghcr.io/element-hq/lk-jwt-service:0.9.0") printf "sha256:%064d\\n" 0 ;;
    "compose -f docker-compose.yml -f mautrix-whatsapp-call/compose.yml build mautrix-whatsapp-call-worker mautrix-whatsapp-call-provisioner mautrix-whatsapp")
        if [[ "${FIXTURE_ROLLBACK:-0}" == 1 ]]; then exit 0; fi
        if [[ "${FIXTURE_EDIT:-0}" == 1 ]]; then
            printf '\n# changed during build\n' >> "$MATRIX_ROOT/mautrix-whatsapp/config.yaml"
            exit 0
        fi
        echo 'fixture build failed' >&2; exit 37 ;;
    "compose -f docker-compose.yml -f mautrix-whatsapp-call/compose.yml stop mautrix-whatsapp-call-worker mautrix-whatsapp-call-provisioner")
        [[ "${FIXTURE_ROLLBACK:-0}" == 1 ]] || exit 99
        printf 'stop\\n' >> "$MATRIX_ROOT/operations.log" ;;
    "compose -f docker-compose.yml -f mautrix-whatsapp-call/compose.yml ps --all --quiet mautrix-whatsapp-call-worker mautrix-whatsapp-call-provisioner")
        [[ "${FIXTURE_ROLLBACK:-0}" == 1 ]] || exit 99 ;;
    "compose -f docker-compose.yml -f mautrix-whatsapp-call/compose.yml config")
        [[ "${FIXTURE_ROLLBACK:-0}" == 1 ]] || exit 99
        # Confirm that actual configuration writes happened before the injected failure.
        grep -q 'experimental_whatsapp_voip: true' "$MATRIX_ROOT/mautrix-whatsapp/config.yaml" || exit 98
        test -f "$MATRIX_ROOT/synapse/matrixrtc-registration.yaml" || exit 98
        printf 'applied\\n' >> "$MATRIX_ROOT/operations.log"
        echo 'fixture final compose validation failed' >&2; exit 38 ;;
    *) echo 'unexpected docker operation' >&2; exit 99 ;;
esac
""")
            docker.chmod(0o755)
            def snapshot():
                return {p.relative_to(matrix).as_posix(): (p.stat().st_mode, p.read_bytes() if p.is_file() else None)
                        for p in matrix.rglob("*") if p.name != ".mautrix-whatsapp-call-install.lock"}
            before = snapshot()
            result = subprocess.run(["bash", str(root / "install-or-upgrade.sh")],
                env={**os.environ, "MATRIX_ROOT": tmp, "MATRIXRTC_SERVER_NAME": "example.org",
                     "MATRIXRTC_WHATSAPP_REGISTRATION_PATH": str(matrix / "mautrix-whatsapp/registration.yaml"),
                     "PATH": tmp + os.pathsep + os.environ["PATH"],
                     "BASE_MODEL": json.dumps(base), "MERGED_MODEL": json.dumps(merged)},
                capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("fixture build failed", result.stderr)
            self.assertIn("no call services have been paused", result.stderr)
            self.assertNotIn("unexpected docker operation", result.stderr)
            self.assertEqual(snapshot(), before)
            # A successful build with an external, valid config change must
            # still fail before any service shutdown or permission changes.
            result = subprocess.run(["bash", str(root / "install-or-upgrade.sh")],
                env={**os.environ, "MATRIX_ROOT": tmp, "MATRIXRTC_SERVER_NAME": "example.org",
                     "MATRIXRTC_WHATSAPP_REGISTRATION_PATH": str(matrix / "mautrix-whatsapp/registration.yaml"),
                     "PATH": tmp + os.pathsep + os.environ["PATH"], "FIXTURE_EDIT": "1",
                     "BASE_MODEL": json.dumps(base), "MERGED_MODEL": json.dumps(merged)},
                capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Deployment inputs changed after validation", result.stderr)
            self.assertNotIn("unexpected docker operation", result.stderr)
            expected = dict(before)
            key = "mautrix-whatsapp/config.yaml"
            expected[key] = (before[key][0], before[key][1] + b"\n# changed during build\n")
            self.assertEqual(snapshot(), expected)
            # Exercise the real installer EXIT trap after live configuration
            # writes, with only Docker simulated. No actual containers or
            # network operations are used by this disposable fixture.
            bridge = matrix / "mautrix-whatsapp/config.yaml"
            hs = matrix / "synapse/homeserver.yaml"
            paths = (bridge, hs, matrix / ".env")
            before_live = [(p.read_bytes(), stat.S_IMODE(p.stat().st_mode), p.stat().st_uid, p.stat().st_gid) for p in paths]
            result = subprocess.run(["bash", str(root / "install-or-upgrade.sh")],
                env={**os.environ, "MATRIX_ROOT": tmp, "MATRIXRTC_SERVER_NAME": "example.org",
                     "MATRIXRTC_WHATSAPP_REGISTRATION_PATH": str(matrix / "mautrix-whatsapp/registration.yaml"),
                     "PATH": tmp + os.pathsep + os.environ["PATH"], "FIXTURE_ROLLBACK": "1",
                     "BASE_MODEL": json.dumps(base), "MERGED_MODEL": json.dumps(merged)},
                capture_output=True, text=True, timeout=60)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("fixture final compose validation failed", result.stderr)
            self.assertNotIn("unexpected docker operation", result.stderr)
            self.assertNotIn("CRITICAL", result.stderr)
            self.assertEqual((matrix / "operations.log").read_text().splitlines(), ["stop", "applied", "stop"])
            self.assertEqual([(p.read_bytes(), stat.S_IMODE(p.stat().st_mode), p.stat().st_uid, p.stat().st_gid) for p in paths], before_live)
            self.assertFalse((matrix / "synapse/matrixrtc-registration.yaml").exists())
            self.assertFalse((matrix / "synapse/matrixrtc-doublepuppet-registration.yaml").exists())
            self.assertFalse(list(matrix.glob(".matrixrtc-config-rollback.*")))
            self.assertTrue((root / "generated/matrixrtc_store_passphrase").is_file())

    def test_candidate_fingerprint_covers_presence_and_contents(self):
        fingerprint = runpy.run_path(str(DEPLOY / "validate-candidate.py"))["fingerprint"]
        with TemporaryDirectory() as tmp:
            root = Path(tmp) / "mautrix-whatsapp-call"
            root.mkdir()
            generated = root / "generated"
            generated.mkdir()
            registration = Path(tmp) / "registration.yaml"
            with mock.patch.dict(os.environ, {"MATRIXRTC_WHATSAPP_REGISTRATION_PATH": str(registration)}):
                original = fingerprint(root)
                self.assertEqual(fingerprint(root), original)
                for path in (Path(tmp) / ".env", registration, root / "compose.yml", generated / "as_token"):
                    path.write_bytes(b"")
                    self.assertNotEqual(fingerprint(root), original)
                    empty = fingerprint(root)
                    path.write_bytes(b"fixture")
                    self.assertNotEqual(fingerprint(root), empty)
                    path.unlink()
                    self.assertEqual(fingerprint(root), original)
                registration.symlink_to(root / "missing")
                with self.assertRaises(ValueError):
                    fingerprint(root)

    def test_candidate_fingerprint_covers_build_inputs(self):
        fingerprint = runpy.run_path(str(DEPLOY / "validate-candidate.py"))["fingerprint"]
        with TemporaryDirectory() as tmp:
            root = Path(tmp) / "mautrix-whatsapp-call"
            shutil.copytree(DEPLOY, root, ignore=shutil.ignore_patterns("__pycache__"))
            with mock.patch.dict(os.environ, {"MATRIXRTC_WHATSAPP_REGISTRATION_PATH": str(root / "absent")}):
                for directory, binary in (("bridge", "mautrix-whatsapp-call-bridge"),
                                          ("worker", "mautrix-whatsapp-call-worker"),
                                          ("worker", "mautrix-whatsapp-call-provision")):
                    (root / directory / binary).write_bytes(fixture_elf_header())
                baseline = fingerprint(root)
                for name in ("bridge/Dockerfile", "bridge/.dockerignore", "bridge/mautrix-whatsapp-call-bridge",
                             "worker/Dockerfile", "worker/.dockerignore", "worker/mautrix-whatsapp-call-worker",
                             "worker/mautrix-whatsapp-call-provision", "worker/portal_kind.py", "worker/ghost_namespace.py",
                             "worker/session_files.py", "worker/provision-ghost.py", "worker/auto-provision-whatsapp-ghosts.py",
                             "preflight.sh", "install-or-upgrade.sh", "validate-candidate.py", "config_files.py",
                             "configure-bridge.py", "merge-synapse.py", "prepare.sh", "reconcile-credentials.py", "render-hostname.py"):
                    with self.subTest(name=name):
                        path = root / name
                        original = path.read_bytes()
                        path.write_bytes(original + b"changed")
                        self.assertNotEqual(fingerprint(root), baseline)
                        path.write_bytes(original)
                        self.assertEqual(fingerprint(root), baseline)
                        path.unlink()
                        self.assertNotEqual(fingerprint(root), baseline)
                        path.symlink_to(root / "absent")
                        with self.assertRaises(ValueError):
                            fingerprint(root)
                        path.unlink()
                        path.write_bytes(original)
                        self.assertEqual(fingerprint(root), baseline)
                # A directory symlink must not substitute a complete payload.
                (root / "worker").rename(root / "worker-real")
                (root / "worker").symlink_to(root / "worker-real", target_is_directory=True)
                with self.assertRaises(ValueError):
                    fingerprint(root)

    def test_installer_requires_existing_base_services_before_pause(self):
        script=(DEPLOY/"install-or-upgrade.sh").read_text()
        start=script.index("verify_existing_base_services()")
        end=script.index("\nverify_existing_base_services\n",start)
        guard=script[start:end]
        self.assertLess(end,script.index("WORKER_PAUSED=1\n"))
        fixture="""docker() {
    [[ "$*" == "compose -f docker-compose.yml config --services" ]] || return 99
    printf '%s\\n' "$BASE_SERVICES"
    return "$BASE_RC"
}
"""
        for services,rc,ok in [("synapse\nmautrix-whatsapp",0,True),
                               ("mautrix-whatsapp\nsynapse\npostgres",0,True),
                               ("synapse\nmautrix-whatsapp",1,False),
                               ("synapse\nmautrix-whatsapp-old",0,False),
                               ("mautrix-whatsapp",0,False), ("",0,False)]:
            with self.subTest(services=services,rc=rc):
                result=subprocess.run(["bash","-ec",fixture+guard+"\nverify_existing_base_services"],
                                      env={**os.environ,"BASE_SERVICES":services,"BASE_RC":str(rc)},
                                      capture_output=True,text=True)
                self.assertEqual(result.returncode==0,ok,result.stderr)

    def test_call_mount_contract_precedes_mutation(self):
        script = (DEPLOY / "install-or-upgrade.sh").read_text()
        start = script.index("verify_call_mount_contract()")
        end = script.index("\nverify_call_mount_contract\n", start)
        self.assertLess(end, script.index("\nnormalize_deployment_permissions\n"))
        self.assertLess(end, script.index("WORKER_PAUSED=1\n"))
        guard = script[start:end] + "\nverify_call_mount_contract"
        volume = "mautrix-whatsapp-call-worker-socket"
        target = "/run/mautrix-whatsapp-call"
        data = {"type": "bind", "source": "/opt/matrix/mautrix-whatsapp",
                "target": "/data", "bind": {"create_host_path": True}}
        socket = {"type": "volume", "source": volume, "target": target}
        cache = {"type": "volume", "source": "bridge_data", "target": "/cache"}
        synapse_data = {"type": "bind", "source": "/opt/matrix/synapse", "target": "/data"}
        base = {"services": {"mautrix-whatsapp": {"volumes": [data, cache]},
                             "synapse": {"volumes": [synapse_data]}},
                "volumes": {"bridge_data": {"name": "existing_bridge_data"}}}
        merged = {"services": {"mautrix-whatsapp": {"volumes": [data, cache, socket]},
                               "synapse": {"volumes": [synapse_data]},
                               "mautrix-whatsapp-call-worker": {"volumes": [socket]}},
                  "volumes": {volume: {"name": "matrix_socket"},
                              "bridge_data": {"name": "existing_bridge_data"}}}
        fixture_call_bindings(merged, "/opt/matrix")
        with TemporaryDirectory() as tmp:
            docker = Path(tmp) / "docker"
            docker.write_text("""#!/bin/bash
case "$*" in
    "compose -f docker-compose.yml config --format json") printf '%s' "$BASE_MODEL"; exit "$BASE_RC" ;;
    "compose -f docker-compose.yml -f mautrix-whatsapp-call/compose.yml config --format json") printf '%s' "$MERGED_MODEL"; exit "$MERGED_RC" ;;
    *) exit 99 ;;
esac
""")
            docker.chmod(0o755)
            def check(model, ok, base_rc=0, merged_rc=0):
                result = subprocess.run(["bash", "-ec", guard], capture_output=True, text=True,
                    env={**os.environ, "PATH": tmp + os.pathsep + os.environ["PATH"],
                         "BASE_MODEL": json.dumps(base), "MERGED_MODEL": json.dumps(model),
                         "BASE_RC": str(base_rc), "MERGED_RC": str(merged_rc),
                         "MATRIX_ROOT": "/opt/matrix", "MATRIXRTC_SERVER_NAME": "example.org",
                         "MATRIXRTC_WHATSAPP_REGISTRATION_PATH": "/opt/matrix/mautrix-whatsapp/registration.yaml"})
                self.assertEqual(result.returncode == 0, ok, result.stderr)
                self.assertNotIn("fixture-secret", result.stdout + result.stderr)
            check(merged, True)
            check(merged, False, base_rc=1)
            check(merged, False, merged_rc=1)
            for change in ("lost_data", "changed_source", "changed_mode", "wrong_volume",
                           "read_only", "subpath", "duplicate", "missing_volume", "missing_worker",
                           "changed_volume_definition", "synapse_missing_data", "synapse_wrong_source",
                           "synapse_read_only", "synapse_shadow_registration", "bridge_shadow_config",
                           "synapse_named_volume", "synapse_shadow_homeserver",
                           "split_state", "readonly_state", "wrong_passphrase", "writable_passphrase",
                           "missing_provisioner", "wrong_registration", "writable_registration",
                           "wrong_provisioner_config", "wrong_livekit_config", "writable_jwt_secret",
                           "shadow_state_child", "shadow_secret_parent",
                           "missing_build", "wrong_context", "remote_context", "wrong_recipe",
                           "inline_recipe", "wrong_image", "wrong_platform", "multi_platform",
                           "target", "additional_contexts", "split_provisioner_build"):
                with self.subTest(change=change):
                    candidate = json.loads(json.dumps(merged))
                    bridge = candidate["services"]["mautrix-whatsapp"]["volumes"]
                    worker = candidate["services"]["mautrix-whatsapp-call-worker"]["volumes"]
                    if change == "lost_data": bridge.pop(0)
                    elif change == "changed_source": bridge[0]["source"] = "/fixture-secret"
                    elif change == "changed_mode": bridge[0]["read_only"] = True
                    elif change == "wrong_volume": worker[0]["source"] = "other"
                    elif change == "read_only": worker[0]["read_only"] = True
                    elif change == "subpath": worker[0]["volume"] = {"subpath": "other"}
                    elif change == "duplicate": bridge.append(bridge[0])
                    elif change == "missing_volume": del candidate["volumes"][volume]
                    elif change == "changed_volume_definition": candidate["volumes"]["bridge_data"]["name"] = "other"
                    elif change == "missing_worker": del candidate["services"]["mautrix-whatsapp-call-worker"]
                    elif change == "synapse_missing_data": candidate["services"]["synapse"]["volumes"].clear()
                    elif change == "synapse_wrong_source": candidate["services"]["synapse"]["volumes"][0]["source"] = "/fixture-secret"
                    elif change == "synapse_read_only": candidate["services"]["synapse"]["volumes"][0]["read_only"] = True
                    elif change == "synapse_named_volume": candidate["services"]["synapse"]["volumes"][0]["type"] = "volume"
                    elif change == "synapse_shadow_registration": candidate["services"]["synapse"]["volumes"].append({"type": "bind", "source": "/other", "target": "/data/matrixrtc-registration.yaml"})
                    elif change == "synapse_shadow_homeserver": candidate["services"]["synapse"]["volumes"].append({"type": "bind", "source": "/other", "target": "/data/homeserver.yaml"})
                    elif change == "bridge_shadow_config": bridge.append({"type": "bind", "source": "/other", "target": "/data/config.yaml"})
                    elif change == "split_state": worker[1]["source"] = "/fixture-secret"
                    elif change == "readonly_state": worker[1]["read_only"] = True
                    elif change == "wrong_passphrase": worker[2]["source"] = "/fixture-secret"
                    elif change == "writable_passphrase": worker[2]["read_only"] = False
                    elif change == "missing_provisioner": del candidate["services"]["mautrix-whatsapp-call-provisioner"]
                    elif change == "wrong_registration": candidate["services"]["mautrix-whatsapp-call-provisioner"]["volumes"][2]["source"] = "/fixture-secret"
                    elif change == "writable_registration": candidate["services"]["mautrix-whatsapp-call-provisioner"]["volumes"][2]["read_only"] = False
                    elif change == "wrong_provisioner_config": candidate["services"]["mautrix-whatsapp-call-provisioner"]["volumes"][3]["source"] = "/fixture-secret"
                    elif change == "wrong_livekit_config": candidate["services"]["livekit"]["volumes"][0]["source"] = "/fixture-secret"
                    elif change == "writable_jwt_secret": candidate["services"]["lk-jwt-service"]["volumes"][0]["read_only"] = False
                    elif change == "shadow_state_child": worker.append({"type": "bind", "source": "/other", "target": "/var/lib/mautrix-whatsapp-call/identities"})
                    elif change == "shadow_secret_parent": worker.append({"type": "bind", "source": "/other", "target": "/run/secrets"})
                    elif change == "missing_build": del candidate["services"]["mautrix-whatsapp"]["build"]
                    elif change == "wrong_context": candidate["services"]["mautrix-whatsapp"]["build"]["context"] = "/fixture-secret"
                    elif change == "remote_context": candidate["services"]["mautrix-whatsapp"]["build"]["context"] = "https://example.invalid/source"
                    elif change == "wrong_recipe": candidate["services"]["mautrix-whatsapp"]["build"]["dockerfile"] = "Old.Dockerfile"
                    elif change == "inline_recipe": candidate["services"]["mautrix-whatsapp"]["build"]["dockerfile_inline"] = "FROM scratch"
                    elif change == "wrong_image": candidate["services"]["mautrix-whatsapp"]["image"] = "old:local"
                    elif change == "wrong_platform": candidate["services"]["mautrix-whatsapp-call-worker"]["platform"] = "linux/arm64"
                    elif change == "multi_platform": candidate["services"]["mautrix-whatsapp-call-worker"]["build"]["platforms"] = ["linux/amd64", "linux/arm64"]
                    elif change == "target": candidate["services"]["mautrix-whatsapp-call-worker"]["build"]["target"] = "old"
                    elif change == "additional_contexts": candidate["services"]["mautrix-whatsapp-call-worker"]["build"]["additional_contexts"] = {"other": "/fixture-secret"}
                    elif change == "split_provisioner_build": candidate["services"]["mautrix-whatsapp-call-provisioner"]["build"]["context"] = "/fixture-secret"
                    check(candidate, False)

    def test_call_process_contract(self):
        import copy
        script = (DEPLOY / "install-or-upgrade.sh").read_text()
        source = script.split("<<'PYMOUNTS'\n", 1)[1].split("\ntry:\n    original = model()", 1)[0]
        namespace = {}
        exec(source, namespace)
        verify = namespace["verify_call_processes"]
        worker = "mautrix-whatsapp-call-worker"
        provisioner = "mautrix-whatsapp-call-provisioner"
        model = {"services": {worker: {"volumes": []}, "mautrix-whatsapp": {}}}
        fixture_call_bindings(model, "/opt/matrix")
        with mock.patch.dict(os.environ, {"MATRIXRTC_SERVER_NAME": "example.org"}):
            verify(model)
            for service, path, value in (
                (worker, ("entrypoint",), ["/bin/sh"]),
                (worker, ("command",), ["sleep", "100"]),
                (provisioner, ("command",), ["python3", "old.py"]),
                (worker, ("environment", "RTC_WORKER_SOCKET"), "/tmp/old.sock"),
                (worker, ("environment", "RTC_WORKER_SOCKET_MODE"), "0600"),
                (worker, ("environment", "RTC_WORKER_FFMPEG"), "/tmp/ffmpeg"),
                (provisioner, ("environment", "RTC_WORKER_STATE_DIR"), "/tmp/state"),
                (provisioner, ("environment", "RTC_AUTOPROVISION_CONFIG"), "/tmp/config"),
                (provisioner, ("environment", "RTC_AUTOPROVISION_REGISTRATION"), "/tmp/registration"),
                (worker, ("environment", "MATRIXRTC_SERVER_NAME"), "other.org"),
                (worker, ("environment", "MATRIX_HOMESERVER"), "http://other:8008"),
                (worker, ("healthcheck", "disable"), True),
                (worker, ("healthcheck", "test"), ["CMD", "true"]),
                (worker, ("volumes",), [{"target": "/usr/local/bin"}]),
                (provisioner, ("volumes",), [{"target": "/"}]),
            ):
                with self.subTest(service=service, path=path):
                    candidate = copy.deepcopy(model)
                    obj = candidate["services"][service]
                    for key in path[:-1]:
                        obj = obj[key]
                    obj[path[-1]] = value
                    with self.assertRaises(ValueError):
                        verify(candidate)

    def test_call_network_contract(self):
        import copy
        script = (DEPLOY / "install-or-upgrade.sh").read_text()
        source = script.split("<<'PYMOUNTS'\n", 1)[1].split("\ntry:\n    original = model()", 1)[0]
        namespace = {}
        exec(source, namespace)
        verify = namespace["verify_call_networks"]
        worker = "mautrix-whatsapp-call-worker"
        provisioner = "mautrix-whatsapp-call-provisioner"
        base = {"services": {"synapse": {"networks": {"matrix": None}},
                             "mautrix-whatsapp": {"networks": {"matrix": None}}},
                "networks": {"matrix": {"name": "existing_matrix"}}}
        merged = copy.deepcopy(base)
        merged["networks"]["rtc"] = {"name": "call_network"}
        for service in (worker, provisioner, "lk-jwt-service"):
            merged["services"][service] = {"networks": {"matrix": None, "rtc": None}}
        merged["services"]["livekit"] = {"networks": ["rtc"]}
        verify(base, merged)
        for service, path, value in (
            (worker, "networks", {"rtc": None}),
            (provisioner, "networks", {"rtc": None}),
            ("lk-jwt-service", "networks", {"rtc": None}),
            ("livekit", "networks", {"isolated": None}),
            (worker, "network_mode", "host"),
            ("livekit", "network_mode", "none"),
            ("synapse", "networks", {"matrix": None, "rtc": None}),
            ("mautrix-whatsapp", "networks", {"rtc": None}),
            ("mautrix-whatsapp", "networks", {"matrix": {"aliases": ["changed"]}}),
        ):
            with self.subTest(service=service, path=path):
                candidate = copy.deepcopy(merged)
                candidate["services"][service][path] = value
                with self.assertRaises(ValueError):
                    verify(base, candidate)
        candidate = copy.deepcopy(merged)
        candidate["networks"]["matrix"]["name"] = "replacement"
        with self.assertRaises(ValueError):
            verify(base, candidate)
        # Compose defaults to the project network when none is specified.
        default_base = {"services": {key: {} for key in base["services"]}}
        default_merged = {"services": {key: {} for key in merged["services"]}}
        verify(default_base, default_merged)

    def test_existing_service_settings_guard(self):
        import copy
        script = (DEPLOY / "install-or-upgrade.sh").read_text()
        start = script.index("def verify_existing_service_settings(")
        end = script.index("def verify_call_networks(", start)
        self.assertIn("verify_existing_service_settings(original, merged)", script[end:])
        scope = {"mounts": lambda config, service: {
            mount["target"]: mount for mount in config["services"][service].get("volumes", [])}}
        exec(script[start:end], scope)
        verify = scope["verify_existing_service_settings"]
        base = {"services": {
            "synapse": {"image": "synapse:original", "command": ["run"]},
            "postgres": {"image": "postgres:16"},
            "mautrix-whatsapp": {"image": "bridge:original", "command": ["run"],
                "entrypoint": ["start"], "ports": [{"target": 29318}],
                "environment": {"GID": "1000", "KEEP": "value"},
                "group_add": ["123"], "volumes": [{"target": "/data"}]},
            "livekit": {"image": "old"}},
            "volumes": {"db": {"name": "persistent-db"}},
            "secrets": {"db-password": {"file": "/local/password"}}}
        merged = copy.deepcopy(base)
        bridge = merged["services"]["mautrix-whatsapp"]
        bridge.update(image="mautrix-whatsapp-call-bridge:local", build={"context": "bridge"},
                      group_add=["123", "29319"], environment={"GID": "29319", "KEEP": "value",
                          "MATRIXRTC_INCOMING_MEMBERSHIP_FORMAT": "room_state"})
        bridge["volumes"].append({"target": "/run/mautrix-whatsapp-call"})
        merged["services"]["livekit"]["image"] = "new"
        merged["services"]["mautrix-whatsapp-call-worker"] = {}
        verify(base, merged)
        for service, field, value in (
            ("synapse", "image", "wrong"), ("synapse", "command", ["other"]),
            ("postgres", "image", "postgres:wrong"),
            ("mautrix-whatsapp", "command", ["other"]),
            ("mautrix-whatsapp", "entrypoint", ["other"]),
            ("mautrix-whatsapp", "ports", []),
            ("mautrix-whatsapp", "group_add", ["29319"]),
            ("mautrix-whatsapp", "group_add", ["123", "29319", "999"]),
            ("mautrix-whatsapp", "environment", {"GID": "29319", "KEEP": "wrong"}),
            ("mautrix-whatsapp", "environment", {"GID": "29319", "KEEP": "value", "EXTRA": "x"}),
            ("mautrix-whatsapp", "volumes", [{"target": "/data"}, {"target": "/unexpected"}]),
        ):
            with self.subTest(service=service, field=field, value=value):
                candidate = copy.deepcopy(merged)
                candidate["services"][service][field] = value
                with self.assertRaises(ValueError):
                    verify(base, candidate)
        for kind, name in (("volumes", "db"), ("secrets", "db-password")):
            candidate = copy.deepcopy(merged)
            del candidate[kind][name]
            with self.assertRaises(ValueError):
                verify(base, candidate)

    def test_permissions_do_not_expose_unknown_files(self):
        script=(DEPLOY/"install-or-upgrade.sh").read_text()
        func=script[script.index("normalize_deployment_permissions()"):script.index("\nenv_setting()")]
        with TemporaryDirectory() as tmp:
            root=Path(tmp)
            for directory in ("worker","bridge","generated","worker-state"):
                (root/directory).mkdir()
            for name in ("README.md","generated/token","worker-state/secret","private.bak"):
                (root/name).write_text("fixture")
                (root/name).chmod(0o600 if name=="private.bak" else 0o777)
            env={**os.environ,"RTC_ROOT":tmp}
            subprocess.run(["bash","-ec",func+"\nnormalize_deployment_permissions"],env=env,check=True)
            self.assertEqual((root/"README.md").stat().st_mode&0o777,0o644)
            for name in ("generated/token","worker-state/secret","private.bak"):
                self.assertEqual((root/name).stat().st_mode&0o777,0o600)

    def test_build_record_snapshot_and_evidence(self):
        record = runpy.run_path(str(ROOT / "build/build-record.py"))
        with TemporaryDirectory() as tmp:
            base = Path(tmp)
            source = base / "source"
            digest = record["snapshot"](ROOT, source)
            self.assertEqual(digest, record["source_digest"](source))
            self.assertFalse((source / ".git").exists())
            self.assertFalse((source / "dist").exists())
            binaries, logs = base / "bin", base / "logs"
            binaries.mkdir(); logs.mkdir()
            for name in record["BINARIES"]:
                (binaries / name).write_bytes(fixture_elf_header() + name.encode())
            for name in (*record["BUILD_LOGS"], *record["TEST_LOGS"]):
                (logs / name).write_text("synthetic successful command log\n")
            path = base / "BUILD-RECORD.json"
            command = [sys.executable, "-B", str(ROOT / "build/build-record.py")]
            args = [str(path), str(source), str(binaries), str(logs), digest]
            subprocess.run(command + ["create"] + args, check=True)
            original = path.read_bytes()
            record["verify_local"](path, source, binaries, logs, digest)
            self.assertNotEqual(subprocess.run(command + ["create"] + args, capture_output=True).returncode, 0)
            self.assertEqual(path.read_bytes(), original)
            for name in (*record["BUILD_LOGS"], *record["TEST_LOGS"]):
                log = logs / name
                data = log.read_bytes()
                for invalid in (None, b"", b"modified"):
                    if invalid is None: log.unlink()
                    else: log.write_bytes(invalid)
                    with self.assertRaises((OSError, ValueError)):
                        record["verify_local"](path, source, binaries, logs, digest)
                    log.write_bytes(data)
            binary = binaries / record["BINARIES"][0]
            data = binary.read_bytes(); binary.write_bytes(data + b"changed")
            with self.assertRaises(ValueError):
                record["verify_local"](path, source, binaries, logs, digest)
            binary.write_bytes(data)
            for name in ("bridge/go.sum", "bridge/third_party/meowcaller/go.sum"):
                checksums = source / name
                data = checksums.read_bytes()
                checksums.write_bytes(data + b"\n# unexpected dependency change\n")
                with self.assertRaisesRegex(ValueError, "source changed"):
                    record["verify_local"](path, source, binaries, logs, digest)
                checksums.write_bytes(data)
                record["verify_local"](path, source, binaries, logs, digest)
            versions = source / "build/versions.env"
            versions.write_bytes(versions.read_bytes() + b"\n# changed\n")
            with self.assertRaisesRegex(ValueError, "source changed"):
                record["verify_local"](path, source, binaries, logs, digest)

    def test_release_record_rejects_missing_or_mismatched_evidence(self):
        record = runpy.run_path(str(ROOT / "build/build-record.py"))
        with TemporaryDirectory() as tmp:
            release = Path(tmp)
            (release / "bin").mkdir()
            versions = release / "SOURCE-VERSIONS.env"
            versions.write_text("fixture")
            for name in record["BINARIES"]:
                (release / "bin" / name).write_bytes(fixture_elf_header())
            path = release / "BUILD-RECORD.json"
            valid = {"format": 2, "test_status": "passed", "source_sha256": "a" * 64,
                     "versions_sha256": record["sha"](versions),
                     "binaries": {name: record["sha"](release / "bin" / name) for name in record["BINARIES"]},
                     "logs": {name: "b" * 64 for name in (*record["BUILD_LOGS"], *record["TEST_LOGS"])}}
            with self.assertRaises(OSError): record["verify_release"](release)
            path.write_text(json.dumps(valid))
            record["verify_release"](release)
            for change in ({"format": 1}, {"test_status": "skipped"}, {"logs": {}},
                           {"versions_sha256": "c" * 64}, {"source_sha256": "invalid"}):
                path.write_text(json.dumps({**valid, **change}))
                with self.assertRaises(ValueError): record["verify_release"](release)
            path.write_text(json.dumps(valid)[:-1] + ', "format": 2}')
            with self.assertRaisesRegex(ValueError, "Duplicate"):
                record["verify_release"](release)
            path.write_text(json.dumps(valid))
            (release / "bin" / record["BINARIES"][1]).write_bytes(b"modified")
            with self.assertRaisesRegex(ValueError, "binary differs"):
                record["verify_release"](release)

    def test_package_verifier_requires_matching_build_record(self):
        record = runpy.run_path(str(ROOT / "build/build-record.py"))
        layout = runpy.run_path(str(ROOT / "build/runtime-layout.py"))
        with TemporaryDirectory() as tmp:
            release = Path(tmp) / "mautrix-whatsapp-call-20261125-000000"
            layout["stage"](ROOT, release)
            for name, directory in layout["BINARIES"].items():
                for rel in ("bin/" + name, layout["DEPLOY"] + directory + "/" + name):
                    path = release / rel
                    path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile("/bin/true", path)  # Real host ELF, not a built bridge.
                    path.chmod(0o755)
            for path in (release / layout["DEPLOY"]).rglob("*"):
                if path.is_file() and path.suffix in (".sh", ".py"): path.chmod(0o755)
            versions = release / "SOURCE-VERSIONS.env"
            shutil.copyfile(ROOT / "build/versions.env", versions)
            evidence = {"format": 2, "test_status": "passed", "source_sha256": "a" * 64,
                        "versions_sha256": record["sha"](versions),
                        "binaries": {name: record["sha"](release / "bin" / name) for name in record["BINARIES"]},
                        "logs": {name: "b" * 64 for name in (*record["BUILD_LOGS"], *record["TEST_LOGS"])}}
            path = release / "BUILD-RECORD.json"
            manifest = release / "SHA256SUMS"
            def verify():
                manifest.write_text("".join(record["sha"](p) + "  ./" + p.relative_to(release).as_posix() + "\n"
                    for p in sorted(release.rglob("*")) if p.is_file() and p != manifest))
                return subprocess.run(["bash", str(ROOT / "build/verify-package.sh"), str(release)],
                                      capture_output=True, text=True)
            self.assertNotEqual(verify().returncode, 0)
            path.write_text(json.dumps(evidence))
            result = verify()
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            # Replace both copies and regenerate the general manifest: only
            # the independently retained build record catches this mismatch.
            name = record["BINARIES"][0]
            for rel in ("bin/" + name, layout["DEPLOY"] + layout["BINARIES"][name] + "/" + name):
                target = release / rel
                shutil.copyfile("/bin/false", target)
            result = verify()
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Release binary differs from build record", result.stderr)

    def test_failed_build_preserves_previous_output(self):
        # Actual shell entry point, but fail source verification before any
        # download or compiler. Prior legacy and per-run outputs must survive.
        with TemporaryDirectory() as tmp:
            base = Path(tmp)
            fixture = base / "source/build"; fixture.mkdir(parents=True)
            for name in ("build.sh", "versions.env"):
                shutil.copyfile(ROOT / "build" / name, fixture / name)
            (fixture / "verify-source.sh").write_text("exit 37\n")
            work = base / "work"; dist = base / "dist"
            prior = (work / "bin/old", work / "logs/old", work / "runs/previous/bin/old",
                     dist / "mautrix-whatsapp-call-20261125-000000.zip")
            for path in prior:
                path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(b"previous output")
            env = {**os.environ, "MAUTRIX_WHATSAPP_CALL_CACHE": str(base / "cache"),
                   "MAUTRIX_WHATSAPP_CALL_WORK": str(work), "MAUTRIX_WHATSAPP_CALL_DIST": str(dist),
                   "MAUTRIX_WHATSAPP_CALL_BUILD_JOBS": "2", "MAUTRIX_WHATSAPP_CALL_SKIP_TESTS": "0"}
            command = ["bash", str(fixture / "build.sh")]
            for skip, jobs, expected in (("1", "2", 2), ("0", "0", 2), ("0", "2", 37)):
                result = subprocess.run(command, env={**env, "MAUTRIX_WHATSAPP_CALL_SKIP_TESTS": skip,
                    "MAUTRIX_WHATSAPP_CALL_BUILD_JOBS": jobs}, capture_output=True, text=True)
                self.assertEqual(result.returncode, expected, result.stderr)
                for path in prior: self.assertEqual(path.read_bytes(), b"previous output")
            runs = list((work / "runs").glob("build.*"))
            self.assertEqual(len(runs), 1)
            self.assertTrue((runs[0] / "logs").is_dir())
            self.assertFalse((runs[0] / "BUILD-RECORD.json").exists())

    def test_runtime_allowlist_and_archive_guards(self):
        layout=runpy.run_path(str(ROOT/"build/runtime-layout.py"))
        with TemporaryDirectory() as tmp:
            base=Path(tmp)
            source=base/"source"
            shutil.copytree(DEPLOY,source/layout["DEPLOY"])
            for name in layout["LICENSE_FILES"]:
                target=source/name
                target.parent.mkdir(parents=True,exist_ok=True)
                shutil.copyfile(ROOT/name,target)
            (source/layout["DEPLOY"]/"generated").mkdir()
            (source/layout["DEPLOY"]/"generated/private").write_text("fixture secret")
            release=base/"mautrix-whatsapp-call-20261124-000000"
            layout["stage"](source,release)
            self.assertFalse((release/layout["DEPLOY"]/"generated").exists())
            # Header-only, deliberately non-executable fixture. Not a release binary.
            fake_elf=b"\x7fELF\x02\x01"+bytes(12)+b"\x3e\x00"
            for name, directory in layout["BINARIES"].items():
                for rel in ("bin/"+name,layout["DEPLOY"]+directory+"/"+name):
                    path=release/rel
                    path.parent.mkdir(parents=True,exist_ok=True)
                    path.write_bytes(fake_elf)
            (release/"SOURCE-VERSIONS.env").write_text("fixture")
            (release/"BUILD-RECORD.json").write_text("fixture layout only")
            manifest=release/"SHA256SUMS"
            def update_manifest():
                manifest.write_text("".join(hashlib.sha256(p.read_bytes()).hexdigest()+"  ./"+p.relative_to(release).as_posix()+"\n" for p in sorted(release.rglob("*")) if p.is_file() and p!=manifest))
            update_manifest()
            layout["verify_tree"](release)
            original=manifest.read_text()
            manifest.write_text("\n".join(original.splitlines()[1:])+"\n")
            with self.assertRaisesRegex(ValueError,"does not cover"): layout["verify_tree"](release)
            manifest.write_text(original)
            (release/"private").write_text("fixture")
            with self.assertRaisesRegex(ValueError,"layout mismatch"): layout["verify_tree"](release)
            (release/"private").unlink()
            binary=release/"bin/mautrix-whatsapp-call-worker"
            binary.write_bytes(fake_elf+b"different")
            update_manifest()
            with self.assertRaisesRegex(ValueError,"binary differs"): layout["verify_tree"](release)
            binary.write_bytes(fake_elf)
            update_manifest()
            archive=base/"fixture.zip"
            with ZipFile(archive,"w") as z:
                for p in release.rglob("*"):
                    if p.is_file(): z.write(p,p.relative_to(base))
            layout["verify_zip"](archive)
            extracted=base/"extracted";extracted.mkdir()
            layout["verify_zip"](archive,extracted)
            self.assertEqual((extracted/release.name/"SHA256SUMS").read_bytes(),manifest.read_bytes())
            with ZipFile(archive,"a") as z: z.writestr(release.name+"/../outside","bad")
            with self.assertRaisesRegex(ValueError,"Invalid release archive entry"): layout["verify_zip"](archive)
            link=ZipInfo(release.name+"/bin/link")
            link.create_system=3
            link.external_attr=(stat.S_IFLNK|0o777)<<16
            with ZipFile(archive,"w") as z: z.writestr(link,"/etc/passwd")
            with self.assertRaisesRegex(ValueError,"Unexpected archive file"): layout["verify_zip"](archive)

    def test_cleanup_preserves_nonrelease_and_incomplete_files(self):
        with TemporaryDirectory() as tmp:
            root=Path(tmp)
            for stamp in ("20261121","20261122","20261123"):
                name="mautrix-whatsapp-call-"+stamp+"-000000.zip"
                (root/name).touch()
                (root/(name+".sha256")).touch()
            (root/"mautrix-whatsapp-call-20261120-000000.zip").touch()
            (root/"unrelated.zip").touch()
            subprocess.run(["bash",str(DEPLOY/"server-cleanup.sh"),tmp],check=True,capture_output=True)
            self.assertFalse((root/"mautrix-whatsapp-call-20261121-000000.zip").exists())
            self.assertFalse((root/"mautrix-whatsapp-call-20261121-000000.zip.sha256").exists())
            self.assertEqual(len(list(root.iterdir())),6)

    def test_redirect_handler_rejects_authenticated_redirect(self):
        from urllib.request import Request
        handler=automatic["NoRedirect"]()
        req=Request("https://example.org",headers={"Authorization":"Bearer fixture"})
        self.assertIsNone(handler.redirect_request(req,None,302,"Found",{},"https://other.example.org"))

    def test_saved_calls_value(self):
        script=(DEPLOY/"install-or-upgrade.sh").read_text()
        getter=script[script.index("env_setting()"):script.index("WA_REG_VALUE=")]
        normalise=script[script.index('CALLS_SETTING="$(env_setting'):script.index("INSTALL_COMPLETE=0")]
        restore=script[script.index("restore_worker()"):script.index("trap restore_calls_on_error EXIT")]
        with TemporaryDirectory() as tmp:
            for raw,expected in (("true","1"),("yes","1"),("1","1"),("false","0"),("0","0")):
                (Path(tmp)/".env").write_text("MATRIXRTC_ENABLE_CALLS="+raw+"\n")
                # Compose and inspect are test functions. No service is touched.
                fixture='''compose(){ if [[ "$1" == up ]]; then echo "$MATRIXRTC_ENABLE_CALLS" > "$MATRIX_ROOT/calls"; else echo fixture; fi; }
docker(){ if [[ "$*" == *State.Running* ]]; then echo true; else echo "RTC_WORKER_ENABLE_CALLS=$(cat "$MATRIX_ROOT/calls")"; fi; }
'''
                env={**os.environ,"MATRIX_ROOT":tmp,"MATRIXRTC_ENABLE_CALLS":"opposite"}
                subprocess.run(["bash","-ec",getter+normalise+restore+fixture+"restore_worker\n"],env=env,check=True)
                self.assertEqual((Path(tmp)/"calls").read_text().strip(),expected)
                result=subprocess.run(["bash","-ec",getter+normalise+restore+fixture+
                    "stop_call_services(){ echo stopped; }\nWORKER_PAUSED=1\nINSTALL_COMPLETE=0\ntrap restore_calls_on_error EXIT\nexit 7"],env=env)
                self.assertEqual(result.returncode,7)

if __name__=="__main__":
    unittest.main()
