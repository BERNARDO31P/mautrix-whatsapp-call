"""Offline regression fixtures. No Matrix, WhatsApp or Docker service is used."""
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest

ROOT=Path(__file__).resolve().parents[1]
DEPLOY=ROOT/"deployment/mautrix-whatsapp-call"
sys.dont_write_bytecode=True
sys.path[:0]=[str(DEPLOY),str(DEPLOY/"worker")]
import yaml
import session_files
configure=runpy.run_path(str(DEPLOY/"configure-bridge.py"))["update_config"]
merge=runpy.run_path(str(DEPLOY/"merge-synapse.py"))["merge"]
reconcile=runpy.run_path(str(DEPLOY/"reconcile-credentials.py"))["reconcile"]
automatic=runpy.run_path(str(DEPLOY/"worker/auto-provision-whatsapp-ghosts.py"))

class ProjectRegression(unittest.TestCase):
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
        self.assertTrue(data["experimental_features"]["other"])
        self.assertEqual(len(data["app_service_config_files"]),3)
        self.assertEqual(len(data["matrix_rtc"]["transports"]),3)
        self.assertEqual(merge(out,extra)[0],out)

    def test_interrupted_identity_and_matching_store(self):
        with TemporaryDirectory() as tmp:
            folder=Path(tmp)/"identity"
            ident="@ghost:example.org"
            session={"user_id":ident,"device_id":"D","access_token":"fixture"}
            with session_files.identity_lock(folder):
                (folder/"session.json").write_text("{")
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
                    "WORKER_PAUSED=1\nINSTALL_COMPLETE=0\ntrap restore_calls_on_error EXIT\nexit 7"],env=env)
                self.assertEqual(result.returncode,7)

if __name__=="__main__":
    unittest.main()
