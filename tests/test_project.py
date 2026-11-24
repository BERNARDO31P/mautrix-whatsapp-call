"""Offline regression fixtures. No Matrix, WhatsApp or Docker service is used."""
import hashlib
import json
import shutil
import stat
from zipfile import ZipFile, ZipInfo
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
import config_files
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

    def test_preflight_parses_actual_yaml_not_indent(self):
        with TemporaryDirectory() as tmp:
            matrix=Path(tmp)
            root=matrix/"mautrix-whatsapp-call"
            (root/"worker").mkdir(parents=True)
            (root/"generated").mkdir()
            (matrix/"mautrix-whatsapp").mkdir()
            (matrix/"docker-compose.yml").write_text("services: {}\n")
            for name in ("preflight.sh","compose.yml","config_files.py"):
                shutil.copyfile(DEPLOY/name,root/name)
            for name in ("Dockerfile","mautrix-whatsapp-call-worker","mautrix-whatsapp-call-provision","provision-ghost.py"):
                (root/"worker"/name).touch()
            passphrase=root/"generated/matrixrtc_store_passphrase"
            passphrase.write_text("fixture")
            passphrase.chmod(0o600)
            config=matrix/"mautrix-whatsapp/config.yaml"
            config.write_text(configure("network: {}\n","example.org","as_token:fixture"))
            fake=matrix/"fake-bin";fake.mkdir(exist_ok=True)
            docker=fake/"docker";docker.write_text("#!/bin/sh\nexit 1\n");docker.chmod(0o755)
            env={**os.environ,"PATH":str(fake)+os.pathsep+os.environ["PATH"]}
            result=subprocess.run(["bash",str(root/"preflight.sh")],capture_output=True,text=True,env=env)
            self.assertEqual(result.returncode,0,result.stderr)
            config.write_text("network: {experimental_whatsapp_voip: false}\n")
            result=subprocess.run(["bash",str(root/"preflight.sh")],capture_output=True,text=True,env=env)
            self.assertNotEqual(result.returncode,0)

    def test_permissions_do_not_expose_unknown_files(self):
        script=(DEPLOY/"install-or-upgrade.sh").read_text()
        func=script[script.index("normalize_deployment_permissions()"):script.index("\nnormalize_deployment_permissions\n")]
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
                    "WORKER_PAUSED=1\nINSTALL_COMPLETE=0\ntrap restore_calls_on_error EXIT\nexit 7"],env=env)
                self.assertEqual(result.returncode,7)

if __name__=="__main__":
    unittest.main()
