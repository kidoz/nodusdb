#!/usr/bin/env python3
"""Run the process compatibility gate with pinned readers or an explicit candidate."""
import argparse
import hashlib
import json
import os
import signal
import shutil
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[2]
OLD = "31897cd7d7bbbf7b51cc3d522a2a77f4e7ec908a"
NEW = "3b4cca438485c7a1b0f1a6c43c4cc2cfba51ea1c"


def run(args, **kwargs):
    deadline = kwargs.pop("timeout", None)
    # Own the process group so a timeout/interrupt also terminates server
    # grandchildren even if Cargo or the Rust test cannot run Drop handlers.
    with subprocess.Popen(args, cwd=ROOT, start_new_session=True, **kwargs) as child:
        try:
            code = child.wait(timeout=deadline)
        except BaseException:
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            child.wait()
            raise
        if code:
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            raise subprocess.CalledProcessError(code, args)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def require(condition, message):
    if not condition:
        raise SystemExit(message)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--build-only", action="store_true")
    parser.add_argument("--reuse-builds", action="store_true")
    parser.add_argument("--candidate", action="store_true",
                        help="build the current working tree as the new reader; retain source hashes and patch separately")
    parser.add_argument("--diagnose", action="store_true",
                        help="continue after known snapshot-login failures using explicit restarts; gate still fails")
    args = parser.parse_args()
    out = (args.output or Path(tempfile.mkdtemp(prefix="nodus-mixed-"))).resolve()
    out.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    # Separate targets are essential: Cargo can reuse path-package fingerprints
    # across relocated archives with preserved historical mtimes.
    env["CARGO_TARGET_DIR"] = str(ROOT / "target/mixed-binary-build/harness")
    env["CARGO_PROFILE_DEV_DEBUG"] = "0"
    env["CARGO_PROFILE_TEST_DEBUG"] = "0"
    manifest = {"schema_version": 1, "old_revision": OLD, "new_revision": NEW,
                "rustc": subprocess.check_output(["rustc", "--version"], text=True).strip(),
                "binaries": {}}
    for label, revision in [("old", OLD), ("new", NEW)]:
        binary = out / ("nodusd-" + label)
        if not args.reuse_builds:
            source = out / ("source-" + label)
            source.mkdir(exist_ok=False)
            archive = out / (label + ".tar")
            run(["git", "archive", "--format=tar", "--output", str(archive), revision])
            run(["tar", "-xf", str(archive), "-C", str(source)])
            build_env = dict(env, CARGO_TARGET_DIR=str(ROOT / "target/mixed-binary-build" / revision))
            with (out / ("build-" + label + ".log")).open("w") as log:
                run(["cargo", "build", "--locked", "-p", "nodus_server", "--bin",
                     "nodus_server", "--manifest-path", str(source / "Cargo.toml")],
                    env=build_env, stdout=log, stderr=subprocess.STDOUT)
            shutil.copy2(Path(build_env["CARGO_TARGET_DIR"]) / "debug/nodus_server", binary)
        require(binary.is_file(), f"missing {binary}")
        manifest["binaries"][label] = {"path": str(binary), "sha256": sha(binary),
                                     "lock_sha256": sha(out / ("source-" + label) / "Cargo.lock")}
    require(manifest["binaries"]["old"]["sha256"] != manifest["binaries"]["new"]["sha256"], "identical binaries")
    manifest_path = out / "builds.json"
    if args.reuse_builds:
        require(json.loads(manifest_path.read_text()) == manifest, "build identity changed")
    else:
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"Pinned binaries and evidence: {out}", flush=True)
    new_binary = out / "nodusd-new"
    if args.candidate:
        # Keep historical artifacts intact. A candidate is explicitly identified
        # by HEAD, the tracked patch, untracked crate sources and binary hash.
        patch = subprocess.check_output(["git", "diff", "--binary", "HEAD", "--", "Cargo.toml", "Cargo.lock", "crates"], cwd=ROOT)
        (out / "candidate.patch").write_bytes(patch)
        untracked = subprocess.check_output(["git", "ls-files", "--others", "--exclude-standard", "-z", "crates"], cwd=ROOT).decode().split("\0")
        sources = {}
        for name in filter(None, untracked):
            destination = out / "candidate-untracked" / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / name, destination)
            sources[name] = sha(destination)
        identity = {"base_revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                    "patch_sha256": sha(out / "candidate.patch"), "untracked_sources": sources}
        fingerprint = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
        build_env = dict(env, CARGO_TARGET_DIR=str(ROOT / "target/mixed-binary-build" / ("candidate-" + fingerprint)))
        with (out / "build-candidate.log").open("w") as log:
            run(["cargo", "build", "--locked", "-p", "nodus_server", "--bin", "nodus_server"],
                env=build_env, stdout=log, stderr=subprocess.STDOUT)
        new_binary = out / "nodusd-candidate"
        shutil.copy2(Path(build_env["CARGO_TARGET_DIR"]) / "debug/nodus_server", new_binary)
        identity.update(binary_sha256=sha(new_binary), lock_sha256=sha(ROOT / "Cargo.lock"), rustc=manifest["rustc"])
        (out / "candidate.json").write_text(json.dumps(identity, indent=2) + "\n")
    if args.build_only:
        return
    env.update(NODUS_MIXED_OLD=str(out / "nodusd-old"),
               NODUS_MIXED_NEW=str(new_binary), NODUS_MIXED_OUTPUT=str(out))
    env["NODUS_MIXED_DIAGNOSE"] = "1" if args.diagnose else "0"
    (out / "results.json").unlink(missing_ok=True)
    try:
        with (out / "test.log").open("w") as log:
            run(["cargo", "test", "--locked", "-p", "nodus_distributed_tests", "--test",
                 "mixed_binary", "--", "--ignored", "--exact", "pinned_binary_upgrade_matrix",
                 "--nocapture"], env=env, stdout=log, stderr=subprocess.STDOUT, timeout=1200)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
        raise SystemExit(f"FAIL: {error}; see {out / 'test.log'} and {out / 'results.json'}") from error
    result = json.loads((out / "results.json").read_text())
    require(result["passed"] is True and result["matrix_completed"] is True
            and len(result["checks"]) == 13 and not result["blockers"], "incomplete or blocked matrix")
    print(f"PASS: {len(result['checks'])} checkpoints; see {out / 'results.json'}", flush=True)


if __name__ == "__main__":
    main()
