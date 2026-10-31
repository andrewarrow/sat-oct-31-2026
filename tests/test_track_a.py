"""Local, dependency-free checks: python3 -m unittest discover -s tests -v.

Set TRACK_A_SANITIZE=1 to compile with AddressSanitizer and UndefinedBehaviorSanitizer.
"""
import collections
import datetime
import hashlib
import os
from pathlib import Path
import random
import shlex
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
START = datetime.datetime(2026, 10, 30, 18)
LONGEST = (
    "/internal/v1/tenants/{tenant_id}/projects/{project_id}"
    "/pipelines/{pipeline_id}/runs/{run_id}/artifacts/{artifact_id}:download"
)
METHODS = ("GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS")


def record(hour, endpoint, status, latency, method="GET", offset=0):
    when = START + datetime.timedelta(hours=hour, microseconds=offset)
    return f"{when:%Y-%m-%dT%H:%M:%S.%f}Z {method} {endpoint} {status} {latency}\n"


def reference(text):
    groups = collections.defaultdict(list)
    for line in text.splitlines():
        timestamp, _, endpoint, status, latency = line.split(" ")
        when = datetime.datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        hour = when.replace(minute=0, second=0, microsecond=0)
        groups[hour, endpoint].append((int(latency), int(status)))
    output = []
    for (hour, endpoint), requests in sorted(groups.items()):
        values = sorted(latency for latency, _ in requests)
        count = len(values)
        errors = sum(500 <= status <= 599 for _, status in requests)
        percentiles = [values[(p * count + 99) // 100 - 1] for p in (50, 95, 99)]
        fields = [count, *percentiles, errors, errors * 1_000_000 // count]
        output.append(f"{hour:%Y-%m-%dT%H:00:00Z} {endpoint} {' '.join(map(str, fields))}\n")
    return "".join(output).encode("ascii")


class TrackATest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.build = tempfile.TemporaryDirectory(prefix="track-a-build-")
        cls.addClassCleanup(cls.build.cleanup)
        cls.binary = Path(cls.build.name) / "solution"
        flags = ["-std=c11", "-Wall", "-Wextra", "-Wpedantic", "-Werror", "-pthread"]
        if os.environ.get("TRACK_A_SANITIZE") == "1":
            flags += ["-O1", "-g", "-fsanitize=address,undefined", "-fno-omit-frame-pointer"]
        else:
            flags += ["-O3"]
        subprocess.run(
            [*shlex.split(os.environ.get("CC", "cc")), *flags,
             str(ROOT / "track-a" / "ingest.c"), "-o", str(cls.binary)],
            check=True,
        )

    def check_summary(self, text, expected=None):
        with tempfile.TemporaryDirectory(prefix="track-a-test-") as temporary:
            data = Path(temporary) / "data"
            out = Path(temporary) / "out"
            data.mkdir()
            (data / "requests.log").write_bytes(text.encode("ascii"))
            result = subprocess.run(
                [self.binary, "--data", data, "--out", out],
                capture_output=True, timeout=30,
            )
            self.assertEqual(result.returncode, 0, result.stderr.decode())
            self.assertEqual(result.stdout, b"")
            self.assertEqual(sorted(path.name for path in out.iterdir()), ["summary.txt"])
            actual = (out / "summary.txt").read_bytes()
            self.assertEqual(actual, reference(text) if expected is None else expected)

    def test_empty_input(self):
        self.check_summary("", b"")

    def test_known_result_and_duplicate_requests(self):
        text = (
            record(1, "/v2/orders/{id}", 200, 1500)
            + record(0, "/v2/orders/{id}", 200, 1841, offset=3_599_999_999)
            + record(0, "/v2/checkout", 503, 204117, "POST")
            + record(0, "/v2/checkout", 201, 30000, "POST")
            + record(0, "/health", 200, 62) * 2
            + record(0, "/health", 200, 4294967295, "HEAD")
            + record(0, "/", 200, 0, "OPTIONS")
            + record(0, "/v2/checkout", 500, 0)
            + record(0, "/v2/checkout", 499, 1500)
            + record(0, "/v2/checkout", 599, 2500)
            + record(0, "/v2/orders/{id}", 500, 900)
            + record(0, "/v2/orders/{id}", 404, 77, "DELETE")
            + record(29, "/v2/checkout", 502, 7, offset=3_599_999_999)
            + record(30, "/v2/checkout", 503, 8)
        )
        expected = (
            "2026-10-30T18:00:00Z / 1 0 0 0 0 0\n"
            "2026-10-30T18:00:00Z /health 3 62 4294967295 4294967295 0 0\n"
            "2026-10-30T18:00:00Z /v2/checkout 5 2500 204117 204117 3 600000\n"
            "2026-10-30T18:00:00Z /v2/orders/{id} 3 900 1841 1841 1 333333\n"
            "2026-10-30T19:00:00Z /v2/orders/{id} 1 1500 1500 1500 0 0\n"
            "2026-10-31T23:00:00Z /v2/checkout 1 7 7 7 1 1000000\n"
            "2026-11-01T00:00:00Z /v2/checkout 1 8 8 8 1 1000000\n"
        ).encode("ascii")
        self.check_summary(text, expected)

    def test_all_hours_and_boundaries_in_reverse_order(self):
        self.check_summary("".join(
            record(hour, "/", 599, hour, METHODS[hour % 7], offset=offset)
            for hour in reversed(range(72)) for offset in (3_599_999_999, 0)
        ))

    def test_byte_order_and_maximum_endpoint_length(self):
        names = [
            "/", "/health", "/healthz", LONGEST, "/v2/orders", "/v2/orders-export",
            "/v2/orders.csv", "/v2/orders/{id}", "/v2/orders/{id}/items",
            "/v2/ordersByDate", "/v2/orders_legacy", "/v2/orderstatus", "/v2/rank100",
            "/v2/rank20", "/A", "/a", "/a.", "/a/", "/a:", "/a_", "/a{", "/a}",
        ]
        self.assertEqual(len(LONGEST), 125)
        self.check_summary("".join(record(0, name, 200, i) for i, name in enumerate(reversed(names))))

    def test_every_status_and_large_error_product(self):
        self.check_summary("".join(record(0, "/status", status, 0) for status in range(100, 600)))
        self.check_summary(record(0, "/errors", 500, 4294967295) * 10_000)

    def test_nearest_rank_and_selection_distributions(self):
        lines = []
        for count in (1, 2, 3, 4, 5, 20, 99, 100, 101, 199, 200, 201, 1000, 4096):
            distributions = [
                list(range(count)), list(reversed(range(count))), [4294967295] * count,
                [0, 4294967295] * (count // 2) + [0] * (count % 2),
                [min(i, count - 1 - i) for i in range(count)],
            ]
            for kind, values in enumerate(distributions):
                lines.extend(record(0, f"/rank/{count}/{kind}", 200, value) for value in values)
        self.check_summary("".join(lines))

    def test_full_endpoint_catalogue_and_hash_collisions(self):
        names = [f"/endpoint/{i:04d}" for i in range(1842)]
        self.check_summary("".join(record(i % 72, name, 200, i) for i, name in enumerate(names)))

    def test_randomized_records_across_multiple_input_buffers(self):
        rng = random.Random(20261031)
        names = ["/", LONGEST, "/v2/orders", "/v2/orders.csv"] + [f"/random/{i}" for i in range(40)]
        lines = [
            record(rng.randrange(72), rng.choice(names), rng.randrange(100, 600),
                   rng.choice([0, 4294967295, rng.getrandbits(32)]), rng.choice(METHODS),
                   offset=rng.randrange(3_600_000_000))
            for _ in range(40_000)
        ]
        lines += lines[:100]
        rng.shuffle(lines)
        text = "".join(lines)
        self.assertGreater(len(text), 2 << 20)
        self.check_summary(text)

    @unittest.skipUnless((ROOT / "data/a/full-seed1/requests.log").exists(),
                         "generate the official seed-1 datasets under data/a first")
    def test_published_seed_one_hashes(self):
        hashes = {
            "tiny": "c24fa507c963eaeac9224fd8435019b4f4e8c002ece7e32f316681a3957eb92f",
            "sample": "ca6311bbeb536fa55d85265bf6fd3d24cc9586f13de1ac73bd2a23ac3893aac3",
            "full": "495ca09682b9673ef836a506496177680bb4a3af54255174befddb1f83481cd3",
        }
        for scale, expected in hashes.items():
            with self.subTest(scale=scale), tempfile.TemporaryDirectory(prefix="track-a-official-") as out:
                subprocess.run(
                    [self.binary, "--data", ROOT / f"data/a/{scale}-seed1", "--out", out],
                    check=True, capture_output=True, timeout=30,
                )
                digest = hashlib.sha256((Path(out) / "summary.txt").read_bytes()).hexdigest()
                self.assertEqual(digest, expected)

    def test_help_and_invalid_arguments(self):
        result = subprocess.run([self.binary, "--help"], capture_output=True)
        self.assertEqual(result.returncode, 0)
        self.assertIn(b"--data", result.stdout)
        for arguments in (["--unknown"], ["--data"], ["--out"]):
            result = subprocess.run([self.binary, *arguments], capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(b"ingest:", result.stderr)

    def test_missing_input_and_unusable_output(self):
        with tempfile.TemporaryDirectory(prefix="track-a-errors-") as temporary:
            data = Path(temporary)
            result = subprocess.run([self.binary, "--data", data], capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(b"requests.log", result.stderr)
            (data / "requests.log").write_text(record(0, "/", 200, 1))
            (data / "summary.txt").mkdir()
            result = subprocess.run([self.binary, "--data", data, "--out", data], capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(b"summary.txt", result.stderr)

    @unittest.skipUnless(Path("/dev/full").exists(), "Linux /dev/full is unavailable")
    def test_output_flush_failure(self):
        with tempfile.TemporaryDirectory(prefix="track-a-write-error-") as temporary:
            data = Path(temporary)
            (data / "requests.log").write_text(record(0, "/", 200, 1))
            (data / "summary.txt").symlink_to("/dev/full")
            result = subprocess.run([self.binary, "--data", data, "--out", data], capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(b"summary.txt", result.stderr)


if __name__ == "__main__":
    unittest.main()
