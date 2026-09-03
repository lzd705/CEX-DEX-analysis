import os
import stat
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.atomic_publication import (
    atomic_replace_bundle,
    atomic_replace_prepared_bundle,
)


class AtomicPublicationTest(unittest.TestCase):
    def test_prepared_bundle_preserves_umask_for_a_new_destination(self):
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            destination = root / "new.csv"
            prepared = root / ".new.prepared"
            previous_umask = os.umask(0o077)
            try:
                prepared.write_bytes(b"private\n")
                atomic_replace_prepared_bundle([(destination, prepared)])
            finally:
                os.umask(previous_umask)

            self.assertEqual(
                stat.S_IMODE(destination.stat().st_mode),
                0o600,
            )

    def test_prepared_bundle_replaces_complete_bundle_without_loading_payloads(self):
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            destinations = [root / "one.csv", root / "two.csv"]
            prepared = [root / ".one.prepared", root / ".two.prepared"]
            for index, path in enumerate(destinations):
                path.write_bytes("old-{}\n".format(index).encode("ascii"))
            for index, path in enumerate(prepared):
                path.write_bytes("new-{}\n".format(index).encode("ascii"))

            atomic_replace_prepared_bundle(zip(destinations, prepared))

            self.assertEqual(
                [path.read_bytes() for path in destinations],
                [b"new-0\n", b"new-1\n"],
            )
            self.assertFalse(any(path.exists() for path in prepared))

    def test_prepared_bundle_replace_failure_restores_all_destinations(self):
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            destinations = [root / "one.csv", root / "two.csv"]
            prepared = [root / ".one.prepared", root / ".two.prepared"]
            originals = []
            for index, path in enumerate(destinations):
                payload = "old-{}\n".format(index).encode("ascii")
                path.write_bytes(payload)
                originals.append(payload)
            for path in prepared:
                path.write_bytes(b"new\n")

            from scripts import atomic_publication

            real_replace = atomic_publication.os.replace
            calls = {"count": 0}

            def fail_second(source, destination):
                if Path(destination) in destinations:
                    calls["count"] += 1
                    if calls["count"] == 2:
                        raise OSError("injected prepared publication failure")
                return real_replace(source, destination)

            with patch(
                "scripts.atomic_publication.os.replace",
                side_effect=fail_second,
            ):
                with self.assertRaisesRegex(
                    OSError,
                    "injected prepared publication failure",
                ):
                    atomic_replace_prepared_bundle(
                        zip(destinations, prepared)
                    )

            self.assertEqual(
                [path.read_bytes() for path in destinations],
                originals,
            )
            self.assertFalse(any(path.exists() for path in prepared))

    def test_success_replaces_the_complete_bundle(self):
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            paths = [root / "one.csv", root / "two.csv", root / "three.csv"]
            for index, path in enumerate(paths):
                path.write_bytes("old-{}\n".format(index).encode("ascii"))

            atomic_replace_bundle(
                [(path, "new-{}\n".format(index).encode("ascii"))
                 for index, path in enumerate(paths)]
            )

            self.assertEqual(
                [path.read_bytes() for path in paths],
                [b"new-0\n", b"new-1\n", b"new-2\n"],
            )

    def test_every_replace_failure_restores_all_preexisting_bytes(self):
        for fail_at in (1, 2, 3):
            with self.subTest(fail_at=fail_at), tempfile.TemporaryDirectory() as directory_name:
                root = Path(directory_name)
                paths = [root / "one.csv", root / "two.csv", root / "three.csv"]
                originals = []
                for index, path in enumerate(paths):
                    payload = "old-{}\n".format(index).encode("ascii")
                    path.write_bytes(payload)
                    originals.append(payload)

                from scripts import atomic_publication

                real_replace = atomic_publication.os.replace
                calls = {"count": 0}

                def fail_once(source, destination):
                    calls["count"] += 1
                    if calls["count"] == fail_at:
                        raise OSError("injected publication failure")
                    return real_replace(source, destination)

                with patch(
                    "scripts.atomic_publication.os.replace",
                    side_effect=fail_once,
                ):
                    with self.assertRaisesRegex(
                        OSError,
                        "injected publication failure",
                    ):
                        atomic_replace_bundle(
                            [(path, b"new\n") for path in paths]
                        )

                self.assertEqual(
                    [path.read_bytes() for path in paths],
                    originals,
                )

    def test_failure_removes_a_new_destination_and_restores_existing_files(self):
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            existing = root / "existing.csv"
            new_path = root / "new.csv"
            existing.write_bytes(b"old\n")

            from scripts import atomic_publication

            real_replace = atomic_publication.os.replace
            calls = {"count": 0}

            def fail_second(source, destination):
                calls["count"] += 1
                if calls["count"] == 2:
                    raise OSError("injected publication failure")
                return real_replace(source, destination)

            with patch(
                "scripts.atomic_publication.os.replace",
                side_effect=fail_second,
            ):
                with self.assertRaises(OSError):
                    atomic_replace_bundle(
                        [(new_path, b"new\n"), (existing, b"changed\n")]
                    )

            self.assertFalse(new_path.exists())
            self.assertEqual(existing.read_bytes(), b"old\n")


if __name__ == "__main__":
    unittest.main()
