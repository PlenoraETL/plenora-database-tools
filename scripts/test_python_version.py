"""Mutation checks for the single Python policy guard."""
import unittest
from scripts.check_python_version import check, declared, reference_errors, wheel_errors


class PythonPolicy(unittest.TestCase):
    def test_repository(self):
        self.assertEqual(check(), [])

    def test_every_spelling_rejects_another_minor(self):
        expected = declared()
        major, minor = map(int, expected.split("."))
        wrong = f"{major}.{minor - 1}"
        for text in (f"Python {wrong}", f"python-version: '{wrong}'",
                     f"requires-python = '>={wrong}'", f"python:{wrong}-slim",
                     "cp" + wrong.replace(".", ""), "abi3-py" + wrong.replace(".", ""),
                     f"sys.version_info < ({major}, {minor - 1})"):
            with self.subTest(text=text):
                self.assertTrue(reference_errors("new-file", text, expected))

    def test_artifact_metadata_and_interpreter_must_agree(self):
        version = declared()
        interpreter = tuple(map(int, version.split(".")))
        tags = ["cp" + version.replace(".", "") + "-abi3-win_amd64"]
        self.assertEqual(wheel_errors(version, ">=" + version, tags, interpreter), [])
        self.assertTrue(wheel_errors(version, ">=0", tags, interpreter))
        self.assertTrue(wheel_errors(version, ">=" + version, [], interpreter))
        self.assertTrue(wheel_errors(version, ">=" + version, ["py3-none-any"], interpreter))
        self.assertTrue(wheel_errors(version, ">=" + version, tags, (0, 0)))

    def test_backports_cannot_return(self):
        for package in ("tom" + "li", "typing" + "_extensions"):
            self.assertTrue(reference_errors("new-file", "import " + package, declared()))


if __name__ == "__main__":
    unittest.main()
