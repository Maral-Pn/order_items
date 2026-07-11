import os
import glob
import tempfile
import unittest

from test.test_base import TestBase
from core.io.output_writer import OutputWriter


class TestOutputWriter(TestBase):
    def setUp(self):
        super().setUp()
        self.writer = OutputWriter()

    def test_write_roundtrip(self):
        rows = [["Maral Pourdayan", 100], ["Hossein Bakhtiari", 200]]
        columns = ["client_name", "total"]
        df = self.spark.createDataFrame(rows, columns)

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "out")
            self.writer.write(df, path)

            read_back = self.spark.read.json(path)
            self.assertDataFrameEqual(
                read_back.select("client_name", "total").orderBy("client_name"),
                df.orderBy("client_name"),
            )

    def test_overwrite_mode_replaces_previous_data(self):
        first = self.spark.createDataFrame(
            [["Maral Pourdayan", 1]], ["client_name", "total"]
        )
        second = self.spark.createDataFrame(
            [["Hossein Bakhtiari", 2]], ["client_name", "total"]
        )

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "out")
            self.writer.write(first, path, mode="overwrite")
            self.writer.write(second, path, mode="overwrite")

            read_back = self.spark.read.json(path)
            self.assertDataFrameEqual(
                read_back.select("client_name", "total"),
                second,
            )

    def test_partition_column_creates_partition_dirs(self):
        rows = [
            ["Maral Pourdayan", 1],
            ["Maral Pourdayan", 2],
            ["Hossein Bakhtiari", 3],
        ]
        df = self.spark.createDataFrame(rows, ["client_name", "total"])

        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "out")
            self.writer.write(df, path, partition_column="client_name")

            partition_dirs = sorted(
                os.path.basename(p)
                for p in glob.glob(os.path.join(path, "client_name=*"))
            )
            self.assertEqual(
                partition_dirs,
                ["client_name=Hossein Bakhtiari", "client_name=Maral Pourdayan"],
            )

            read_back = self.spark.read.json(path)
            self.assertEqual(read_back.count(), 3)
            self.assertIn("client_name", read_back.columns)


if __name__ == "__main__":
    unittest.main()
