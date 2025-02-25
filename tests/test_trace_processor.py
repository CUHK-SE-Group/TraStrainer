import csv
import json
import os
import tempfile
import unittest

from TraStrainer import TraceProcessor


class TestTraceProcessorNewFormat(unittest.TestCase):
    def setUp(self):
        # Create a temporary CSV file in the new trace format.
        self.temp_file = tempfile.NamedTemporaryFile(
            mode="w+", suffix=".csv", delete=False, encoding="utf-8"
        )
        fieldnames = [
            "Timestamp",
            "TraceId",
            "SpanId",
            "ParentSpanId",
            "TraceState",
            "SpanName",
            "SpanKind",
            "ServiceName",
            "ResourceAttributes",
            "SpanAttributes",
            "Duration",
            "StatusCode",
            "StatusMessage",
        ]
        writer = csv.DictWriter(self.temp_file, fieldnames=fieldnames)
        writer.writeheader()
        # Row 1: a root span (ParentSpanId empty)
        writer.writerow(
            {
                "Timestamp": "2024-11-21 12:50:51.550497367",
                "TraceId": "trace-1",
                "SpanId": "root-span",
                "ParentSpanId": "",
                "TraceState": "",
                "SpanName": "GET",
                "SpanKind": "Client",
                "ServiceName": "ts-auth-service",
                "ResourceAttributes": json.dumps({"dummy": "value"}),
                "SpanAttributes": json.dumps({"attr": "val"}),
                "Duration": "3000000000",
                "StatusCode": "Unset",
                "StatusMessage": "",
            }
        )
        # Row 2: a child span (ParentSpanId = "root-span")
        writer.writerow(
            {
                "Timestamp": "2024-11-21 12:50:50.550497367",
                "TraceId": "trace-1",
                "SpanId": "child-span",
                "ParentSpanId": "root-span",
                "TraceState": "",
                "SpanName": "GET",
                "SpanKind": "Client",
                "ServiceName": "ts-auth-service",
                "ResourceAttributes": json.dumps({"dummy": "value"}),
                "SpanAttributes": json.dumps({"attr": "val"}),
                "Duration": "1500000000",
                "StatusCode": "Unset",
                "StatusMessage": "",
            }
        )
        self.temp_file.close()

    def tearDown(self):
        os.remove(self.temp_file.name)

    def test_read_traces_new_format(self):
        # Test reading the new trace CSV format.
        traces = TraceProcessor.read_traces(self.temp_file.name)
        # Ensure we have one trace with two spans.
        self.assertIn("trace-1", traces)
        self.assertEqual(len(traces["trace-1"]), 2)

        # Verify processing of the new fields.
        root_span = traces["trace-1"][0]
        self.assertEqual(root_span.get("StartTime"), "2024-11-21 12:50:48")
        self.assertEqual(root_span.get("EndTime"), "2024-11-21 12:50:51")
        # When ParentSpanId is empty, our code below sets ParentID to 'root'
        self.assertEqual(root_span.get("ParentID"), "root")
        # PodName is set from ServiceName.
        self.assertEqual(root_span.get("PodName"), "ts-auth-service")
        # OperationName should be equal to SpanName.
        self.assertEqual(root_span.get("OperationName"), "GET")
        # status should be set to 'success'
        self.assertEqual(root_span.get("status"), "success")

    def test_build_tree_and_seq_span(self):
        # Use read_traces to load spans.
        traces = TraceProcessor.read_traces(self.temp_file.name)
        spans = traces.get("trace-1", [])
        # Build trace tree
        tree = TraceProcessor.build_trace_tree(spans)
        # Verify that the tree has root and child.
        self.assertTrue(tree.contains("root-span"))
        self.assertTrue(tree.contains("child-span"))
        # Check child relationship: child-span's parent should be root-span.
        self.assertEqual(tree.parent("child-span").identifier, "root-span")

        # Test get_seq_span returns a non-empty list.
        seq = TraceProcessor.get_seq_span(spans, tree)
        self.assertIsInstance(seq, list)
        self.assertGreater(len(seq), 0)


if __name__ == "__main__":
    unittest.main()
