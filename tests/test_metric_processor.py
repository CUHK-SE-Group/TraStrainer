import csv
import json
import os
import tempfile
import unittest

from TraStrainer import MetricProcessor


class TestMetricProcessorNewFormat(unittest.TestCase):
    def setUp(self):
        # Create a temporary CSV file with the new single-metric format
        self.temp_file = tempfile.NamedTemporaryFile(
            mode="w+", suffix=".csv", delete=False
        )
        writer = csv.writer(self.temp_file)
        # Write header and two sample rows
        writer.writerow(
            [
                "TimeUnix",
                "MetricName",
                "MetricDescription",
                "Value",
                "ServiceName",
                "MetricUnit",
                "ResourceAttributes",
                "Attributes",
            ]
        )
        writer.writerow(
            [
                "2024-11-21 12:27:10.272774559",
                "k8s.pod.cpu_limit_utilization",
                "Container CPU utilization",
                "0.0084843",
                "",
                "1",
                json.dumps(
                    {
                        "k8s.deployment.name": "ts-delivery-service",
                        "k8s.node.name": "worker2",
                        "k8s.pod.uid": "ed4d8823-5ae1-4376-8c69-8d9aa8b4b89c",
                        "k8s.pod.name": "ts-delivery-service-84c7fcb8d9-jxlfr",
                        "k8s.namespace.name": "ts",
                        "k8s.container.name": "ts-delivery-service",
                        "k8s.pod.start_time": "2024-11-21T03:51:23Z",
                    }
                ),
                "{}",
            ]
        )
        writer.writerow(
            [
                "2024-11-21 12:27:10.272774559",
                "k8s.pod.cpu_limit_utilization",
                "Container CPU utilization",
                "0.004430306",
                "",
                "1",
                json.dumps(
                    {
                        "k8s.namespace.name": "ts",
                        "k8s.container.name": "ts-route-plan-service",
                        "k8s.deployment.name": "ts-route-plan-service",
                        "k8s.node.name": "worker2",
                        "k8s.pod.start_time": "2024-11-21T03:51:18Z",
                        "k8s.pod.uid": "b84865f7-da60-44eb-8e5d-152530aec0a1",
                        "k8s.pod.name": "ts-route-plan-service-857fc9469b-wpsdb",
                    }
                ),
                "{}",
            ]
        )
        self.temp_file.close()

    def tearDown(self):
        os.remove(self.temp_file.name)

    def test_new_csv_format(self):
        # When a CSV file is provided, process_metrics should use the new input method.
        metrics = MetricProcessor.process_metrics(self.temp_file.name)

        # Expected keys based on service name extraction:
        # For first row: ("ts-delivery-service", "k8s.pod.cpu_limit_utilization")
        # For second row: ("ts-route-plan-service", "k8s.pod.cpu_limit_utilization")

        self.assertIn(("ts-delivery-service", "k8s.pod.cpu_limit_utilization"), metrics)
        self.assertIn(("ts-route-plan-service", "k8s.pod.cpu_limit_utilization"), metrics)
        # Assert that two data records exist
        self.assertEqual(
            len(metrics[("ts-delivery-service", "k8s.pod.cpu_limit_utilization")]), 1
        )
        self.assertEqual(
            len(metrics[("ts-route-plan-service", "k8s.pod.cpu_limit_utilization")]), 1
        )

        # Verify that the date is extracted from "TimeUnix" column (first 19 chars)
        first_record = metrics[("ts-delivery-service", "k8s.pod.cpu_limit_utilization")][
            0
        ]
        self.assertEqual(first_record["date"], "2024-11-21 12:27:10")
        # Assert the value matches the conversion
        self.assertAlmostEqual(first_record["value"], 0.0084843)


if __name__ == "__main__":
    unittest.main()
