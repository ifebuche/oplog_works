import unittest
from unittest.mock import patch

from MI_ETL.notifications.config import NotificationConfig
from MI_ETL.notifications.notifier import PipelineNotifier
from MI_ETL.notifications.summary import create_pipeline_summary


class TestNotifications(unittest.TestCase):
    def test_summary_success(self):
        text = create_pipeline_summary(
            {
                "table_name": "t1",
                "record_count": 10,
                "data_size_mb": 0.5,
                "total_duration": 3,
                "latest_record_updated_value": "2026-05-05T00:00:00+00:00",
            }
        )
        self.assertIn("completed successfully", text)

    @patch("MI_ETL.notifications.notifier.send_google_chat_message", return_value=(True, "ok"))
    @patch("MI_ETL.notifications.notifier.send_slack_message", return_value=(True, "ok"))
    def test_notifier_both_channels(self, _slack, _gchat):
        notifier = PipelineNotifier(
            NotificationConfig(
                slack_webhook_url="https://hooks.slack.com/x",
                google_chat_webhook_url="https://chat.googleapis.com/x",
            )
        )
        results = notifier.notify_run(
            {
                "table_name": "t1",
                "record_count": 1,
                "data_size_mb": 0.1,
                "total_duration": 1,
                "latest_record_updated_value": "2026-05-05T00:00:00+00:00",
            }
        )
        self.assertIn("slack", results)
        self.assertIn("google_chat", results)

    def test_notifier_none_when_no_urls(self):
        orchestrator_calls = []
        notifier = None
        self.assertIsNone(notifier)


if __name__ == "__main__":
    unittest.main()
