"""Сквозные тесты HTTP-слоя: настоящий сервер на свободном порту + urllib."""

import json
import threading
import unittest
import urllib.error
import urllib.request

from demo.app import build_server
from demo.service import SubscriptionService, sign_payload

SECRET = "test-secret-change-me"
ADMIN = "test-admin-change-me"


class HttpTests(unittest.TestCase):
    def setUp(self):
        self.svc = SubscriptionService(":memory:", SECRET)
        self.server = build_server(self.svc, port=0, admin_token=ADMIN)
        self.base = "http://127.0.0.1:%d" % self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self.svc.close()

    def request(self, method, path, body=None, headers=None):
        data = body if isinstance(body, (bytes, type(None))) else json.dumps(body).encode("utf-8")
        req = urllib.request.Request(self.base + path, data=data, method=method, headers=headers or {})
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as err:
            return err.code, json.loads(err.read())

    def webhook(self, order, external_id="pay-1", secret=SECRET):
        body = json.dumps(
            {"order_id": order["order_id"], "external_id": external_id, "amount": order["amount"]}
        ).encode("utf-8")
        return self.request(
            "POST", "/webhooks/payment", body,
            {"X-Signature": sign_payload(secret, body), "Content-Type": "application/json"},
        )

    def test_health(self):
        status, payload = self.request("GET", "/health")
        self.assertEqual(status, 200)
        self.assertEqual(payload["status"], "ok")

    def test_full_flow_order_pay_token(self):
        status, order = self.request("POST", "/orders", {"email": "client@example.org", "plan": "1m"})
        self.assertEqual(status, 201)
        status, view = self.request("GET", "/orders/" + order["order_id"], headers={"X-Order-Key": order["order_key"]})
        self.assertEqual((status, view["status"]), (200, "pending"))
        status, res = self.webhook(order)
        self.assertEqual((status, res["result"]), (200, "processed"))
        status, view = self.request("GET", "/orders/" + order["order_id"], headers={"X-Order-Key": order["order_key"]})
        self.assertEqual(view["status"], "paid")
        self.assertIn("access_token", view)
        self.assertTrue(self.svc.check_access("client@example.org", view["access_token"]))

    def test_bad_signature_returns_403(self):
        _, order = self.request("POST", "/orders", {"email": "client@example.org", "plan": "1m"})
        status, payload = self.webhook(order, secret="wrong-secret")
        self.assertEqual((status, payload["error"]), (403, "bad_signature"))
        self.assertEqual(self.svc.order_status(order["order_id"]), "pending")

    def test_missing_signature_returns_403(self):
        status, _ = self.request("POST", "/webhooks/payment", {"order_id": "x"})
        self.assertEqual(status, 403)

    def test_duplicate_webhook_single_credit(self):
        _, order = self.request("POST", "/orders", {"email": "client@example.org", "plan": "3m"})
        self.assertEqual(self.webhook(order, "pay-7")[1]["result"], "processed")
        self.assertEqual(self.webhook(order, "pay-7")[1]["result"], "duplicate")
        self.assertEqual(self.svc.count("payments"), 1)

    def test_bad_order_input(self):
        status, payload = self.request("POST", "/orders", {"email": "client@example.org", "plan": "forever"})
        self.assertEqual((status, payload["error"]), (400, "invalid_plan"))
        status, payload = self.request("POST", "/orders", b"not json", {"Content-Type": "application/json"})
        self.assertEqual((status, payload["error"]), (400, "bad_json"))

    def test_admin_requires_token(self):
        status, _ = self.request("GET", "/admin/audit")
        self.assertEqual(status, 403)
        status, _ = self.request("GET", "/admin/audit", headers={"X-Admin-Token": "nope"})
        self.assertEqual(status, 403)
        self.request("POST", "/orders", {"email": "client@example.org", "plan": "1m"})
        status, payload = self.request("GET", "/admin/audit", headers={"X-Admin-Token": ADMIN})
        self.assertEqual(status, 200)
        self.assertEqual(payload["entries"][0]["action"], "order_created")

    def test_admin_reconcile_endpoint(self):
        status, report = self.request("POST", "/admin/reconcile", b"", {"X-Admin-Token": ADMIN})
        self.assertEqual(status, 200)
        self.assertEqual(report["checked"], 0)

    def test_unknown_path(self):
        status, _ = self.request("GET", "/nowhere")
        self.assertEqual(status, 404)


class AdminDisabledTests(unittest.TestCase):
    def test_admin_closed_without_token_configured(self):
        svc = SubscriptionService(":memory:", SECRET)
        server = build_server(svc, port=0, admin_token=None)
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()
        try:
            url = "http://127.0.0.1:%d/admin/audit" % server.server_address[1]
            req = urllib.request.Request(url, headers={"X-Admin-Token": ""})
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                urllib.request.urlopen(req, timeout=5)
            self.assertEqual(ctx.exception.code, 403)
            ctx.exception.close()
        finally:
            server.shutdown()
            server.server_close()
            t.join(timeout=5)
            svc.close()


if __name__ == "__main__":
    unittest.main()
