"""Модульные тесты бизнес-логики (без HTTP)."""

import json
import unittest

from demo.service import (
    DAY,
    ORDER_CANCELLED,
    ORDER_PAID,
    ORDER_PENDING,
    PLANS,
    ServiceError,
    SubscriptionService,
    sign_payload,
)

SECRET = "test-secret-change-me"


class FakeClock:
    def __init__(self, start=1_700_000_000.0):
        self.now = start

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def webhook_body(order_id, external_id, amount, status="succeeded"):
    return json.dumps(
        {"order_id": order_id, "external_id": external_id, "amount": amount, "status": status}
    ).encode("utf-8")


class ServiceTestCase(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock()
        self.svc = SubscriptionService(":memory:", SECRET, clock=self.clock, pending_ttl_seconds=1800)

    def tearDown(self):
        self.svc.close()

    def pay(self, order, external_id="pay-1", amount=None, secret=SECRET):
        body = webhook_body(order["order_id"], external_id, order["amount"] if amount is None else amount)
        return self.svc.handle_webhook(body, sign_payload(secret, body))


class OrderTests(ServiceTestCase):
    def test_create_order_uses_plan_price(self):
        order = self.svc.create_order("Client@Example.org", "3m")
        self.assertEqual(order["amount"], PLANS["3m"][0])
        self.assertEqual(self.svc.order_status(order["order_id"]), ORDER_PENDING)
        self.assertTrue(order["order_key"])

    def test_invalid_plan_and_email_rejected(self):
        with self.assertRaises(ServiceError) as ctx:
            self.svc.create_order("client@example.org", "7d")
        self.assertEqual(ctx.exception.code, "invalid_plan")
        with self.assertRaises(ServiceError) as ctx:
            self.svc.create_order("not-an-email", "1m")
        self.assertEqual(ctx.exception.code, "invalid_email")
        self.assertEqual(self.svc.count("orders"), 0)

    def test_wrong_order_key_looks_like_missing_order(self):
        order = self.svc.create_order("client@example.org", "1m")
        with self.assertRaises(ServiceError) as ctx:
            self.svc.get_order_for_customer(order["order_id"], "wrong-key")
        self.assertEqual(ctx.exception.status, 404)


class WebhookTests(ServiceTestCase):
    def test_bad_signature_rejected_and_logged(self):
        order = self.svc.create_order("client@example.org", "1m")
        with self.assertRaises(ServiceError) as ctx:
            self.pay(order, secret="another-secret")
        self.assertEqual(ctx.exception.status, 403)
        self.assertEqual(self.svc.order_status(order["order_id"]), ORDER_PENDING)
        self.assertEqual(self.svc.count("payments"), 0)
        actions = [e["action"] for e in self.svc.audit_entries()]
        self.assertIn("webhook_rejected", actions)

    def test_tampered_body_rejected(self):
        order = self.svc.create_order("client@example.org", "1m")
        body = webhook_body(order["order_id"], "pay-1", order["amount"])
        signature = sign_payload(SECRET, body)
        tampered = body.replace(b"pay-1", b"pay-2")
        with self.assertRaises(ServiceError):
            self.svc.handle_webhook(tampered, signature)

    def test_payment_activates_subscription_and_token_given_once(self):
        order = self.svc.create_order("client@example.org", "1m")
        res = self.pay(order)
        self.assertEqual(res.result, "processed")
        self.assertEqual(self.svc.order_status(order["order_id"]), ORDER_PAID)
        self.assertAlmostEqual(
            self.svc.subscription_expires_at("client@example.org"), self.clock.now + 30 * DAY
        )
        first = self.svc.get_order_for_customer(order["order_id"], order["order_key"])
        token = first["access_token"]
        self.assertTrue(self.svc.check_access("client@example.org", token))
        second = self.svc.get_order_for_customer(order["order_id"], order["order_key"])
        self.assertNotIn("access_token", second)

    def test_token_stored_only_as_hash(self):
        order = self.svc.create_order("client@example.org", "1m")
        self.pay(order)
        token = self.svc.get_order_for_customer(order["order_id"], order["order_key"])["access_token"]
        row = self.svc._db.execute("SELECT token_hash FROM subscriptions").fetchone()
        self.assertNotEqual(row["token_hash"], token)
        leftovers = self.svc._db.execute("SELECT token FROM deliveries").fetchone()
        self.assertIsNone(leftovers["token"])

    def test_repeated_webhook_credits_once(self):
        order = self.svc.create_order("client@example.org", "1m")
        first = self.pay(order, external_id="pay-42")
        expires = self.svc.subscription_expires_at("client@example.org")
        for _ in range(3):
            again = self.pay(order, external_id="pay-42")
            self.assertEqual(again.result, "duplicate")
        self.assertEqual(first.result, "processed")
        self.assertEqual(self.svc.count("payments"), 1)
        self.assertEqual(self.svc.subscription_expires_at("client@example.org"), expires)

    def test_amount_mismatch_not_activated(self):
        order = self.svc.create_order("client@example.org", "12m")
        with self.assertRaises(ServiceError) as ctx:
            self.pay(order, amount=100)
        self.assertEqual(ctx.exception.status, 422)
        self.assertIsNone(self.svc.subscription_expires_at("client@example.org"))
        self.assertEqual(self.svc.count("payments"), 0)

    def test_unknown_order(self):
        body = webhook_body("ord_missing", "pay-1", 100)
        with self.assertRaises(ServiceError) as ctx:
            self.svc.handle_webhook(body, sign_payload(SECRET, body))
        self.assertEqual(ctx.exception.status, 404)

    def test_signed_but_malformed_payload(self):
        body = b'{"order_id": "x"'
        with self.assertRaises(ServiceError) as ctx:
            self.svc.handle_webhook(body, sign_payload(SECRET, body))
        self.assertEqual(ctx.exception.code, "bad_payload")

    def test_failed_status_is_ignored(self):
        order = self.svc.create_order("client@example.org", "1m")
        body = webhook_body(order["order_id"], "pay-1", order["amount"], status="failed")
        res = self.svc.handle_webhook(body, sign_payload(SECRET, body))
        self.assertEqual(res.result, "ignored")
        self.assertEqual(self.svc.order_status(order["order_id"]), ORDER_PENDING)

    def test_renewal_extends_from_current_expiry(self):
        first = self.svc.create_order("client@example.org", "1m")
        self.pay(first, external_id="pay-1")
        self.clock.advance(10 * DAY)
        second = self.svc.create_order("client@example.org", "3m")
        self.pay(second, external_id="pay-2")
        start = 1_700_000_000.0
        self.assertAlmostEqual(
            self.svc.subscription_expires_at("client@example.org"), start + 30 * DAY + 90 * DAY
        )


class ReconcileTests(ServiceTestCase):
    def test_stale_order_cancelled(self):
        stale = self.svc.create_order("a@example.org", "1m")
        self.clock.advance(25 * 60)
        fresh = self.svc.create_order("b@example.org", "1m")
        self.clock.advance(10 * 60)  # stale: 35 мин, fresh: 10 мин
        report = self.svc.reconcile_stale_orders()
        self.assertEqual(report, {"checked": 1, "paid": 0, "cancelled": 1, "duplicate": 0})
        self.assertEqual(self.svc.order_status(stale["order_id"]), ORDER_CANCELLED)
        self.assertEqual(self.svc.order_status(fresh["order_id"]), ORDER_PENDING)
        actions = [e["action"] for e in self.svc.audit_entries()]
        self.assertIn("order_cancelled_stale", actions)

    def test_reconcile_finds_lost_payment(self):
        order = self.svc.create_order("client@example.org", "1m")
        self.clock.advance(40 * 60)

        def checker(order_id):
            return {"status": "succeeded", "external_id": "pay-lost", "amount": order["amount"]}

        report = self.svc.reconcile_stale_orders(status_checker=checker)
        self.assertEqual(report["paid"], 1)
        self.assertEqual(self.svc.order_status(order["order_id"]), ORDER_PAID)
        # Запоздавший webhook с тем же внешним id не даёт второго начисления.
        res = self.pay(order, external_id="pay-lost")
        self.assertEqual(res.result, "duplicate")
        self.assertEqual(self.svc.count("payments"), 1)

    def test_late_payment_after_cancel_still_activates(self):
        order = self.svc.create_order("client@example.org", "1m")
        self.clock.advance(31 * 60)
        self.svc.reconcile_stale_orders()
        self.assertEqual(self.svc.order_status(order["order_id"]), ORDER_CANCELLED)
        res = self.pay(order, external_id="pay-late")
        self.assertEqual(res.result, "processed")
        self.assertEqual(self.svc.order_status(order["order_id"]), ORDER_PAID)
        actions = [e["action"] for e in self.svc.audit_entries()]
        self.assertIn("late_payment", actions)

    def test_reconcile_is_repeatable(self):
        self.svc.create_order("client@example.org", "1m")
        self.clock.advance(31 * 60)
        self.assertEqual(self.svc.reconcile_stale_orders()["cancelled"], 1)
        self.assertEqual(self.svc.reconcile_stale_orders()["checked"], 0)


class HealthAndAuditTests(ServiceTestCase):
    def test_health(self):
        self.svc.create_order("client@example.org", "1m")
        self.assertEqual(self.svc.health(), {"status": "ok", "database": "ok", "pending_orders": 1})

    def test_audit_does_not_store_secrets(self):
        order = self.svc.create_order("client@example.org", "1m")
        self.pay(order)
        token = self.svc.get_order_for_customer(order["order_id"], order["order_key"])["access_token"]
        dump = json.dumps(self.svc.audit_entries(), ensure_ascii=False)
        for secret_value in (SECRET, token, order["order_key"]):
            self.assertNotIn(secret_value, dump)

    def test_empty_secret_refused(self):
        with self.assertRaises(ValueError):
            SubscriptionService(":memory:", "")


if __name__ == "__main__":
    unittest.main()
