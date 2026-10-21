"""Модульные тесты бизнес-логики (без HTTP, с подменой часов)."""

import json
import unittest

from demo.service import (
    DAY,
    HOUR,
    LISTING_PENDING,
    LISTING_PUBLISHED,
    LISTING_REJECTED,
    LISTING_SOLD,
    ORDER_CANCELLED,
    ORDER_PAID,
    ORDER_PENDING,
    PLANS,
    SUB_ACTIVE,
    SUB_EXPIRED,
    AgencyService,
    ServiceError,
    norm_district,
    sign_payload,
)

SECRET = "test-secret-change-me"
MSG_SECRET = "test-messenger-change-me"
START = 1_700_000_000.0

FLAT = {"title": "2-комнатная у парка, эксклюзив", "district": "Центральный",
        "price": 7_500_000, "rooms": 2, "area": 54.5}


class FakeClock:
    def __init__(self, start=START):
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
        self.svc = AgencyService(":memory:", SECRET, messenger_secret=MSG_SECRET,
                                 clock=self.clock, pending_ttl_seconds=1800)
        self.agent = self.svc.create_agent("Ирина Агентова")["agent_token"]

    def tearDown(self):
        self.svc.close()

    # -- помощники
    def pay(self, order, external_id="pay-1", amount=None, secret=SECRET, status="succeeded"):
        body = webhook_body(order["order_id"], external_id,
                            order["amount"] if amount is None else amount, status)
        return self.svc.handle_gateway_webhook(body, sign_payload(secret, body))

    def subscriber(self, email="client@example.org", plan="1m"):
        order = self.svc.create_order(plan, email=email)
        self.pay(order, external_id="pay-" + email)
        return order["client_key"]

    def published(self, **overrides):
        data = dict(FLAT, **overrides)
        listing_id = self.svc.submit_listing(self.agent, data)["listing_id"]
        self.svc.approve_listing(listing_id)
        return listing_id

    def actions(self):
        return [e["action"] for e in self.svc.audit_entries(limit=1000)]


class ListingTests(ServiceTestCase):
    def test_agent_submits_listing_for_moderation(self):
        res = self.svc.submit_listing(self.agent, FLAT)
        self.assertEqual(res["status"], LISTING_PENDING)
        queue = self.svc.moderation_queue()
        self.assertEqual([q["id"] for q in queue], [res["listing_id"]])

    def test_unknown_agent_token_rejected(self):
        with self.assertRaises(ServiceError) as ctx:
            self.svc.submit_listing("not-a-token", FLAT)
        self.assertEqual(ctx.exception.status, 401)
        self.assertEqual(self.svc.count("listings"), 0)

    def test_listing_validation(self):
        bad_cases = [
            ({"price": 50}, "invalid_price"),
            ({"rooms": 2.5}, "invalid_rooms"),
            ({"rooms": True}, "invalid_rooms"),
            ({"area": "много"}, "invalid_area"),
            ({"title": "кв"}, "invalid_title"),
            ({"district": ""}, "invalid_district"),
        ]
        for override, code in bad_cases:
            with self.subTest(code=code):
                with self.assertRaises(ServiceError) as ctx:
                    self.svc.submit_listing(self.agent, dict(FLAT, **override))
                self.assertEqual(ctx.exception.code, code)
        missing = dict(FLAT)
        del missing["price"]
        with self.assertRaises(ServiceError) as ctx:
            self.svc.submit_listing(self.agent, missing)
        self.assertEqual(ctx.exception.code, "missing_price")

    def test_approve_publishes_with_early_access_window(self):
        listing_id = self.published()
        self.assertEqual(self.svc.listing_status(listing_id), LISTING_PUBLISHED)
        key = self.subscriber()
        item = self.svc.catalog(key)[0]
        self.assertEqual(item["early_access_until"], START + AgencyService.EARLY_ACCESS_DAYS * DAY)

    def test_reject_requires_reason_and_hides_listing(self):
        listing_id = self.svc.submit_listing(self.agent, FLAT)["listing_id"]
        with self.assertRaises(ServiceError):
            self.svc.reject_listing(listing_id, "")
        self.svc.reject_listing(listing_id, "Нет подтверждения права продажи")
        self.assertEqual(self.svc.listing_status(listing_id), LISTING_REJECTED)
        self.assertEqual(self.svc.catalog(self.subscriber()), [])

    def test_moderation_twice_is_conflict(self):
        listing_id = self.published()
        with self.assertRaises(ServiceError) as ctx:
            self.svc.approve_listing(listing_id)
        self.assertEqual(ctx.exception.status, 409)
        with self.assertRaises(ServiceError) as ctx:
            self.svc.approve_listing("obj_missing")
        self.assertEqual(ctx.exception.status, 404)


class CatalogAccessTests(ServiceTestCase):
    def test_catalog_closed_without_subscription(self):
        self.published()
        order = self.svc.create_order("1m", email="guest@example.org")
        with self.assertRaises(ServiceError) as ctx:
            self.svc.catalog(order["client_key"])
        self.assertEqual((ctx.exception.status, ctx.exception.code), (403, "subscription_required"))

    def test_catalog_requires_valid_key(self):
        with self.assertRaises(ServiceError) as ctx:
            self.svc.catalog("forged-key")
        self.assertEqual(ctx.exception.status, 401)

    def test_subscriber_sees_only_published(self):
        published = self.published()
        self.svc.submit_listing(self.agent, dict(FLAT, title="Ещё на модерации, студия"))
        key = self.subscriber()
        self.assertEqual([i["id"] for i in self.svc.catalog(key)], [published])

    def test_detail_view_is_counted(self):
        listing_id = self.published()
        key = self.subscriber()
        detail = self.svc.listing_detail(key, listing_id)
        self.assertEqual(detail["agent_name"], "Ирина Агентова")
        self.assertEqual(self.svc.count("listing_views"), 1)

    def test_access_ends_exactly_at_expiry_even_before_job_runs(self):
        self.published()
        key = self.subscriber(plan="1m")
        self.clock.advance(30 * DAY - 1)
        self.assertEqual(len(self.svc.catalog(key)), 1)
        self.clock.advance(1)
        with self.assertRaises(ServiceError) as ctx:
            self.svc.catalog(key)
        self.assertEqual(ctx.exception.code, "subscription_required")


class OrderAndPaymentTests(ServiceTestCase):
    def test_order_uses_plan_price_and_returns_key_once(self):
        order = self.svc.create_order("3m", email="Client@Example.org")
        self.assertEqual(order["amount"], PLANS["3m"]["amount"])
        self.assertTrue(order["client_key"])
        self.assertEqual(self.svc.order_status(order["order_id"]), ORDER_PENDING)
        renewal = self.svc.create_order("1m", client_key=order["client_key"])
        self.assertNotIn("client_key", renewal)

    def test_second_key_for_same_email_refused(self):
        self.svc.create_order("1m", email="client@example.org")
        with self.assertRaises(ServiceError) as ctx:
            self.svc.create_order("1m", email="CLIENT@example.org")
        self.assertEqual((ctx.exception.status, ctx.exception.code), (409, "client_exists"))

    def test_invalid_plan_and_email_rejected(self):
        for kwargs, code in (({"plan": "7d", "email": "a@example.org"}, "invalid_plan"),
                             ({"plan": "1m", "email": "not-an-email"}, "invalid_email")):
            with self.subTest(code=code):
                with self.assertRaises(ServiceError) as ctx:
                    self.svc.create_order(**kwargs)
                self.assertEqual(ctx.exception.code, code)
        self.assertEqual(self.svc.count("orders"), 0)
        self.assertEqual(self.svc.count("clients"), 0)

    def test_bad_signature_rejected_and_logged(self):
        order = self.svc.create_order("1m", email="client@example.org")
        with self.assertRaises(ServiceError) as ctx:
            self.pay(order, secret="another-secret")
        self.assertEqual(ctx.exception.status, 403)
        self.assertEqual(self.svc.order_status(order["order_id"]), ORDER_PENDING)
        self.assertIn("webhook_rejected", self.actions())

    def test_tampered_body_rejected(self):
        order = self.svc.create_order("1m", email="client@example.org")
        body = webhook_body(order["order_id"], "pay-1", order["amount"])
        signature = sign_payload(SECRET, body)
        with self.assertRaises(ServiceError):
            self.svc.handle_gateway_webhook(body.replace(b"pay-1", b"pay-2"), signature)
        self.assertEqual(self.svc.count("payments"), 0)

    def test_payment_activates_subscription_immediately(self):
        order = self.svc.create_order("1m", email="client@example.org")
        res = self.pay(order)
        self.assertEqual(res.result, "processed")
        self.assertEqual(self.svc.order_status(order["order_id"]), ORDER_PAID)
        sub = self.svc.subscription(order["client_key"])
        self.assertEqual(sub, {"expires_at": START + 30 * DAY, "status": SUB_ACTIVE})
        self.assertTrue(self.svc.get_order(order["order_id"], order["client_key"])["subscription_active"])

    def test_repeated_webhook_credits_once(self):
        order = self.svc.create_order("1m", email="client@example.org")
        self.assertEqual(self.pay(order, external_id="pay-42").result, "processed")
        expires = self.svc.subscription(order["client_key"])["expires_at"]
        for _ in range(3):
            self.assertEqual(self.pay(order, external_id="pay-42").result, "duplicate")
        self.assertEqual(self.svc.count("payments"), 1)
        self.assertEqual(self.svc.subscription(order["client_key"])["expires_at"], expires)

    def test_extra_payment_for_paid_order_does_not_extend(self):
        order = self.svc.create_order("1m", email="client@example.org")
        self.pay(order, external_id="pay-1")
        res = self.pay(order, external_id="pay-2")
        self.assertEqual(res.result, "extra_payment")
        self.assertEqual(self.svc.subscription(order["client_key"])["expires_at"], START + 30 * DAY)
        self.assertIn("payment_extra_for_paid_order", self.actions())

    def test_amount_mismatch_not_activated(self):
        order = self.svc.create_order("12m", email="client@example.org")
        with self.assertRaises(ServiceError) as ctx:
            self.pay(order, amount=100)
        self.assertEqual(ctx.exception.status, 422)
        self.assertIsNone(self.svc.subscription(order["client_key"]))
        self.assertEqual(self.svc.count("payments"), 0)

    def test_failed_status_ignored_and_malformed_payload_rejected(self):
        order = self.svc.create_order("1m", email="client@example.org")
        self.assertEqual(self.pay(order, status="failed").result, "ignored")
        self.assertEqual(self.svc.order_status(order["order_id"]), ORDER_PENDING)
        body = b'{"order_id": "x"'
        with self.assertRaises(ServiceError) as ctx:
            self.svc.handle_gateway_webhook(body, sign_payload(SECRET, body))
        self.assertEqual(ctx.exception.code, "bad_payload")

    def test_renewal_extends_from_current_expiry(self):
        first = self.svc.create_order("1m", email="client@example.org")
        self.pay(first, external_id="pay-1")
        self.clock.advance(10 * DAY)
        second = self.svc.create_order("3m", client_key=first["client_key"])
        self.pay(second, external_id="pay-2")
        self.assertEqual(self.svc.subscription(first["client_key"])["expires_at"], START + 30 * DAY + 90 * DAY)

    def test_foreign_order_not_visible(self):
        mine = self.svc.create_order("1m", email="a@example.org")
        other = self.svc.create_order("1m", email="b@example.org")
        with self.assertRaises(ServiceError) as ctx:
            self.svc.get_order(mine["order_id"], other["client_key"])
        self.assertEqual(ctx.exception.status, 404)


class MessengerPaymentTests(ServiceTestCase):
    def body(self, order, charge="ch-1", units=None):
        return json.dumps({"order_id": order["order_id"], "charge_id": charge,
                           "units": order["units"] if units is None else units}).encode("utf-8")

    def test_messenger_payment_activates_and_is_idempotent(self):
        order = self.svc.create_order("3m", email="client@example.org")
        self.assertEqual(self.svc.handle_messenger_payment(MSG_SECRET, self.body(order)).result, "processed")
        self.assertEqual(self.svc.handle_messenger_payment(MSG_SECRET, self.body(order)).result, "duplicate")
        self.assertEqual(self.svc.subscription(order["client_key"])["expires_at"], START + 90 * DAY)
        self.assertEqual(self.svc.metrics()["subscription_revenue_units"], PLANS["3m"]["units"])

    def test_messenger_wrong_secret_or_amount(self):
        order = self.svc.create_order("1m", email="client@example.org")
        with self.assertRaises(ServiceError) as ctx:
            self.svc.handle_messenger_payment("guess", self.body(order))
        self.assertEqual(ctx.exception.status, 403)
        with self.assertRaises(ServiceError) as ctx:
            self.svc.handle_messenger_payment(MSG_SECRET, self.body(order, units=1))
        self.assertEqual(ctx.exception.status, 422)
        self.assertIsNone(self.svc.subscription(order["client_key"]))

    def test_messenger_disabled_without_secret(self):
        svc = AgencyService(":memory:", SECRET, clock=self.clock)
        try:
            order = svc.create_order("1m", email="client@example.org")
            with self.assertRaises(ServiceError) as ctx:
                svc.handle_messenger_payment("", self.body(order))
            self.assertEqual(ctx.exception.status, 403)
        finally:
            svc.close()


class ReconcileTests(ServiceTestCase):
    def test_stale_order_cancelled_fresh_kept(self):
        stale = self.svc.create_order("1m", email="a@example.org")
        self.clock.advance(25 * 60)
        fresh = self.svc.create_order("1m", email="b@example.org")
        self.clock.advance(10 * 60)  # первому 35 мин, второму 10 мин
        report = self.svc.reconcile_stale_orders()
        self.assertEqual(report, {"checked": 1, "paid": 0, "cancelled": 1, "duplicate": 0})
        self.assertEqual(self.svc.order_status(stale["order_id"]), ORDER_CANCELLED)
        self.assertEqual(self.svc.order_status(fresh["order_id"]), ORDER_PENDING)

    def test_reconcile_finds_lost_payment_and_late_webhook_is_duplicate(self):
        order = self.svc.create_order("1m", email="client@example.org")
        self.clock.advance(40 * 60)
        report = self.svc.reconcile_stale_orders(
            lambda _id: {"status": "succeeded", "external_id": "pay-lost", "amount": order["amount"]})
        self.assertEqual(report["paid"], 1)
        self.assertEqual(self.svc.order_status(order["order_id"]), ORDER_PAID)
        self.assertEqual(self.pay(order, external_id="pay-lost").result, "duplicate")
        self.assertEqual(self.svc.count("payments"), 1)

    def test_late_payment_after_cancel_still_activates(self):
        order = self.svc.create_order("1m", email="client@example.org")
        self.clock.advance(31 * 60)
        self.svc.reconcile_stale_orders()
        self.assertEqual(self.pay(order, external_id="pay-late").result, "processed")
        self.assertEqual(self.svc.order_status(order["order_id"]), ORDER_PAID)
        self.assertIn("late_payment", self.actions())

    def test_reconcile_is_repeatable(self):
        self.svc.create_order("1m", email="client@example.org")
        self.clock.advance(31 * 60)
        self.assertEqual(self.svc.reconcile_stale_orders()["cancelled"], 1)
        self.assertEqual(self.svc.reconcile_stale_orders()["checked"], 0)


class MatchingTests(ServiceTestCase):
    def test_new_listing_queued_for_matching_subscribers_only(self):
        fits = self.subscriber("fits@example.org")
        self.svc.save_search(fits, {"district": "  центральный ", "price_max": 8_000_000,
                                    "rooms_min": 2, "rooms_max": 3})
        too_cheap = self.subscriber("cheap@example.org")
        self.svc.save_search(too_cheap, {"price_max": 5_000_000})
        other_district = self.subscriber("north@example.org")
        self.svc.save_search(other_district, {"district": "Северный"})
        not_paid = self.svc.create_order("1m", email="guest@example.org")["client_key"]
        self.svc.save_search(not_paid, {"district": "Центральный"})

        listing_id = self.svc.submit_listing(self.agent, FLAT)["listing_id"]
        res = self.svc.approve_listing(listing_id)
        self.assertEqual(res["queued_notifications"], 1)
        self.assertEqual([n["kind"] for n in self.svc.notifications_for(fits)], ["new_listing"])
        for key in (too_cheap, other_district, not_paid):
            self.assertEqual(self.svc.notifications_for(key), [])

    def test_two_matching_searches_give_one_notification(self):
        key = self.subscriber()
        self.svc.save_search(key, {"district": "Центральный"})
        self.svc.save_search(key, {"rooms_min": 1})
        self.published()
        self.assertEqual(len(self.svc.notifications_for(key)), 1)

    def test_empty_search_matches_everything(self):
        key = self.subscriber()
        self.svc.save_search(key, {})
        self.published(rooms=0, district="Заречный", title="Студия в новостройке, закрытые продажи")
        self.assertEqual(self.svc.notifications_for(key)[0]["payload"]["district"], "Заречный")

    def test_search_validation_and_limit(self):
        key = self.subscriber()
        with self.assertRaises(ServiceError) as ctx:
            self.svc.save_search(key, {"rooms_min": 3, "rooms_max": 1})
        self.assertEqual(ctx.exception.code, "invalid_rooms_range")
        for _ in range(AgencyService.MAX_SEARCHES_PER_CLIENT):
            self.svc.save_search(key, {"rooms_min": 1})
        with self.assertRaises(ServiceError) as ctx:
            self.svc.save_search(key, {"rooms_min": 1})
        self.assertEqual(ctx.exception.code, "too_many_searches")

    def test_dispatch_marks_sent_retries_failures_and_measures_delay(self):
        key = self.subscriber()
        self.svc.save_search(key, {"district": "Центральный"})
        self.published()
        self.assertEqual(self.svc.dispatch_notifications(), {"sent": 1, "failed": 0})   # задержка 0 с
        self.assertIsNotNone(self.svc.notifications_for(key)[0]["sent_at"])

        other = self.subscriber("b@example.org")
        self.svc.save_search(other, {})
        self.published(title="Трёшка у набережной, ранний доступ", rooms=3, district="Северный")

        def broken(_email, _kind, _payload):
            raise OSError("почтовый сервис недоступен")

        self.assertEqual(self.svc.dispatch_notifications(broken), {"sent": 0, "failed": 1})
        self.clock.advance(120)
        sent = []
        self.assertEqual(self.svc.dispatch_notifications(lambda e, k, p: sent.append((e, k))),
                         {"sent": 1, "failed": 0})
        self.assertEqual(sent, [("b@example.org", "new_listing")])
        self.assertEqual(self.svc.metrics()["avg_notify_delay_seconds"], 60.0)

    def test_norm_district(self):
        self.assertEqual(norm_district("  Центральный   РАЙОН "), "центральный район")


class ViewingTests(ServiceTestCase):
    def setUp(self):
        super().setUp()
        self.listing = self.published()
        self.key = self.subscriber()
        self.slot = START + 2 * DAY

    def test_full_viewing_flow_with_single_reminder(self):
        vw = self.svc.request_viewing(self.key, self.listing, self.slot)
        self.assertEqual(self.svc.agent_viewings(self.agent)[0]["status"], "requested")
        self.svc.confirm_viewing(self.agent, vw["viewing_id"])
        self.assertEqual(self.svc.send_viewing_reminders(), 0)      # до показа больше суток
        self.clock.advance(DAY + HOUR)
        self.assertEqual(self.svc.send_viewing_reminders(), 1)
        self.assertEqual(self.svc.send_viewing_reminders(), 0)      # второе напоминание не нужно
        kinds = [n["kind"] for n in self.svc.notifications_for(self.key)]
        self.assertEqual(kinds, ["viewing_confirmed", "viewing_reminder"])
        self.clock.advance(DAY)
        self.assertEqual(self.svc.complete_viewing(self.agent, vw["viewing_id"])["status"], "done")

    def test_slot_conflict_and_bounds(self):
        self.svc.request_viewing(self.key, self.listing, self.slot)
        other = self.subscriber("b@example.org")
        with self.assertRaises(ServiceError) as ctx:
            self.svc.request_viewing(other, self.listing, self.slot)
        self.assertEqual(ctx.exception.code, "slot_taken")
        for bad in (START + 10 * 60, START + 40 * DAY, "завтра"):
            with self.subTest(slot=bad):
                with self.assertRaises(ServiceError) as ctx:
                    self.svc.request_viewing(self.key, self.listing, bad)
                self.assertEqual(ctx.exception.code, "invalid_slot")

    def test_viewing_requires_subscription(self):
        guest = self.svc.create_order("1m", email="guest@example.org")["client_key"]
        with self.assertRaises(ServiceError) as ctx:
            self.svc.request_viewing(guest, self.listing, self.slot)
        self.assertEqual(ctx.exception.status, 403)

    def test_other_agent_cannot_confirm(self):
        vw = self.svc.request_viewing(self.key, self.listing, self.slot)
        stranger = self.svc.create_agent("Чужой агент")["agent_token"]
        with self.assertRaises(ServiceError) as ctx:
            self.svc.confirm_viewing(stranger, vw["viewing_id"])
        self.assertEqual(ctx.exception.status, 404)
        with self.assertRaises(ServiceError) as ctx:
            self.svc.complete_viewing(self.agent, vw["viewing_id"])   # ещё не подтверждён
        self.assertEqual(ctx.exception.status, 409)

    def test_deal_closes_listing_and_counts_in_funnel(self):
        self.svc.listing_detail(self.key, self.listing)
        vw = self.svc.request_viewing(self.key, self.listing, self.slot)
        self.svc.confirm_viewing(self.agent, vw["viewing_id"])
        self.svc.complete_viewing(self.agent, vw["viewing_id"])
        self.svc.close_deal(self.agent, self.listing, 150_000)
        self.assertEqual(self.svc.listing_status(self.listing), LISTING_SOLD)
        self.assertEqual(self.svc.catalog(self.key), [])
        m = self.svc.metrics()
        self.assertEqual(m["funnel"], {"views": 1, "viewings_requested": 1, "viewings_done": 1, "deals": 1,
                                       "view_to_viewing": 1.0, "viewing_to_deal": 1.0})
        self.assertEqual(m["by_agent"][0]["commission"], 150_000)
        with self.assertRaises(ServiceError) as ctx:
            self.svc.close_deal(self.agent, self.listing, 1)
        self.assertEqual(ctx.exception.status, 409)


class LifecycleTests(ServiceTestCase):
    def test_warning_then_expiry_then_renewal(self):
        key = self.subscriber(plan="1m")
        self.clock.advance(26 * DAY)
        self.assertEqual(self.svc.run_subscription_lifecycle(), {"expiring_notified": 0, "expired": 0})
        self.clock.advance(2 * DAY)                               # до конца меньше 3 дней
        self.assertEqual(self.svc.run_subscription_lifecycle(), {"expiring_notified": 1, "expired": 0})
        self.assertEqual(self.svc.run_subscription_lifecycle(), {"expiring_notified": 0, "expired": 0})
        self.clock.advance(3 * DAY)
        self.assertEqual(self.svc.run_subscription_lifecycle(), {"expiring_notified": 0, "expired": 1})
        self.assertEqual(self.svc.subscription(key)["status"], SUB_EXPIRED)
        self.assertIn("subscription_expired", self.actions())
        # Продление после окончания считается от сегодняшнего дня.
        renewal = self.svc.create_order("1m", client_key=key)
        self.pay(renewal, external_id="pay-renew")
        self.assertEqual(self.svc.subscription(key), {"expires_at": self.clock.now + 30 * DAY,
                                                      "status": SUB_ACTIVE})

    def test_expired_subscriber_gets_no_new_listing_notifications(self):
        key = self.subscriber(plan="1m")
        self.svc.save_search(key, {})
        self.clock.advance(31 * DAY)
        self.svc.run_subscription_lifecycle()
        self.published()
        kinds = [n["kind"] for n in self.svc.notifications_for(key)]
        self.assertNotIn("new_listing", kinds)
        self.assertIn("subscription_expired", kinds)


class HealthAuditMetricsTests(ServiceTestCase):
    def test_health(self):
        self.svc.create_order("1m", email="client@example.org")
        self.svc.submit_listing(self.agent, FLAT)
        self.assertEqual(self.svc.health(), {"status": "ok", "database": "ok", "pending_orders": 1,
                                             "pending_moderation": 1, "queued_notifications": 0})

    def test_audit_has_no_secrets_or_email(self):
        order = self.svc.create_order("1m", email="private.person@example.org")
        self.pay(order)
        agent = self.svc.create_agent("Агент Б")
        dump = json.dumps(self.svc.audit_entries(limit=1000), ensure_ascii=False)
        for value in (SECRET, order["client_key"], agent["agent_token"], self.agent, "private.person"):
            self.assertNotIn(value, dump)

    def test_audit_records_actor_for_each_step(self):
        listing_id = self.svc.submit_listing(self.agent, FLAT)["listing_id"]
        self.svc.approve_listing(listing_id, moderator="moderator")
        entries = {e["action"]: e["actor"] for e in self.svc.audit_entries()}
        self.assertTrue(entries["listing_submitted"].startswith("agent:"))
        self.assertEqual(entries["listing_published"], "moderator")
        self.assertEqual(entries["agent_created"], "owner")

    def test_run_jobs_combines_all_scheduled_work(self):
        self.svc.create_order("1m", email="a@example.org")
        self.clock.advance(31 * 60)
        report = self.svc.run_jobs()
        self.assertEqual(report["reconcile"]["cancelled"], 1)
        self.assertEqual(set(report), {"reconcile", "subscriptions", "viewing_reminders", "notifications"})

    def test_metrics_active_subscribers(self):
        self.subscriber("a@example.org")
        self.subscriber("b@example.org", plan="12m")
        self.assertEqual(self.svc.metrics()["active_subscribers"], 2)
        self.clock.advance(31 * DAY)
        self.assertEqual(self.svc.metrics()["active_subscribers"], 1)

    def test_empty_secret_refused(self):
        with self.assertRaises(ValueError):
            AgencyService(":memory:", "")


if __name__ == "__main__":
    unittest.main()
