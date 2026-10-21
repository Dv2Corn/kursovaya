"""Сквозные тесты HTTP-слоя: настоящий сервер на свободном порту + urllib."""

import json
import threading
import time
import unittest
import urllib.error
import urllib.request

from demo.app import build_server
from demo.service import DAY, AgencyService, sign_payload

SECRET = "test-secret-change-me"
MSG_SECRET = "test-messenger-change-me"
OWNER = "test-owner-change-me"
MODERATOR = "test-moderator-change-me"

FLAT = {"title": "2-комнатная у парка, эксклюзив", "district": "Центральный",
        "price": 7_500_000, "rooms": 2, "area": 54.5}


class HttpTestCase(unittest.TestCase):
    def setUp(self):
        self.svc = AgencyService(":memory:", SECRET, messenger_secret=MSG_SECRET)
        self.server = build_server(self.svc, port=0, admin_token=OWNER, moderator_token=MODERATOR)
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
            try:
                return err.code, json.loads(err.read())
            finally:
                err.close()

    def webhook(self, order, external_id="pay-1", secret=SECRET):
        body = json.dumps({"order_id": order["order_id"], "external_id": external_id,
                           "amount": order["amount"], "status": "succeeded"}).encode("utf-8")
        return self.request("POST", "/webhooks/payment", body,
                            {"X-Signature": sign_payload(secret, body), "Content-Type": "application/json"})

    def new_agent(self):
        status, agent = self.request("POST", "/admin/agents", {"name": "Ирина"}, {"X-Admin-Token": OWNER})
        self.assertEqual(status, 201)
        return {"X-Agent-Token": agent["agent_token"]}

    def register(self, email="client@example.org"):
        """Регистрация по HTTP; ключ «приходит письмом» — читаем его из outbox демо."""
        status, payload = self.request("POST", "/clients", {"email": email})
        self.assertEqual(status, 202)
        key = [m for m in self.svc.outbox_for(email) if m["kind"] == "client_key"][-1]["body"]
        return {"X-Client-Key": key}

    def order(self, plan="1m", email="client@example.org"):
        client = self.register(email)
        status, order = self.request("POST", "/orders", {"plan": plan}, client)
        self.assertEqual(status, 201)
        return order, client


class HttpFlowTests(HttpTestCase):
    def test_health(self):
        status, payload = self.request("GET", "/health")
        self.assertEqual((status, payload["status"], payload["database"]), (200, "ok", "ok"))

    def test_full_flow_listing_subscription_catalog_viewing(self):
        agent = self.new_agent()
        # Клиент без подписки: каталог закрыт.
        order, client = self.order("3m")
        status, payload = self.request("GET", "/catalog", headers=client)
        self.assertEqual((status, payload["error"]), (403, "subscription_required"))
        self.assertEqual(self.request("POST", "/searches", {"district": "центральный"}, client)[0], 201)
        # Оплата → подписка активна сразу.
        self.assertEqual(self.webhook(order)[1]["result"], "processed")
        status, view = self.request("GET", "/orders/" + order["order_id"], headers=client)
        self.assertEqual((status, view["status"], view["subscription_active"]), (200, "paid", True))
        # Агент заносит объект, модератор публикует → уведомление в очереди.
        status, listing = self.request("POST", "/listings", FLAT, agent)
        self.assertEqual((status, listing["status"]), (201, "pending"))
        status, res = self.request("POST", "/admin/listings/%s/approve" % listing["listing_id"], b"",
                                   {"X-Admin-Token": MODERATOR})
        self.assertEqual((status, res["queued_notifications"]), (200, 1))
        _, catalog = self.request("GET", "/catalog", headers=client)
        self.assertEqual([i["id"] for i in catalog["listings"]], [listing["listing_id"]])
        _, notes = self.request("GET", "/notifications", headers=client)
        self.assertEqual(notes["notifications"][0]["kind"], "new_listing")
        # Запись на показ и подтверждение агентом.
        status, vw = self.request("POST", "/viewings",
                                  {"listing_id": listing["listing_id"], "slot_at": time.time() + 2 * DAY}, client)
        self.assertEqual(status, 201)
        status, conf = self.request("POST", "/viewings/%s/confirm" % vw["viewing_id"], b"", agent)
        self.assertEqual((status, conf["status"]), (200, "confirmed"))
        # Руководитель видит воронку.
        status, metrics = self.request("GET", "/admin/metrics", headers={"X-Admin-Token": OWNER})
        self.assertEqual((status, metrics["active_subscribers"], metrics["funnel"]["viewings_requested"]),
                         (200, 1, 1))

    def test_bad_signature_returns_403(self):
        order, _ = self.order("1m")
        status, payload = self.webhook(order, secret="wrong-secret")
        self.assertEqual((status, payload["error"]), (403, "bad_signature"))
        self.assertEqual(self.svc.order_status(order["order_id"]), "pending")
        status, _ = self.request("POST", "/webhooks/payment", {"order_id": "x"})
        self.assertEqual(status, 403)

    def test_duplicate_webhook_single_credit(self):
        order, _ = self.order("3m")
        self.assertEqual(self.webhook(order, "pay-7")[1]["result"], "processed")
        self.assertEqual(self.webhook(order, "pay-7")[1]["result"], "duplicate")
        self.assertEqual(self.svc.count("payments"), 1)

    def test_messenger_payment_endpoint(self):
        order, _ = self.order("1m")
        body = {"order_id": order["order_id"], "charge_id": "ch-1", "units": order["units"]}
        self.assertEqual(self.request("POST", "/webhooks/messenger", body, {"X-Messenger-Secret": "nope"})[0], 403)
        status, res = self.request("POST", "/webhooks/messenger", body, {"X-Messenger-Secret": MSG_SECRET})
        self.assertEqual((status, res["result"]), (200, "processed"))

    def test_registration_does_not_reveal_email_or_key(self):
        first = self.request("POST", "/clients", {"email": "client@example.org"})
        second = self.request("POST", "/clients", {"email": "client@example.org"})
        self.assertEqual(first, second)
        self.assertEqual(first[0], 202)
        self.assertNotIn("key", json.dumps(first[1]))
        self.assertEqual(self.request("POST", "/orders", {"plan": "1m"})[0], 401)

    def test_bad_input(self):
        client = self.register()
        status, payload = self.request("POST", "/orders", {"plan": "forever"}, client)
        self.assertEqual((status, payload["error"]), (400, "invalid_plan"))
        status, payload = self.request("POST", "/orders", b"not json", dict(client, **{"Content-Type": "application/json"}))
        self.assertEqual((status, payload["error"]), (400, "bad_json"))
        status, payload = self.request("POST", "/clients", {"email": "nope"})
        self.assertEqual((status, payload["error"]), (400, "invalid_email"))
        status, payload = self.request("POST", "/listings", dict(FLAT, rooms=99), self.new_agent())
        self.assertEqual((status, payload["error"]), (400, "invalid_rooms"))
        self.assertEqual(self.request("GET", "/nowhere")[0], 404)


class HttpRoleTests(HttpTestCase):
    def test_admin_requires_token(self):
        self.assertEqual(self.request("GET", "/admin/audit")[0], 403)
        self.assertEqual(self.request("GET", "/admin/audit", headers={"X-Admin-Token": "nope"})[0], 403)
        status, payload = self.request("GET", "/admin/audit", headers={"X-Admin-Token": OWNER})
        self.assertEqual(status, 200)
        self.assertIsInstance(payload["entries"], list)

    def test_moderator_can_moderate_but_not_see_audit_or_create_agents(self):
        mod = {"X-Admin-Token": MODERATOR}
        self.assertEqual(self.request("GET", "/admin/listings", headers=mod)[0], 200)
        self.assertEqual(self.request("GET", "/admin/audit", headers=mod)[0], 403)
        self.assertEqual(self.request("GET", "/admin/metrics", headers=mod)[0], 403)
        self.assertEqual(self.request("POST", "/admin/agents", {"name": "Х"}, mod)[0], 403)

    def test_reject_via_http_and_agent_token_required(self):
        agent = self.new_agent()
        self.assertEqual(self.request("POST", "/listings", FLAT)[0], 401)
        _, listing = self.request("POST", "/listings", FLAT, agent)
        status, res = self.request("POST", "/admin/listings/%s/reject" % listing["listing_id"],
                                   {"reason": "Цена не подтверждена продавцом"}, {"X-Admin-Token": OWNER})
        self.assertEqual((status, res["status"]), (200, "rejected"))

    def test_run_jobs_endpoint(self):
        status, report = self.request("POST", "/admin/run-jobs", b"", {"X-Admin-Token": OWNER})
        self.assertEqual(status, 200)
        self.assertEqual(report["reconcile"]["checked"], 0)


class AdminDisabledTests(unittest.TestCase):
    def test_admin_closed_without_tokens_configured(self):
        svc = AgencyService(":memory:", SECRET)
        server = build_server(svc, port=0)
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()
        try:
            url = "http://127.0.0.1:%d/admin/listings" % server.server_address[1]
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
