"""Бизнес-логика демо-сервиса подписок ООО «Сервис».

Модуль намеренно не зависит от HTTP: его функции вызываются и из веб-слоя
(demo/app.py), и напрямую из тестов. Используется только стандартная
библиотека Python 3.9+.

Ключевые свойства, которые демонстрирует код:
  * проверка HMAC-подписи уведомления платёжного агрегатора;
  * идемпотентность: повтор уведомления с тем же внешним id не даёт
    второго начисления (UNIQUE-ограничение в базе, а не проверка «в памяти»);
  * автоматическая активация (или продление) подписки и выдача токена
    доступа, который хранится только в виде хеша;
  * сверка «зависших» заказов по расписанию;
  * журнал действий (кто, что, когда).
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
import secrets
import sqlite3
import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

DAY = 24 * 60 * 60

# Тарифы: код -> (цена в копейках, длительность в днях, название).
PLANS: Dict[str, tuple] = {
    "1m": (29_900, 30, "Подписка на 1 месяц"),
    "3m": (79_900, 90, "Подписка на 3 месяца"),
    "12m": (249_900, 365, "Подписка на 12 месяцев"),
}

ORDER_PENDING = "pending"
ORDER_PAID = "paid"
ORDER_CANCELLED = "cancelled"

_EMAIL_RE = re.compile(r"^[^@\s]{1,64}@[^@\s]{1,190}\.[^@\s]{2,24}$")

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    id            TEXT PRIMARY KEY,
    email         TEXT NOT NULL,
    plan          TEXT NOT NULL,
    amount        INTEGER NOT NULL,
    status        TEXT NOT NULL,
    order_key_hash TEXT NOT NULL,
    created_at    REAL NOT NULL,
    closed_at     REAL
);
CREATE TABLE IF NOT EXISTS payments (
    external_id   TEXT PRIMARY KEY,          -- идемпотентность по внешнему id
    order_id      TEXT NOT NULL REFERENCES orders(id),
    amount        INTEGER NOT NULL,
    source        TEXT NOT NULL,             -- webhook | reconcile
    received_at   REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS subscriptions (
    email         TEXT PRIMARY KEY,
    expires_at    REAL NOT NULL,
    token_hash    TEXT NOT NULL,
    updated_at    REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS deliveries (
    order_id      TEXT PRIMARY KEY REFERENCES orders(id),
    token         TEXT,                      -- выдаётся клиенту один раз
    created_at    REAL NOT NULL,
    delivered_at  REAL
);
CREATE TABLE IF NOT EXISTS audit_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    ts            REAL NOT NULL,
    actor         TEXT NOT NULL,
    action        TEXT NOT NULL,
    details       TEXT NOT NULL
);
"""


class ServiceError(Exception):
    """Ошибка бизнес-логики с HTTP-кодом, который вернёт веб-слой."""

    def __init__(self, status: int, code: str, message: str = ""):
        super().__init__(message or code)
        self.status = status
        self.code = code


@dataclass
class WebhookResult:
    result: str          # processed | duplicate
    order_id: str
    subscription_expires_at: Optional[float] = None


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sign_payload(secret: str, body: bytes) -> str:
    """Подпись так, как её считает агрегатор: HMAC-SHA256(body) в hex."""
    return hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()


class SubscriptionService:
    """Сервис подписок поверх одной базы SQLite."""

    def __init__(
        self,
        db_path: str,
        webhook_secret: str,
        clock: Callable[[], float] = time.time,
        pending_ttl_seconds: int = 30 * 60,
    ):
        if not webhook_secret:
            raise ValueError("webhook_secret обязателен")
        self._secret = webhook_secret
        self._clock = clock
        self.pending_ttl_seconds = pending_ttl_seconds
        self._lock = threading.Lock()
        self._db = sqlite3.connect(db_path, check_same_thread=False, isolation_level=None)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA foreign_keys = ON")
        self._db.executescript(SCHEMA)

    # ------------------------------------------------------------------ util
    def close(self) -> None:
        self._db.close()

    def _audit(self, actor: str, action: str, **details) -> None:
        self._db.execute(
            "INSERT INTO audit_log (ts, actor, action, details) VALUES (?, ?, ?, ?)",
            (self._clock(), actor, action, json.dumps(details, ensure_ascii=False, sort_keys=True)),
        )

    def health(self) -> dict:
        with self._lock:
            self._db.execute("SELECT 1").fetchone()
            pending = self._db.execute(
                "SELECT COUNT(*) FROM orders WHERE status = ?", (ORDER_PENDING,)
            ).fetchone()[0]
        return {"status": "ok", "database": "ok", "pending_orders": pending}

    # ---------------------------------------------------------------- orders
    def create_order(self, email: str, plan: str) -> dict:
        email = (email or "").strip().lower()
        if not _EMAIL_RE.match(email):
            raise ServiceError(400, "invalid_email")
        if plan not in PLANS:
            raise ServiceError(400, "invalid_plan")
        amount = PLANS[plan][0]
        order_id = "ord_" + secrets.token_hex(8)
        order_key = secrets.token_urlsafe(24)
        with self._lock:
            self._db.execute("BEGIN")
            try:
                self._db.execute(
                    "INSERT INTO orders (id, email, plan, amount, status, order_key_hash, created_at)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (order_id, email, plan, amount, ORDER_PENDING, _sha256(order_key), self._clock()),
                )
                self._audit("customer", "order_created", order_id=order_id, plan=plan, amount=amount)
                self._db.execute("COMMIT")
            except Exception:
                self._db.execute("ROLLBACK")
                raise
        # order_key возвращается один раз: по нему клиент забирает свой заказ.
        return {"order_id": order_id, "amount": amount, "plan": plan, "order_key": order_key}

    def get_order_for_customer(self, order_id: str, order_key: str) -> dict:
        """Статус заказа для клиента; токен доступа отдаётся ровно один раз."""
        with self._lock:
            row = self._db.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
            if row is None or not hmac.compare_digest(row["order_key_hash"], _sha256(order_key or "")):
                # Одинаковый ответ на «нет заказа» и «чужой ключ»: не подсказываем, что id существует.
                raise ServiceError(404, "order_not_found")
            result = {"order_id": row["id"], "status": row["status"], "plan": row["plan"]}
            delivery = self._db.execute(
                "SELECT token FROM deliveries WHERE order_id = ? AND token IS NOT NULL", (order_id,)
            ).fetchone()
            if delivery is not None:
                result["access_token"] = delivery["token"]
                self._db.execute(
                    "UPDATE deliveries SET token = NULL, delivered_at = ? WHERE order_id = ?",
                    (self._clock(), order_id),
                )
                self._audit("customer", "token_delivered", order_id=order_id)
            sub = self._db.execute(
                "SELECT expires_at FROM subscriptions WHERE email = ?", (row["email"],)
            ).fetchone()
            if sub is not None:
                result["subscription_expires_at"] = sub["expires_at"]
        return result

    # --------------------------------------------------------------- payment
    def verify_signature(self, body: bytes, signature: str) -> bool:
        expected = sign_payload(self._secret, body)
        return hmac.compare_digest(expected, (signature or "").strip().lower())

    def handle_webhook(self, body: bytes, signature: str) -> WebhookResult:
        """Уведомление агрегатора: подпись → разбор → идемпотентное начисление."""
        if not self.verify_signature(body, signature):
            with self._lock:
                self._audit("payment-gateway", "webhook_rejected", reason="bad_signature")
            raise ServiceError(403, "bad_signature")
        try:
            payload = json.loads(body.decode("utf-8"))
            order_id = str(payload["order_id"])
            external_id = str(payload["external_id"])
            amount = int(payload["amount"])
            status = str(payload.get("status", "succeeded"))
        except (ValueError, KeyError, TypeError, UnicodeDecodeError):
            raise ServiceError(400, "bad_payload")
        if status != "succeeded":
            with self._lock:
                self._audit("payment-gateway", "webhook_ignored", order_id=order_id, status=status)
            return WebhookResult("ignored", order_id)
        return self._apply_payment(order_id, external_id, amount, source="webhook")

    def _apply_payment(self, order_id: str, external_id: str, amount: int, source: str) -> WebhookResult:
        with self._lock:
            order = self._db.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
            if order is None:
                self._audit("payment-gateway", "payment_unknown_order", order_id=order_id)
                raise ServiceError(404, "order_not_found")
            if amount != order["amount"]:
                self._audit(
                    "payment-gateway", "payment_amount_mismatch",
                    order_id=order_id, expected=order["amount"], got=amount,
                )
                raise ServiceError(422, "amount_mismatch")

            self._db.execute("BEGIN IMMEDIATE")
            try:
                try:
                    self._db.execute(
                        "INSERT INTO payments (external_id, order_id, amount, source, received_at)"
                        " VALUES (?, ?, ?, ?, ?)",
                        (external_id, order_id, amount, source, self._clock()),
                    )
                except sqlite3.IntegrityError:
                    # Этот платёж уже учтён: повтор уведомления или гонка со сверкой.
                    self._db.execute("ROLLBACK")
                    self._audit("payment-gateway", "payment_duplicate", order_id=order_id, source=source)
                    return WebhookResult("duplicate", order_id)

                if order["status"] == ORDER_PAID:
                    # Второй платёж по уже оплаченному заказу с другим внешним id —
                    # деньги учитываем, но подписку повторно не продлеваем: это разбор вручную.
                    self._audit("system", "payment_extra_for_paid_order", order_id=order_id)
                    self._db.execute("COMMIT")
                    return WebhookResult("duplicate", order_id)

                if order["status"] == ORDER_CANCELLED:
                    # Деньги пришли после отмены сверкой: клиент заплатил — услугу выдаём.
                    self._audit("system", "late_payment", order_id=order_id)

                expires_at = self._activate(order)
                self._db.execute(
                    "UPDATE orders SET status = ?, closed_at = ? WHERE id = ?",
                    (ORDER_PAID, self._clock(), order_id),
                )
                self._audit("system", "order_paid", order_id=order_id, source=source)
                self._db.execute("COMMIT")
            except Exception:
                if self._db.in_transaction:
                    self._db.execute("ROLLBACK")
                raise
        return WebhookResult("processed", order_id, expires_at)

    def _activate(self, order: sqlite3.Row) -> float:
        """Создать или продлить подписку и положить новый токен в «выдачу»."""
        now = self._clock()
        days = PLANS[order["plan"]][1]
        token = secrets.token_urlsafe(32)
        sub = self._db.execute(
            "SELECT expires_at FROM subscriptions WHERE email = ?", (order["email"],)
        ).fetchone()
        start = max(now, sub["expires_at"]) if sub is not None else now
        expires_at = start + days * DAY
        self._db.execute(
            "INSERT INTO subscriptions (email, expires_at, token_hash, updated_at) VALUES (?, ?, ?, ?)"
            " ON CONFLICT(email) DO UPDATE SET expires_at = excluded.expires_at,"
            " token_hash = excluded.token_hash, updated_at = excluded.updated_at",
            (order["email"], expires_at, _sha256(token), now),
        )
        self._db.execute(
            "INSERT OR REPLACE INTO deliveries (order_id, token, created_at) VALUES (?, ?, ?)",
            (order["id"], token, now),
        )
        self._audit("system", "subscription_activated", order_id=order["id"], days=days)
        return expires_at

    def check_access(self, email: str, token: str) -> bool:
        """Проверка токена доступа (то, что делал бы облачный сервис)."""
        with self._lock:
            row = self._db.execute(
                "SELECT expires_at, token_hash FROM subscriptions WHERE email = ?",
                ((email or "").strip().lower(),),
            ).fetchone()
        if row is None:
            return False
        return hmac.compare_digest(row["token_hash"], _sha256(token or "")) and row["expires_at"] > self._clock()

    # ------------------------------------------------------------- reconcile
    def reconcile_stale_orders(
        self,
        status_checker: Optional[Callable[[str], Optional[dict]]] = None,
    ) -> dict:
        """Сверка «зависших» заказов (вызывается планировщиком или тестом).

        Для каждого заказа в статусе pending старше pending_ttl_seconds:
          * если status_checker (запрос статуса у агрегатора) сообщает об оплате —
            платёж проводится тем же идемпотентным путём, что и webhook;
          * иначе заказ отменяется.
        """
        cutoff = self._clock() - self.pending_ttl_seconds
        with self._lock:
            stale = [
                dict(r)
                for r in self._db.execute(
                    "SELECT id, amount FROM orders WHERE status = ? AND created_at <= ? ORDER BY created_at",
                    (ORDER_PENDING, cutoff),
                ).fetchall()
            ]
        report = {"checked": len(stale), "paid": 0, "cancelled": 0, "duplicate": 0}
        for order in stale:
            remote = status_checker(order["id"]) if status_checker else None
            if remote and remote.get("status") == "succeeded":
                res = self._apply_payment(
                    order["id"], str(remote["external_id"]), int(remote["amount"]), source="reconcile"
                )
                report["paid" if res.result == "processed" else "duplicate"] += 1
                continue
            with self._lock:
                cur = self._db.execute(
                    "UPDATE orders SET status = ?, closed_at = ? WHERE id = ? AND status = ?",
                    (ORDER_CANCELLED, self._clock(), order["id"], ORDER_PENDING),
                )
                if cur.rowcount:
                    report["cancelled"] += 1
                    self._audit("scheduler", "order_cancelled_stale", order_id=order["id"])
        with self._lock:
            self._audit("scheduler", "reconcile_run", **report)
        return report

    # ----------------------------------------------------------------- admin
    def audit_entries(self, limit: int = 100) -> List[dict]:
        limit = max(1, min(int(limit), 1000))
        with self._lock:
            rows = self._db.execute(
                "SELECT id, ts, actor, action, details FROM audit_log ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        return [
            {"id": r["id"], "ts": r["ts"], "actor": r["actor"], "action": r["action"],
             "details": json.loads(r["details"])}
            for r in rows
        ]

    def count(self, table: str) -> int:
        if table not in {"orders", "payments", "subscriptions", "deliveries", "audit_log"}:
            raise ValueError(table)
        with self._lock:
            return self._db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    def order_status(self, order_id: str) -> Optional[str]:
        with self._lock:
            row = self._db.execute("SELECT status FROM orders WHERE id = ?", (order_id,)).fetchone()
        return row["status"] if row else None

    def subscription_expires_at(self, email: str) -> Optional[float]:
        with self._lock:
            row = self._db.execute(
                "SELECT expires_at FROM subscriptions WHERE email = ?", (email.strip().lower(),)
            ).fetchone()
        return row["expires_at"] if row else None
