"""Бизнес-логика демо-сервиса ООО «Квартирант плюс».

Модуль не зависит от HTTP: его вызывают и веб-слой (demo/app.py), и тесты.
Используется только стандартная библиотека Python 3.9+.

Что показывает код:
  * объекты: агент заносит эксклюзив через форму, модератор публикует или
    отклоняет его; опубликованный объект попадает в закрытый каталог;
  * закрытый каталог виден только клиентам с действующей подпиской;
  * регистрация по почте без перебора адресов: ответ одинаковый для нового и
    занятого адреса, ключ клиента уходит только письмом (очередь outbox);
  * подписка: заказ → уведомление платёжного агрегатора с HMAC-подписью
    (или платёж внутренней валютой мессенджера) → автоматическая активация;
    повтор уведомления не даёт второго начисления (ограничение уникальности
    в базе, а не проверка «в памяти»);
  * сверка зависших заказов по расписанию;
  * подбор новых объектов под сохранённые запросы подписчиков и очередь
    уведомлений без дублей;
  * запись на показ с подтверждением агентом и напоминанием;
  * автоматическое окончание подписки по сроку с предупреждением заранее;
  * журнал действий (кто, что, когда) без секретов и персональных данных;
  * сводные показатели для руководителя (воронка, время до уведомления).
"""

from __future__ import annotations

import contextlib
import hashlib
import hmac
import json
import re
import secrets
import sqlite3
import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, Iterator, List, Optional

HOUR = 60 * 60
DAY = 24 * HOUR

# Тарифы подписки: цена в копейках (агрегатор), цена во внутренней валюте
# мессенджера (в единицах этой валюты) и длительность.
PLANS: Dict[str, dict] = {
    "1m": {"amount": 99_000, "units": 990, "days": 30, "title": "Подписка на 1 месяц"},
    "3m": {"amount": 249_000, "units": 2_490, "days": 90, "title": "Подписка на 3 месяца"},
    "12m": {"amount": 799_000, "units": 7_990, "days": 365, "title": "Подписка на 12 месяцев"},
}

ORDER_PENDING = "pending"
ORDER_PAID = "paid"
ORDER_CANCELLED = "cancelled"

LISTING_PENDING = "pending"        # ждёт модерации
LISTING_PUBLISHED = "published"    # в закрытом каталоге
LISTING_REJECTED = "rejected"
LISTING_SOLD = "sold"

SUB_ACTIVE = "active"
SUB_EXPIRED = "expired"

VIEWING_REQUESTED = "requested"
VIEWING_CONFIRMED = "confirmed"
VIEWING_DONE = "done"

_EMAIL_RE = re.compile(r"^[^@\s]{1,64}@[^@\s]{1,190}\.[^@\s]{2,24}$")

SCHEMA = """
CREATE TABLE IF NOT EXISTS agents (
    id            TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    token_hash    TEXT NOT NULL UNIQUE,      -- сам токен не хранится
    created_at    REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS clients (
    id            TEXT PRIMARY KEY,
    email         TEXT NOT NULL UNIQUE,
    key_hash      TEXT NOT NULL UNIQUE,      -- ключ клиента, только хеш
    created_at    REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS listings (
    id            TEXT PRIMARY KEY,
    agent_id      TEXT NOT NULL REFERENCES agents(id),
    title         TEXT NOT NULL,
    district      TEXT NOT NULL,
    district_key  TEXT NOT NULL,             -- район в нормализованном виде
    price         INTEGER NOT NULL,          -- рубли
    rooms         INTEGER NOT NULL,          -- 0 = студия
    area          REAL NOT NULL,
    status        TEXT NOT NULL,
    reject_reason TEXT,
    created_at    REAL NOT NULL,
    moderated_at  REAL,
    published_at  REAL,
    early_access_until REAL,                 -- до этого момента — только подписчикам
    closed_at     REAL
);
CREATE TABLE IF NOT EXISTS orders (
    id            TEXT PRIMARY KEY,
    client_id     TEXT NOT NULL REFERENCES clients(id),
    plan          TEXT NOT NULL,
    amount        INTEGER NOT NULL,          -- копейки
    status        TEXT NOT NULL,
    created_at    REAL NOT NULL,
    closed_at     REAL
);
CREATE TABLE IF NOT EXISTS payments (
    provider      TEXT NOT NULL,             -- gateway | messenger
    external_id   TEXT NOT NULL,             -- id платежа у провайдера
    order_id      TEXT NOT NULL REFERENCES orders(id),
    amount        INTEGER NOT NULL,
    source        TEXT NOT NULL,             -- webhook | messenger | reconcile
    received_at   REAL NOT NULL,
    PRIMARY KEY (provider, external_id)      -- идемпотентность
);
CREATE TABLE IF NOT EXISTS subscriptions (
    client_id     TEXT PRIMARY KEY REFERENCES clients(id),
    expires_at    REAL NOT NULL,
    status        TEXT NOT NULL,
    updated_at    REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS saved_searches (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id     TEXT NOT NULL REFERENCES clients(id),
    district_key  TEXT,                      -- NULL = любой район
    price_max     INTEGER,
    rooms_min     INTEGER,
    rooms_max     INTEGER,
    created_at    REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS notifications (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id     TEXT NOT NULL REFERENCES clients(id),
    kind          TEXT NOT NULL,
    ref           TEXT NOT NULL,
    payload       TEXT NOT NULL,
    created_at    REAL NOT NULL,
    sent_at       REAL,
    attempts      INTEGER NOT NULL DEFAULT 0,
    UNIQUE (client_id, kind, ref)            -- одно уведомление на событие
);
CREATE TABLE IF NOT EXISTS listing_views (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    listing_id    TEXT NOT NULL REFERENCES listings(id),
    client_id     TEXT NOT NULL REFERENCES clients(id),
    ts            REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS viewings (
    id            TEXT PRIMARY KEY,
    listing_id    TEXT NOT NULL REFERENCES listings(id),
    client_id     TEXT NOT NULL REFERENCES clients(id),
    slot_at       REAL NOT NULL,
    status        TEXT NOT NULL,
    created_at    REAL NOT NULL,
    confirmed_at  REAL,
    reminder_sent_at REAL,
    done_at       REAL
);
CREATE UNIQUE INDEX IF NOT EXISTS viewings_slot_busy
    ON viewings (listing_id, slot_at) WHERE status IN ('requested', 'confirmed');
CREATE TABLE IF NOT EXISTS deals (
    listing_id    TEXT PRIMARY KEY REFERENCES listings(id),
    agent_id      TEXT NOT NULL REFERENCES agents(id),
    commission    INTEGER NOT NULL,          -- рубли
    closed_at     REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS outbox (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    email         TEXT NOT NULL,             -- «исходящие письма» демо вместо почтового сервиса
    kind          TEXT NOT NULL,             -- client_key | already_registered
    body          TEXT NOT NULL,
    created_at    REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS audit_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    ts            REAL NOT NULL,
    actor         TEXT NOT NULL,
    action        TEXT NOT NULL,
    details       TEXT NOT NULL
);
"""

_COUNTABLE = {
    "agents", "clients", "listings", "orders", "payments", "subscriptions",
    "saved_searches", "notifications", "listing_views", "viewings", "deals", "outbox", "audit_log",
}


class ServiceError(Exception):
    """Ошибка бизнес-логики с HTTP-кодом, который вернёт веб-слой."""

    def __init__(self, status: int, code: str, message: str = ""):
        super().__init__(message or code)
        self.status = status
        self.code = code


@dataclass
class PaymentResult:
    result: str          # processed | duplicate | extra_payment | ignored
    order_id: str
    subscription_expires_at: Optional[float] = None


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _new_id(prefix: str) -> str:
    return prefix + "_" + secrets.token_hex(8)


def norm_district(value: str) -> str:
    """«  Центральный   РАЙОН » → «центральный район»: так районы сравниваются."""
    return " ".join(str(value).split()).lower()


def sign_payload(secret: str, body: bytes) -> str:
    """Подпись так, как её считает агрегатор: HMAC-SHA256(тело) в hex."""
    return hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()


def _int_field(data: dict, name: str, lo: int, hi: int, required: bool = True) -> Optional[int]:
    value = data.get(name)
    if value is None or value == "":
        if required:
            raise ServiceError(400, "missing_" + name)
        return None
    if isinstance(value, bool):
        raise ServiceError(400, "invalid_" + name)
    try:
        number = int(value)
    except (TypeError, ValueError):
        raise ServiceError(400, "invalid_" + name)
    if not isinstance(value, str) and number != value:
        raise ServiceError(400, "invalid_" + name)   # 2,5 комнаты — ошибка, а не 2
    if not lo <= number <= hi:
        raise ServiceError(400, "invalid_" + name)
    return number


class AgencyService:
    """Сервис агентства поверх одной базы SQLite."""

    MAX_SEARCHES_PER_CLIENT = 5
    EARLY_ACCESS_DAYS = 3          # столько дней объект виден только подписчикам
    REMINDER_WINDOW = 24 * HOUR    # напоминание о показе — за сутки
    EXPIRING_WINDOW = 3 * DAY      # предупреждение об окончании подписки — за 3 дня

    def __init__(
        self,
        db_path: str,
        webhook_secret: str,
        messenger_secret: Optional[str] = None,
        clock: Callable[[], float] = time.time,
        pending_ttl_seconds: int = 30 * 60,
    ):
        if not webhook_secret:
            raise ValueError("webhook_secret обязателен")
        self._secret = webhook_secret
        self._messenger_secret = messenger_secret or None
        self._clock = clock
        self.pending_ttl_seconds = pending_ttl_seconds
        self._lock = threading.RLock()
        self._db = sqlite3.connect(db_path, check_same_thread=False, isolation_level=None)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA foreign_keys = ON")
        self._db.executescript(SCHEMA)

    # ------------------------------------------------------------------ util
    def close(self) -> None:
        self._db.close()

    @contextlib.contextmanager
    def _tx(self) -> Iterator[None]:
        """Одна транзакция под общим замком: либо всё, либо ничего."""
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                yield
            except BaseException:
                if self._db.in_transaction:
                    self._db.execute("ROLLBACK")
                raise
            else:
                self._db.execute("COMMIT")

    def _one(self, sql: str, params: tuple = ()) -> Optional[sqlite3.Row]:
        with self._lock:
            return self._db.execute(sql, params).fetchone()

    def _all(self, sql: str, params: tuple = ()) -> List[sqlite3.Row]:
        with self._lock:
            return self._db.execute(sql, params).fetchall()

    def _audit(self, actor: str, action: str, **details) -> None:
        # В журнал пишутся только идентификаторы и факты: ни ключей, ни почты.
        with self._lock:
            self._db.execute(
                "INSERT INTO audit_log (ts, actor, action, details) VALUES (?, ?, ?, ?)",
                (self._clock(), actor, action, json.dumps(details, ensure_ascii=False, sort_keys=True)),
            )

    def _enqueue(self, client_id: str, kind: str, ref: str, payload: dict) -> bool:
        """Поставить уведомление в очередь; повтор того же события — без дубля."""
        cur = self._db.execute(
            "INSERT OR IGNORE INTO notifications (client_id, kind, ref, payload, created_at)"
            " VALUES (?, ?, ?, ?, ?)",
            (client_id, kind, ref, json.dumps(payload, ensure_ascii=False, sort_keys=True), self._clock()),
        )
        return cur.rowcount == 1

    def health(self) -> dict:
        with self._lock:
            def q(sql: str, *params) -> int:
                return self._db.execute(sql, params).fetchone()[0]

            q("SELECT 1")
            return {
                "status": "ok",
                "database": "ok",
                "pending_orders": q("SELECT COUNT(*) FROM orders WHERE status = ?", ORDER_PENDING),
                "pending_moderation": q("SELECT COUNT(*) FROM listings WHERE status = ?", LISTING_PENDING),
                "queued_notifications": q("SELECT COUNT(*) FROM notifications WHERE sent_at IS NULL"),
            }

    # ------------------------------------------------------- identities
    def create_agent(self, name: str, actor: str = "owner") -> dict:
        name = " ".join(str(name or "").split())
        if not 1 <= len(name) <= 100:
            raise ServiceError(400, "invalid_name")
        agent_id = _new_id("ag")
        token = secrets.token_urlsafe(24)
        with self._tx():
            self._db.execute(
                "INSERT INTO agents (id, name, token_hash, created_at) VALUES (?, ?, ?, ?)",
                (agent_id, name, _sha256(token), self._clock()),
            )
            self._audit(actor, "agent_created", agent_id=agent_id)
        # Токен показывается один раз; в базе остаётся только его хеш.
        return {"agent_id": agent_id, "agent_token": token}

    def _agent(self, token: str) -> sqlite3.Row:
        row = self._one("SELECT * FROM agents WHERE token_hash = ?", (_sha256(token),)) if token else None
        if row is None:
            raise ServiceError(401, "unauthorized")
        return row

    def _client(self, key: str) -> sqlite3.Row:
        row = self._one("SELECT * FROM clients WHERE key_hash = ?", (_sha256(key),)) if key else None
        if row is None:
            raise ServiceError(401, "unauthorized")
        return row

    def _is_active(self, client_id: str) -> bool:
        # Срок проверяется напрямую, даже если плановое задание ещё не перевело
        # подписку в «истекла»: доступ не должен зависеть от расписания.
        row = self._one(
            "SELECT 1 FROM subscriptions WHERE client_id = ? AND status = ? AND expires_at > ?",
            (client_id, SUB_ACTIVE, self._clock()),
        )
        return row is not None

    def _require_subscriber(self, client_key: str) -> sqlite3.Row:
        client = self._client(client_key)
        if not self._is_active(client["id"]):
            raise ServiceError(403, "subscription_required")
        return client

    # ---------------------------------------------------------- listings
    def submit_listing(self, agent_token: str, data: dict) -> dict:
        """Агент заносит эксклюзивный объект через форму; объект ждёт модерации."""
        agent = self._agent(agent_token)
        title = " ".join(str(data.get("title", "")).split())
        district = " ".join(str(data.get("district", "")).split())
        if not 5 <= len(title) <= 200:
            raise ServiceError(400, "invalid_title")
        if not 2 <= len(district) <= 64:
            raise ServiceError(400, "invalid_district")
        price = _int_field(data, "price", 100_000, 2_000_000_000)
        rooms = _int_field(data, "rooms", 0, 10)
        try:
            area = float(data.get("area"))
        except (TypeError, ValueError):
            raise ServiceError(400, "invalid_area")
        if not 10 <= area <= 1000:
            raise ServiceError(400, "invalid_area")
        listing_id = _new_id("obj")
        with self._tx():
            self._db.execute(
                "INSERT INTO listings (id, agent_id, title, district, district_key, price, rooms, area,"
                " status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (listing_id, agent["id"], title, district, norm_district(district), price, rooms, area,
                 LISTING_PENDING, self._clock()),
            )
            self._audit("agent:" + agent["id"], "listing_submitted", listing_id=listing_id)
        return {"listing_id": listing_id, "status": LISTING_PENDING}

    def moderation_queue(self) -> List[dict]:
        rows = self._all(
            "SELECT id, agent_id, title, district, price, rooms, area, created_at FROM listings"
            " WHERE status = ? ORDER BY created_at", (LISTING_PENDING,),
        )
        return [dict(r) for r in rows]

    def approve_listing(self, listing_id: str, moderator: str = "moderator") -> dict:
        """Публикация объекта и постановка уведомлений подписчикам с подходящим запросом."""
        now = self._clock()
        with self._tx():
            listing = self._db.execute("SELECT * FROM listings WHERE id = ?", (listing_id,)).fetchone()
            if listing is None:
                raise ServiceError(404, "listing_not_found")
            if listing["status"] != LISTING_PENDING:
                raise ServiceError(409, "invalid_state")
            self._db.execute(
                "UPDATE listings SET status = ?, moderated_at = ?, published_at = ?, early_access_until = ?"
                " WHERE id = ?",
                (LISTING_PUBLISHED, now, now, now + self.EARLY_ACCESS_DAYS * DAY, listing_id),
            )
            matched = self._db.execute(
                "SELECT DISTINCT s.client_id FROM saved_searches s"
                " JOIN subscriptions sub ON sub.client_id = s.client_id"
                " WHERE sub.status = ? AND sub.expires_at > ?"
                "   AND (s.district_key IS NULL OR s.district_key = ?)"
                "   AND (s.price_max IS NULL OR ? <= s.price_max)"
                "   AND (s.rooms_min IS NULL OR ? >= s.rooms_min)"
                "   AND (s.rooms_max IS NULL OR ? <= s.rooms_max)",
                (SUB_ACTIVE, now, listing["district_key"], listing["price"], listing["rooms"], listing["rooms"]),
            ).fetchall()
            payload = {
                "listing_id": listing_id, "title": listing["title"], "district": listing["district"],
                "price": listing["price"], "rooms": listing["rooms"],
            }
            queued = sum(self._enqueue(r["client_id"], "new_listing", listing_id, payload) for r in matched)
            self._audit(moderator, "listing_published", listing_id=listing_id, queued_notifications=queued)
        return {"listing_id": listing_id, "status": LISTING_PUBLISHED, "queued_notifications": queued}

    def reject_listing(self, listing_id: str, reason: str, moderator: str = "moderator") -> dict:
        reason = " ".join(str(reason or "").split())
        if not 3 <= len(reason) <= 300:
            raise ServiceError(400, "invalid_reason")
        with self._tx():
            listing = self._db.execute("SELECT status FROM listings WHERE id = ?", (listing_id,)).fetchone()
            if listing is None:
                raise ServiceError(404, "listing_not_found")
            if listing["status"] != LISTING_PENDING:
                raise ServiceError(409, "invalid_state")
            self._db.execute(
                "UPDATE listings SET status = ?, reject_reason = ?, moderated_at = ? WHERE id = ?",
                (LISTING_REJECTED, reason, self._clock(), listing_id),
            )
            self._audit(moderator, "listing_rejected", listing_id=listing_id)
        return {"listing_id": listing_id, "status": LISTING_REJECTED, "reason": reason}

    def listing_status(self, listing_id: str) -> Optional[str]:
        row = self._one("SELECT status FROM listings WHERE id = ?", (listing_id,))
        return row["status"] if row else None

    # ----------------------------------------------------------- catalog
    def catalog(self, client_key: str) -> List[dict]:
        """Закрытый каталог: только опубликованные объекты и только подписчикам."""
        self._require_subscriber(client_key)
        rows = self._all(
            "SELECT id, title, district, price, rooms, area, published_at, early_access_until"
            " FROM listings WHERE status = ? ORDER BY published_at DESC", (LISTING_PUBLISHED,),
        )
        return [dict(r) for r in rows]

    def listing_detail(self, client_key: str, listing_id: str) -> dict:
        client = self._require_subscriber(client_key)
        with self._lock:
            row = self._db.execute(
                "SELECT l.id, l.title, l.district, l.price, l.rooms, l.area, l.published_at,"
                " l.early_access_until, a.name AS agent_name FROM listings l JOIN agents a ON a.id = l.agent_id"
                " WHERE l.id = ? AND l.status = ?", (listing_id, LISTING_PUBLISHED),
            ).fetchone()
            if row is None:
                raise ServiceError(404, "listing_not_found")
            self._db.execute(
                "INSERT INTO listing_views (listing_id, client_id, ts) VALUES (?, ?, ?)",
                (listing_id, client["id"], self._clock()),
            )
        return dict(row)

    def save_search(self, client_key: str, data: dict) -> dict:
        """Сохранённый запрос подписчика: район, предельная цена, число комнат."""
        client = self._client(client_key)
        district = data.get("district")
        district_key = norm_district(district) if district not in (None, "") else None
        if district_key is not None and not 2 <= len(district_key) <= 64:
            raise ServiceError(400, "invalid_district")
        price_max = _int_field(data, "price_max", 100_000, 2_000_000_000, required=False)
        rooms_min = _int_field(data, "rooms_min", 0, 10, required=False)
        rooms_max = _int_field(data, "rooms_max", 0, 10, required=False)
        if rooms_min is not None and rooms_max is not None and rooms_min > rooms_max:
            raise ServiceError(400, "invalid_rooms_range")
        with self._tx():
            count = self._db.execute(
                "SELECT COUNT(*) FROM saved_searches WHERE client_id = ?", (client["id"],)
            ).fetchone()[0]
            if count >= self.MAX_SEARCHES_PER_CLIENT:
                raise ServiceError(409, "too_many_searches")
            cur = self._db.execute(
                "INSERT INTO saved_searches (client_id, district_key, price_max, rooms_min, rooms_max, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (client["id"], district_key, price_max, rooms_min, rooms_max, self._clock()),
            )
            self._audit("client:" + client["id"], "search_saved", search_id=cur.lastrowid)
        return {"search_id": cur.lastrowid, "district": district_key, "price_max": price_max,
                "rooms_min": rooms_min, "rooms_max": rooms_max}

    def notifications_for(self, client_key: str) -> List[dict]:
        client = self._client(client_key)
        rows = self._all(
            "SELECT kind, ref, payload, created_at, sent_at FROM notifications WHERE client_id = ? ORDER BY id",
            (client["id"],),
        )
        return [
            {"kind": r["kind"], "ref": r["ref"], "payload": json.loads(r["payload"]),
             "created_at": r["created_at"], "sent_at": r["sent_at"]}
            for r in rows
        ]

    # ---------------------------------------------------------- viewings
    def request_viewing(self, client_key: str, listing_id: str, slot_at) -> dict:
        """Запись на показ: подписчик выбирает время, агент подтверждает."""
        client = self._require_subscriber(client_key)
        try:
            slot = float(slot_at)
        except (TypeError, ValueError):
            raise ServiceError(400, "invalid_slot")
        now = self._clock()
        if not now + HOUR <= slot <= now + 30 * DAY:
            raise ServiceError(400, "invalid_slot")
        viewing_id = _new_id("vw")
        with self._tx():
            listing = self._db.execute(
                "SELECT status FROM listings WHERE id = ?", (listing_id,)
            ).fetchone()
            if listing is None or listing["status"] != LISTING_PUBLISHED:
                raise ServiceError(404, "listing_not_found")
            try:
                self._db.execute(
                    "INSERT INTO viewings (id, listing_id, client_id, slot_at, status, created_at)"
                    " VALUES (?, ?, ?, ?, ?, ?)",
                    (viewing_id, listing_id, client["id"], slot, VIEWING_REQUESTED, now),
                )
            except sqlite3.IntegrityError:
                raise ServiceError(409, "slot_taken")
            self._audit("client:" + client["id"], "viewing_requested", viewing_id=viewing_id, listing_id=listing_id)
        return {"viewing_id": viewing_id, "status": VIEWING_REQUESTED, "slot_at": slot}

    def _agent_viewing(self, agent: sqlite3.Row, viewing_id: str) -> sqlite3.Row:
        row = self._db.execute(
            "SELECT v.* FROM viewings v JOIN listings l ON l.id = v.listing_id WHERE v.id = ? AND l.agent_id = ?",
            (viewing_id, agent["id"]),
        ).fetchone()
        if row is None:
            # Чужой показ и несуществующий показ для агента неразличимы.
            raise ServiceError(404, "viewing_not_found")
        return row

    def agent_viewings(self, agent_token: str) -> List[dict]:
        agent = self._agent(agent_token)
        rows = self._all(
            "SELECT v.id, v.listing_id, v.slot_at, v.status FROM viewings v JOIN listings l ON l.id = v.listing_id"
            " WHERE l.agent_id = ? ORDER BY v.slot_at", (agent["id"],),
        )
        return [dict(r) for r in rows]

    def confirm_viewing(self, agent_token: str, viewing_id: str) -> dict:
        agent = self._agent(agent_token)
        with self._tx():
            viewing = self._agent_viewing(agent, viewing_id)
            if viewing["status"] != VIEWING_REQUESTED:
                raise ServiceError(409, "invalid_state")
            self._db.execute(
                "UPDATE viewings SET status = ?, confirmed_at = ? WHERE id = ?",
                (VIEWING_CONFIRMED, self._clock(), viewing_id),
            )
            self._enqueue(viewing["client_id"], "viewing_confirmed", viewing_id,
                          {"viewing_id": viewing_id, "listing_id": viewing["listing_id"],
                           "slot_at": viewing["slot_at"]})
            self._audit("agent:" + agent["id"], "viewing_confirmed", viewing_id=viewing_id)
        return {"viewing_id": viewing_id, "status": VIEWING_CONFIRMED}

    def complete_viewing(self, agent_token: str, viewing_id: str) -> dict:
        agent = self._agent(agent_token)
        with self._tx():
            viewing = self._agent_viewing(agent, viewing_id)
            if viewing["status"] != VIEWING_CONFIRMED:
                raise ServiceError(409, "invalid_state")
            self._db.execute(
                "UPDATE viewings SET status = ?, done_at = ? WHERE id = ?",
                (VIEWING_DONE, self._clock(), viewing_id),
            )
            self._audit("agent:" + agent["id"], "viewing_done", viewing_id=viewing_id)
        return {"viewing_id": viewing_id, "status": VIEWING_DONE}

    def close_deal(self, agent_token: str, listing_id: str, commission) -> dict:
        agent = self._agent(agent_token)
        commission = _int_field({"commission": commission}, "commission", 0, 100_000_000)
        with self._tx():
            listing = self._db.execute(
                "SELECT status FROM listings WHERE id = ? AND agent_id = ?", (listing_id, agent["id"])
            ).fetchone()
            if listing is None:
                raise ServiceError(404, "listing_not_found")
            if listing["status"] != LISTING_PUBLISHED:
                raise ServiceError(409, "invalid_state")
            now = self._clock()
            self._db.execute("UPDATE listings SET status = ?, closed_at = ? WHERE id = ?",
                             (LISTING_SOLD, now, listing_id))
            self._db.execute(
                "INSERT INTO deals (listing_id, agent_id, commission, closed_at) VALUES (?, ?, ?, ?)",
                (listing_id, agent["id"], commission, now),
            )
            self._audit("agent:" + agent["id"], "deal_closed", listing_id=listing_id, commission=commission)
        return {"listing_id": listing_id, "status": LISTING_SOLD}

    # ------------------------------------------------------------ orders
    REGISTRATION_ACCEPTED = {
        "status": "accepted",
        "message": "Если адрес новый, ключ доступа отправлен на почту.",
    }

    def register_client(self, email: str) -> dict:
        """Регистрация по почте. Ответ одинаковый для нового и занятого адреса.

        Ключ клиента не возвращается в ответе: он уходит письмом на указанный
        адрес (в демо — в таблицу outbox). Поэтому по ответу нельзя ни узнать,
        зарегистрирован ли адрес, ни получить ключ к чужой почте.
        """
        email = (email or "").strip().lower()
        if not _EMAIL_RE.match(email):
            raise ServiceError(400, "invalid_email")
        now = self._clock()
        with self._tx():
            if self._db.execute("SELECT 1 FROM clients WHERE email = ?", (email,)).fetchone():
                # Хозяину адреса — уведомление о попытке; второй ключ не выдаём.
                self._db.execute(
                    "INSERT INTO outbox (email, kind, body, created_at) VALUES (?, ?, ?, ?)",
                    (email, "already_registered",
                     "Адрес уже зарегистрирован. Если это были не вы, просто проигнорируйте письмо.", now),
                )
                self._audit("anonymous", "registration_repeat")
            else:
                client_id = _new_id("cl")
                key = secrets.token_urlsafe(24)
                self._db.execute(
                    "INSERT INTO clients (id, email, key_hash, created_at) VALUES (?, ?, ?, ?)",
                    (client_id, email, _sha256(key), now),
                )
                self._db.execute(
                    "INSERT INTO outbox (email, kind, body, created_at) VALUES (?, ?, ?, ?)",
                    (email, "client_key", key, now),
                )
                self._audit("client:" + client_id, "client_registered")
        return dict(self.REGISTRATION_ACCEPTED)

    def outbox_for(self, email: str) -> List[dict]:
        """Письма, «отправленные» на адрес (в настоящей системе — почтовый сервис)."""
        rows = self._all(
            "SELECT kind, body, created_at FROM outbox WHERE email = ? ORDER BY id",
            ((email or "").strip().lower(),),
        )
        return [dict(r) for r in rows]

    def create_order(self, plan: str, client_key: str) -> dict:
        """Заказ подписки (первый или продление) — только по ключу клиента."""
        client_id = self._client(client_key)["id"]
        if plan not in PLANS:
            raise ServiceError(400, "invalid_plan")
        order_id = _new_id("ord")
        amount = PLANS[plan]["amount"]
        with self._tx():
            self._db.execute(
                "INSERT INTO orders (id, client_id, plan, amount, status, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (order_id, client_id, plan, amount, ORDER_PENDING, self._clock()),
            )
            self._audit("client:" + client_id, "order_created", order_id=order_id, plan=plan, amount=amount)
        return {"order_id": order_id, "plan": plan, "amount": amount, "units": PLANS[plan]["units"]}

    def get_order(self, order_id: str, client_key: str) -> dict:
        client = self._client(client_key)
        row = self._one("SELECT * FROM orders WHERE id = ? AND client_id = ?", (order_id, client["id"]))
        if row is None:
            raise ServiceError(404, "order_not_found")
        result = {"order_id": row["id"], "status": row["status"], "plan": row["plan"]}
        sub = self._one("SELECT expires_at FROM subscriptions WHERE client_id = ?", (client["id"],))
        if sub is not None:
            result["subscription_expires_at"] = sub["expires_at"]
            result["subscription_active"] = self._is_active(client["id"])
        return result

    def order_status(self, order_id: str) -> Optional[str]:
        row = self._one("SELECT status FROM orders WHERE id = ?", (order_id,))
        return row["status"] if row else None

    def subscription(self, client_key: str) -> Optional[dict]:
        client = self._client(client_key)
        row = self._one("SELECT expires_at, status FROM subscriptions WHERE client_id = ?", (client["id"],))
        return dict(row) if row else None

    # ----------------------------------------------------------- payment
    def verify_signature(self, body: bytes, signature: str) -> bool:
        expected = sign_payload(self._secret, body)
        return hmac.compare_digest(expected, (signature or "").strip().lower())

    def handle_gateway_webhook(self, body: bytes, signature: str) -> PaymentResult:
        """Уведомление агрегатора: подпись → разбор → идемпотентное начисление."""
        if not self.verify_signature(body, signature):
            self._audit("payment-gateway", "webhook_rejected", reason="bad_signature")
            raise ServiceError(403, "bad_signature")
        try:
            payload = json.loads(body.decode("utf-8"))
            order_id = str(payload["order_id"])
            external_id = str(payload["external_id"])
            amount = int(payload["amount"])
            status = str(payload.get("status", "succeeded"))
        except (ValueError, KeyError, TypeError, UnicodeDecodeError, AttributeError):
            raise ServiceError(400, "bad_payload")
        if status != "succeeded":
            self._audit("payment-gateway", "webhook_ignored", order_id=order_id, status=status)
            return PaymentResult("ignored", order_id)
        return self._apply_payment("gateway", external_id, order_id, amount, source="webhook")

    def handle_messenger_payment(self, secret: str, body: bytes) -> PaymentResult:
        """Платёж внутренней валютой мессенджера: его подтверждает сама платформа."""
        if not self._messenger_secret or not hmac.compare_digest(
            (secret or "").encode("utf-8"), self._messenger_secret.encode("utf-8")
        ):
            self._audit("messenger", "payment_rejected", reason="bad_secret")
            raise ServiceError(403, "bad_secret")
        try:
            payload = json.loads(body.decode("utf-8"))
            order_id = str(payload["order_id"])
            charge_id = str(payload["charge_id"])
            units = int(payload["units"])
        except (ValueError, KeyError, TypeError, UnicodeDecodeError, AttributeError):
            raise ServiceError(400, "bad_payload")
        return self._apply_payment("messenger", charge_id, order_id, units, source="messenger")

    def _apply_payment(self, provider: str, external_id: str, order_id: str, amount: int,
                       source: str) -> PaymentResult:
        actor = "messenger" if provider == "messenger" else "payment-gateway"
        with self._lock:
            order = self._db.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
            if order is None:
                self._audit(actor, "payment_unknown_order", order_id=order_id)
                raise ServiceError(404, "order_not_found")
            expected = PLANS[order["plan"]]["units"] if provider == "messenger" else order["amount"]
            if amount != expected:
                self._audit(actor, "payment_amount_mismatch", order_id=order_id, expected=expected, got=amount)
                raise ServiceError(422, "amount_mismatch")
            with self._tx():
                try:
                    self._db.execute(
                        "INSERT INTO payments (provider, external_id, order_id, amount, source, received_at)"
                        " VALUES (?, ?, ?, ?, ?, ?)",
                        (provider, external_id, order_id, amount, source, self._clock()),
                    )
                except sqlite3.IntegrityError:
                    # Этот платёж уже учтён: повтор уведомления или гонка со сверкой.
                    self._audit(actor, "payment_duplicate", order_id=order_id, source=source)
                    return PaymentResult("duplicate", order_id)
                if order["status"] == ORDER_PAID:
                    # Второй платёж по оплаченному заказу с другим id: деньги учтены,
                    # срок повторно не продлеваем — это случай для возврата вручную.
                    self._audit("system", "payment_extra_for_paid_order", order_id=order_id)
                    return PaymentResult("extra_payment", order_id)
                if order["status"] == ORDER_CANCELLED:
                    # Деньги пришли после отмены сверкой: клиент заплатил — доступ выдаём.
                    self._audit("system", "late_payment", order_id=order_id)
                expires_at = self._activate(order)
                self._db.execute(
                    "UPDATE orders SET status = ?, closed_at = ? WHERE id = ?",
                    (ORDER_PAID, self._clock(), order_id),
                )
                self._audit("system", "order_paid", order_id=order_id, provider=provider, source=source)
        return PaymentResult("processed", order_id, expires_at)

    def _activate(self, order: sqlite3.Row) -> float:
        """Создать или продлить подписку: новый срок прибавляется к концу текущего."""
        now = self._clock()
        days = PLANS[order["plan"]]["days"]
        sub = self._db.execute(
            "SELECT expires_at FROM subscriptions WHERE client_id = ?", (order["client_id"],)
        ).fetchone()
        start = max(now, sub["expires_at"]) if sub is not None else now
        expires_at = start + days * DAY
        self._db.execute(
            "INSERT INTO subscriptions (client_id, expires_at, status, updated_at) VALUES (?, ?, ?, ?)"
            " ON CONFLICT(client_id) DO UPDATE SET expires_at = excluded.expires_at,"
            " status = excluded.status, updated_at = excluded.updated_at",
            (order["client_id"], expires_at, SUB_ACTIVE, now),
        )
        self._audit("system", "subscription_activated", order_id=order["id"], days=days)
        return expires_at

    # ------------------------------------------------------ scheduled jobs
    def reconcile_stale_orders(
        self, status_checker: Optional[Callable[[str], Optional[dict]]] = None
    ) -> dict:
        """Сверка «зависших» заказов: найденную оплату провести, остальное отменить.

        Для каждого заказа «ожидает оплаты» старше pending_ttl_seconds
        status_checker (запрос статуса у агрегатора) сообщает, был ли платёж.
        Найденный платёж проводится тем же идемпотентным путём, что и webhook.
        """
        cutoff = self._clock() - self.pending_ttl_seconds
        stale = [dict(r) for r in self._all(
            "SELECT id FROM orders WHERE status = ? AND created_at <= ? ORDER BY created_at",
            (ORDER_PENDING, cutoff),
        )]
        report = {"checked": len(stale), "paid": 0, "cancelled": 0, "duplicate": 0}
        for order in stale:
            remote = status_checker(order["id"]) if status_checker else None
            if remote and remote.get("status") == "succeeded":
                res = self._apply_payment(
                    "gateway", str(remote["external_id"]), order["id"], int(remote["amount"]), source="reconcile"
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
        self._audit("scheduler", "reconcile_run", **report)
        return report

    def run_subscription_lifecycle(self) -> dict:
        """Предупредить за 3 дня до окончания и закрыть истёкшие подписки."""
        now = self._clock()
        report = {"expiring_notified": 0, "expired": 0}
        with self._tx():
            expiring = self._db.execute(
                "SELECT client_id, expires_at FROM subscriptions WHERE status = ? AND expires_at > ?"
                " AND expires_at <= ?", (SUB_ACTIVE, now, now + self.EXPIRING_WINDOW),
            ).fetchall()
            for row in expiring:
                # ref — срок окончания: после продления придёт новое предупреждение.
                if self._enqueue(row["client_id"], "subscription_expiring", "%d" % row["expires_at"],
                                 {"expires_at": row["expires_at"]}):
                    report["expiring_notified"] += 1
            expired = self._db.execute(
                "SELECT client_id, expires_at FROM subscriptions WHERE status = ? AND expires_at <= ?",
                (SUB_ACTIVE, now),
            ).fetchall()
            for row in expired:
                self._db.execute(
                    "UPDATE subscriptions SET status = ?, updated_at = ? WHERE client_id = ?",
                    (SUB_EXPIRED, now, row["client_id"]),
                )
                self._enqueue(row["client_id"], "subscription_expired", "%d" % row["expires_at"],
                              {"expires_at": row["expires_at"]})
                self._audit("scheduler", "subscription_expired", client_id=row["client_id"])
                report["expired"] += 1
        return report

    def send_viewing_reminders(self) -> int:
        """Напоминание о подтверждённом показе за сутки до него (один раз)."""
        now = self._clock()
        sent = 0
        with self._tx():
            due = self._db.execute(
                "SELECT id, client_id, listing_id, slot_at FROM viewings WHERE status = ?"
                " AND reminder_sent_at IS NULL AND slot_at > ? AND slot_at <= ?",
                (VIEWING_CONFIRMED, now, now + self.REMINDER_WINDOW),
            ).fetchall()
            for row in due:
                self._enqueue(row["client_id"], "viewing_reminder", row["id"],
                              {"viewing_id": row["id"], "listing_id": row["listing_id"], "slot_at": row["slot_at"]})
                self._db.execute("UPDATE viewings SET reminder_sent_at = ? WHERE id = ?", (now, row["id"]))
                sent += 1
        return sent

    def dispatch_notifications(
        self, sender: Optional[Callable[[str, str, dict], None]] = None, limit: int = 100
    ) -> dict:
        """Отправить очередь. Неудачная отправка остаётся в очереди до следующего прохода."""
        rows = self._all(
            "SELECT n.id, n.kind, n.payload, c.email FROM notifications n JOIN clients c ON c.id = n.client_id"
            " WHERE n.sent_at IS NULL ORDER BY n.id LIMIT ?", (max(1, min(int(limit), 1000)),),
        )
        report = {"sent": 0, "failed": 0}
        for row in rows:
            try:
                if sender is not None:
                    sender(row["email"], row["kind"], json.loads(row["payload"]))
            except Exception:
                with self._lock:
                    self._db.execute("UPDATE notifications SET attempts = attempts + 1 WHERE id = ?", (row["id"],))
                report["failed"] += 1
                continue
            with self._lock:
                self._db.execute(
                    "UPDATE notifications SET sent_at = ?, attempts = attempts + 1 WHERE id = ?",
                    (self._clock(), row["id"]),
                )
            report["sent"] += 1
        if rows:
            self._audit("scheduler", "notifications_dispatched", **report)
        return report

    def run_jobs(self, status_checker=None, sender=None) -> dict:
        """Один проход планировщика: сверка, сроки подписок, напоминания, отправка."""
        return {
            "reconcile": self.reconcile_stale_orders(status_checker),
            "subscriptions": self.run_subscription_lifecycle(),
            "viewing_reminders": self.send_viewing_reminders(),
            "notifications": self.dispatch_notifications(sender),
        }

    # ------------------------------------------------------------- admin
    def metrics(self) -> dict:
        """Сводка для руководителя: воронка, подписчики, вклад агентов, время до уведомления."""
        now = self._clock()
        with self._lock:
            def q(sql: str, *params):
                return self._db.execute(sql, params).fetchone()[0]

            views = q("SELECT COUNT(*) FROM listing_views")
            requested = q("SELECT COUNT(*) FROM viewings")
            done = q("SELECT COUNT(*) FROM viewings WHERE status = ?", VIEWING_DONE)
            deals = q("SELECT COUNT(*) FROM deals")
            delay = q(
                "SELECT AVG(n.sent_at - l.published_at) FROM notifications n JOIN listings l ON l.id = n.ref"
                " WHERE n.kind = 'new_listing' AND n.sent_at IS NOT NULL"
            )
            by_agent = [dict(r) for r in self._db.execute(
                "SELECT a.id AS agent_id, a.name,"
                " (SELECT COUNT(*) FROM listings l WHERE l.agent_id = a.id AND l.status IN ('published', 'sold'))"
                "   AS listings_published,"
                " (SELECT COUNT(*) FROM deals d WHERE d.agent_id = a.id) AS deals,"
                " (SELECT COALESCE(SUM(d.commission), 0) FROM deals d WHERE d.agent_id = a.id) AS commission"
                " FROM agents a ORDER BY a.name"
            ).fetchall()]
            return {
                "active_subscribers": q(
                    "SELECT COUNT(*) FROM subscriptions WHERE status = ? AND expires_at > ?", SUB_ACTIVE, now),
                "listings": {
                    s: q("SELECT COUNT(*) FROM listings WHERE status = ?", s)
                    for s in (LISTING_PENDING, LISTING_PUBLISHED, LISTING_REJECTED, LISTING_SOLD)
                },
                "funnel": {
                    "views": views, "viewings_requested": requested, "viewings_done": done, "deals": deals,
                    "view_to_viewing": round(requested / views, 4) if views else None,
                    "viewing_to_deal": round(deals / done, 4) if done else None,
                },
                "avg_notify_delay_seconds": round(delay, 1) if delay is not None else None,
                "subscription_revenue_kopecks": q(
                    "SELECT COALESCE(SUM(amount), 0) FROM payments WHERE provider = 'gateway'"),
                "subscription_revenue_units": q(
                    "SELECT COALESCE(SUM(amount), 0) FROM payments WHERE provider = 'messenger'"),
                "by_agent": by_agent,
            }

    def audit_entries(self, limit: int = 100) -> List[dict]:
        limit = max(1, min(int(limit), 1000))
        rows = self._all(
            "SELECT id, ts, actor, action, details FROM audit_log ORDER BY id DESC LIMIT ?", (limit,)
        )
        return [
            {"id": r["id"], "ts": r["ts"], "actor": r["actor"], "action": r["action"],
             "details": json.loads(r["details"])}
            for r in rows
        ]

    def count(self, table: str) -> int:
        if table not in _COUNTABLE:
            raise ValueError(table)
        return self._one(f"SELECT COUNT(*) FROM {table}")[0]
