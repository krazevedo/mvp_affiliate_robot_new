#!/usr/bin/env python3
# conversions_sync.py — baixa conversionReport e salva no SQLite + EV materializado
from __future__ import annotations
import argparse, os, time, sqlite3, re
from typing import Any, Dict
from shopee_monorepo_modules.conversions import make_session, iter_conversion_report

def parse_money(s):
    if s is None: return 0.0
    t = str(s)
    t = re.sub(r"[^0-9,\.]+", "", t)
    if "," in t and "." not in t:
        t = t.replace(",", ".")
    parts = t.split(".")
    if len(parts) > 2:
        t = parts[0] + "." + "".join(parts[1:])
    try:
        return float(t)
    except:
        return 0.0

def ensure_schema(con: sqlite3.Connection):
    con.executescript("""
    CREATE TABLE IF NOT EXISTS conversions (
      conversion_id       INTEGER PRIMARY KEY,
      purchase_time       INTEGER,
      click_time          INTEGER,
      buyer_type          TEXT,
      device              TEXT,
      utm_content         TEXT,
      referrer            TEXT,
      net_commission      REAL,
      total_commission    REAL,
      campaign_type       TEXT
    );
    CREATE TABLE IF NOT EXISTS conversion_orders (
      conversion_id   INTEGER,
      order_id        TEXT,
      order_status    TEXT,
      shop_type       TEXT,
      PRIMARY KEY (conversion_id, order_id)
    );
    CREATE TABLE IF NOT EXISTS conversion_items (
      conversion_id               INTEGER,
      order_id                    TEXT,
      item_id                     INTEGER,
      model_id                    INTEGER,
      item_name                   TEXT,
      qty                         INTEGER,
      actual_amount               REAL,
      item_total_commission       REAL,
      item_seller_commission      REAL,
      item_shopee_commission_capped REAL,
      item_seller_commission_rate REAL,
      item_shopee_commission_rate REAL,
      display_item_status         TEXT,
      fraud_status                TEXT,
      channel_type                TEXT,
      attribution_type            TEXT,
      shop_id                     INTEGER,
      shop_name                   TEXT,
      image_url                   TEXT,
      complete_time               INTEGER,
      globalCategoryLv1Name       TEXT,
      globalCategoryLv2Name       TEXT,
      globalCategoryLv3Name       TEXT,
      PRIMARY KEY (conversion_id, order_id, item_id, model_id)
    );
    CREATE INDEX IF NOT EXISTS idx_conv_utm ON conversions(utm_content);
    CREATE INDEX IF NOT EXISTS idx_conv_item ON conversion_items(item_id);
    CREATE INDEX IF NOT EXISTS idx_conv_shop ON conversion_items(shop_name);
    CREATE INDEX IF NOT EXISTS idx_conv_cat ON conversion_items(globalCategoryLv1Name);
    """)
    con.commit()

def upsert_conversion(con: sqlite3.Connection, node: Dict[str, Any]):
    con.execute("""
        INSERT INTO conversions(conversion_id, purchase_time, click_time, buyer_type, device,
                                utm_content, referrer, net_commission, total_commission, campaign_type)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(conversion_id) DO UPDATE SET
          purchase_time=excluded.purchase_time,
          click_time=excluded.click_time,
          buyer_type=excluded.buyer_type,
          device=excluded.device,
          utm_content=excluded.utm_content,
          referrer=excluded.referrer,
          net_commission=excluded.net_commission,
          total_commission=excluded.total_commission,
          campaign_type=excluded.campaign_type
    """, (
        int(node.get("conversionId")),
        int(node.get("purchaseTime") or 0) or None,
        int(node.get("clickTime") or 0) or None,
        node.get("buyerType"),
        node.get("device"),
        node.get("utmContent"),
        node.get("referrer"),
        parse_money(node.get("netCommission")),
        parse_money(node.get("totalCommission")),
        node.get("campaignType"),
    ))

def upsert_orders_items(con: sqlite3.Connection, node: Dict[str, Any]):
    cid = int(node.get("conversionId"))
    orders = node.get("orders") or []
    for od in orders:
        oid = str(od.get("orderId"))
        con.execute("""
            INSERT INTO conversion_orders(conversion_id, order_id, order_status, shop_type)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(conversion_id, order_id) DO UPDATE SET
              order_status=excluded.order_status,
              shop_type=excluded.shop_type
        """, (cid, oid, od.get("orderStatus"), od.get("shopType")))
        items = od.get("items") or []
        for it in items:
            con.execute("""
                INSERT INTO conversion_items(
                  conversion_id, order_id, item_id, model_id, item_name, qty, actual_amount,
                  item_total_commission, item_seller_commission, item_shopee_commission_capped,
                  item_seller_commission_rate, item_shopee_commission_rate,
                  display_item_status, fraud_status, channel_type, attribution_type,
                  shop_id, shop_name, image_url, complete_time,
                  globalCategoryLv1Name, globalCategoryLv2Name, globalCategoryLv3Name
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(conversion_id, order_id, item_id, model_id) DO UPDATE SET
                  item_name=excluded.item_name,
                  qty=excluded.qty,
                  actual_amount=excluded.actual_amount,
                  item_total_commission=excluded.item_total_commission,
                  item_seller_commission=excluded.item_seller_commission,
                  item_shopee_commission_capped=excluded.item_shopee_commission_capped,
                  item_seller_commission_rate=excluded.item_seller_commission_rate,
                  item_shopee_commission_rate=excluded.item_shopee_commission_rate,
                  display_item_status=excluded.display_item_status,
                  fraud_status=excluded.fraud_status,
                  channel_type=excluded.channel_type,
                  attribution_type=excluded.attribution_type,
                  shop_id=excluded.shop_id,
                  shop_name=excluded.shop_name,
                  image_url=excluded.image_url,
                  complete_time=excluded.complete_time,
                  globalCategoryLv1Name=excluded.globalCategoryLv1Name,
                  globalCategoryLv2Name=excluded.globalCategoryLv2Name,
                  globalCategoryLv3Name=excluded.globalCategoryLv3Name
            """, (
                cid, oid,
                int(it.get("itemId") or 0), int(it.get("modelId") or 0),
                it.get("itemName"),
                int(it.get("qty") or 0),
                parse_money(it.get("actualAmount")),
                parse_money(it.get("itemTotalCommission")),
                parse_money(it.get("itemSellerCommission")),
                parse_money(it.get("itemShopeeCommissionCapped")),
                parse_money(it.get("itemSellerCommissionRate")),
                parse_money(it.get("itemShopeeCommissionRate")),
                it.get("displayItemStatus"),
                it.get("fraudStatus"),
                it.get("channelType"),
                it.get("attributionType"),
                int(it.get("shopId") or 0),
                it.get("shopName"),
                it.get("imageUrl"),
                int(it.get("completeTime") or 0) or None,
                it.get("globalCategoryLv1Name"),
                it.get("globalCategoryLv2Name"),
                it.get("globalCategoryLv3Name"),
            ))

def rebuild_ev_tables(con: sqlite3.Connection, window_days: int = 28):
    cutoff = int(time.time()) - window_days * 86400
    cur = con.cursor()
    cur.executescript("""
        DROP TABLE IF EXISTS ev_item_agg;
        DROP TABLE IF EXISTS ev_shop_agg;
        DROP TABLE IF EXISTS ev_cat_agg;
    """)
    cur.execute("""
        CREATE TABLE ev_item_agg AS
        SELECT ci.item_id AS key, COALESCE(SUM(ci.item_total_commission),0.0) AS ev_sum
        FROM conversion_items ci
        JOIN conversions c ON c.conversion_id = ci.conversion_id
        WHERE (c.purchase_time IS NULL OR c.purchase_time >= ?)
        GROUP BY ci.item_id
    """, (cutoff,))
    cur.execute("""
        CREATE TABLE ev_shop_agg AS
        SELECT ci.shop_name AS key, COALESCE(SUM(ci.item_total_commission),0.0) AS ev_sum
        FROM conversion_items ci
        JOIN conversions c ON c.conversion_id = ci.conversion_id
        WHERE (c.purchase_time IS NULL OR c.purchase_time >= ?)
        GROUP BY ci.shop_name
    """, (cutoff,))
    cur.execute("""
        CREATE TABLE ev_cat_agg AS
        SELECT ci.globalCategoryLv1Name AS key, COALESCE(SUM(ci.item_total_commission),0.0) AS ev_sum
        FROM conversion_items ci
        JOIN conversions c ON c.conversion_id = ci.conversion_id
        WHERE (c.purchase_time IS NULL OR c.purchase_time >= ?)
        GROUP BY ci.globalCategoryLv1Name
    """, (cutoff,))
    con.commit()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.getenv("DB_PATH", "data/bot.db"))
    ap.add_argument("--purchase-days", type=int, default=1, help="Janela em dias para purchaseTime (últimas 24h)")
    ap.add_argument("--complete-days", type=int, default=7, help="Janela em dias para completeTime (últimos 7 dias)")
    ap.add_argument("--partner-id", type=int, default=int(os.getenv("SHOPEE_PARTNER_ID","0")))
    ap.add_argument("--api-key", default=os.getenv("SHOPEE_API_KEY",""))
    args = ap.parse_args()

    if not args.partner_id or not args.api_key:
        raise SystemExit("Defina SHOPEE_PARTNER_ID e SHOPEE_API_KEY (ou use --partner-id/--api-key).")

    os.makedirs(os.path.dirname(args.db), exist_ok=True)
    con = sqlite3.connect(args.db)
    ensure_schema(con)

    session = make_session()
    now = int(time.time())

    # 1) Últimas 24h por purchaseTime
    p_start = now - args.purchase_days * 86400
    for node in iter_conversion_report(session, args.partner_id, args.api_key,
                                       purchase_start=p_start, purchase_end=now, limit=500):
        upsert_conversion(con, node)
        upsert_orders_items(con, node)

    # 2) Últimos 7 dias por completeTime
    c_start = now - args.complete_days * 86400
    for node in iter_conversion_report(session, args.partner_id, args.api_key,
                                       complete_start=c_start, complete_end=now, limit=500):
        upsert_conversion(con, node)
        upsert_orders_items(con, node)

    rebuild_ev_tables(con, window_days=28)
    con.commit()
    con.close()
    print("OK: conversions sincronizadas e EV materializado.")

if __name__ == "__main__":
    main()
