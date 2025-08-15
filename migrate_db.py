#!/usr/bin/env python3
"""migrate_db.py — aplica migrações no SQLite usando só Python (sem CLI sqlite3).

- Se o arquivo SQL existir, executa-o.
- Se NÃO existir, aplica um fallback embutido que cria todas as tabelas/índices necessárias.
"""
from __future__ import annotations
import argparse, os, sqlite3, sys

FALLBACK_SQL = """
PRAGMA foreign_keys=OFF;

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
"""

def apply_sql(db_path: str, sql: str) -> None:
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    con = sqlite3.connect(db_path)
    try:
        con.executescript(sql)
        con.commit()
    finally:
        con.close()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=os.getenv("DB_PATH", "data/bot.db"))
    ap.add_argument("--sql", default="sql_migrations/001_add_conversions.sql")
    args = ap.parse_args()

    if os.path.exists(args.sql):
        with open(args.sql, "r", encoding="utf-8") as f:
            sql = f.read()
        apply_sql(args.db, sql)
        print(f"OK: migração aplicada a {args.db} via {args.sql}")
    else:
        print(f"Aviso: {args.sql} não encontrado — aplicando FALLBACK embutido.")
        apply_sql(args.db, FALLBACK_SQL)
        print(f"OK: schema criado via fallback em {args.db}")

if __name__ == "__main__":
    main()
