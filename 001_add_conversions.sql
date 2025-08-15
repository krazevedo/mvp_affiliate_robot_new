-- sql_migrations/001_add_conversions.sql
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
