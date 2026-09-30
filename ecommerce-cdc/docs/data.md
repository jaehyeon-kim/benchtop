# Data

The tables the [ecommerce-cdc](../README.md) simulation writes, the topics Debezium makes from them, and a change event.

## Tables

The six tables are in PostgreSQL's `cdc` schema, each keyed on `id`. Times are ISO 8601 text in UTC. dynamic-des sends each row as JSON-friendly values, so a time arrives as a string, which a timestamp column does not accept. The models and their bounds are in `ecommerce/core/models.py`.

| Table | Rows | Columns |
|---|---|---|
| `dist_centers` | 10, written once | `id`, `name`, `latitude`, `longitude` |
| `products` | 260, written once: 5 brands × 26 categories × 2 departments, from a fixed seed | `id`, `name`, `brand`, `category`, `department`, `retail_price`, `cost` (39% to 59% of the price), `sku`, `distribution_center_id` |
| `users` | 100 at the start, plus each new buyer; updated on a move | `id`, `first_name`, `last_name`, `email`, `age` (12 to 70), `gender`, `street_address`, `postal_code`, `city`, `state`, `country`, `latitude`, `longitude`, `traffic_source`, `created_at`, `updated_at` |
| `orders` | one per order; updated at each change of status | `id`, `user_id`, `status`, `num_of_items` (1 to 4), `created_at`, `updated_at`, `shipped_at`, `delivered_at`, `cancelled_at`, `returned_at` |
| `order_items` | one per product in an order; carries its order's status and times | `id`, `order_id`, `product_id`, `status`, `quantity` (1 to 3), `sale_price`, and the order's time columns |
| `events` | one per page view, including anonymous visitors; inserted only | `id`, `user_id` (null if anonymous), `session_id`, `sequence_number`, `event_type` (`home`, `product`, `cart`, `purchase`), `uri`, `city`, `state`, `postal_code`, `browser`, `traffic_source`, `ip_address`, `created_at` |

Users live in 60 cities, the 10 largest in each of 6 countries, chosen by population. An order's status moves only from `Processing` to `Shipped` or `Cancelled`, from `Shipped` to `Delivered`, and from `Delivered` to `Returned`.

## Topics and schemas

Debezium writes one topic per table, `ecommerce.cdc.<table>`, each with 3 partitions, and registers two subjects for each in Karapace: `ecommerce.cdc.<table>-key`, holding `id`, and `ecommerce.cdc.<table>-value`, holding the change event. The simulation reads live changes from one more topic, `ecommerce-control`.

## Change events

An order moving from `Processing` to `Shipped`, decoded with its schema:

```json
{
  "before": null,
  "after": {
    "id": "23b2d4a9-360d-4973-a6da-fd1cc394ca10",
    "user_id": "862dd089-c746-4127-89d7-db4fcab59a2e",
    "status": "Shipped",
    "num_of_items": 1,
    "created_at": "2026-09-30T13:38:56+00:00",
    "updated_at": "2026-09-30T13:39:01+00:00",
    "shipped_at": "2026-09-30T13:39:01+00:00",
    "delivered_at": null,
    "cancelled_at": null,
    "returned_at": null
  },
  "source": {
    "version": "3.5.1.Final",
    "connector": "postgresql",
    "name": "ecommerce",
    "ts_ms": 1790775541329,
    "snapshot": "false",
    "db": "odctl",
    "schema": "cdc",
    "table": "orders",
    "txId": 45922,
    "lsn": 161004832
  },
  "op": "u",
  "ts_ms": 1790775541830
}
```

Some `source` and time fields are left out. `ts_ms` minus `source.ts_ms` shows Debezium sent the change about half a second after PostgreSQL made it. In SeaweedFS, the S3 sink writes each event as one line, wrapped with its key, offset and timestamp. The first line of the first `orders` file is the same order's create event:

```json
{"offset":0,"value":{"before":null,"after":{...},"source":{...},"op":"c",...},"key":{"id":"23b2d4a9-360d-4973-a6da-fd1cc394ca10"},"timestamp":"..."}
```
