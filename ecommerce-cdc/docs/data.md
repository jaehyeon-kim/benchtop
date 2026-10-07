# Data

The tables the [ecommerce-cdc](../README.md) simulation writes, the topics Debezium makes from them, and the shape of a change event.

## Tables

The six tables are in PostgreSQL's `cdc` schema, each keyed on `id`. Times are ISO 8601 text in UTC. dynamic-des sends each row as JSON-friendly values, so a time arrives as a string, which a timestamp column does not accept. The models and their bounds are in [`models.py`](../ecommerce/core/models.py).

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

Each message's key holds the row's `id`. Each value is one Debezium change event with five fields: `before`, `after`, `source`, `op` and `ts_ms`. [Concepts](concepts.md#debezium-change-events) explains each field. `after` holds the whole row, with the table's columns above, and `before` is null in every update.

The S3 sink writes each event as one line of a JSON lines file in SeaweedFS. Each line is a JSON object with four fields: `key`, `value` (the change event), `offset` and `timestamp`.
