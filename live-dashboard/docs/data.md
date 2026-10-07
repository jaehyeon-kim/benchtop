# Data

The tables the [live-dashboard](../README.md) simulation writes, and the records the WebSocket server sends. The tables are in the `dashboard` schema of odctl's PostgreSQL database `odctl`, created by [`postgres.py`](../sales/stores/postgres.py).

Times are ISO 8601 text in UTC, because dynamic-des publishes rows as JSON-friendly values. A query casts them with `created_at::timestamptz` before comparing.

## Shop tables

| Table | Column | Description |
|---|---|---|
| `products` | `id` | 1 to 260: one product per brand (5), category (26) and department (2), the same every run |
| | `name` | such as `Kestrel Women's Jeans` |
| | `category`, `department` | such as `Jeans`, and `Men` or `Women` |
| | `retail_price`, `cost` | dollars; the cost is 39% to 59% of the price |
| `users` | `id` | a UUID; a first-time buyer signs up as a user |
| | `age`, `gender` | 12 to 70, and `M` or `F` |
| | `country`, `traffic_source` | one of six countries, and how the user found the shop, such as `Search` |
| | `created_at` | when the user signed up |
| `orders` | `id`, `user_id` | a UUID, and the buyer |
| | `status` | `Processing`, `Shipped`, `Complete`, `Returned` or `Cancelled` |
| | `num_of_item` | 1 to 4 |
| | `created_at` | when it was placed |
| `order_items` | `id`, `order_id`, `user_id` | a UUID, its order and the buyer |
| | `product_id`, `sale_price` | the product, and its retail price |
| | `status`, `created_at` | the order's status and time |

Orders and order items are upserted on `id` each time the status changes, so a row always shows the latest status. [Concepts](concepts.md#discrete-event-simulation) explains how an order moves between statuses.

## Parameter table

`params` holds the live changes: `id`, `param_path` (such as `sales.arrival.visitor.rate`), `param_value`, `is_applied` and `created_at`. [Concepts](concepts.md#live-changes-through-the-parameter-table) explains how a row becomes a change.

## Records the server sends

Each message is one JSON list, with a record for each order item of the last five minutes, joined to its user and product:

| Field | From | Used for |
|---|---|---|
| `order_id` | `order_items` | **Number of Orders**, counted once per order |
| `item_id` | `order_items.id` | **Number of Order Items** |
| `sale_price` | `order_items` | **Total Sales** and both charts |
| `country`, `traffic_source` | `users` | the two charts' groups |
| `user_id`, `age`, `gender` | `users` | not shown |
| `category`, `cost` | `products` | not shown |
| `item_status`, `created_at` | `order_items` | not shown |

The dashboards count every record whatever its status, so a cancelled order still counts.
