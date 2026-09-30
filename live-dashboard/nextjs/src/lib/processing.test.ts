import { describe, expect, it } from "vitest";

import {
  createMetricItems,
  createOptionsItems,
  getMetrics,
  Record,
} from "@/lib/processing";

const records: Record[] = [
  { user_id: "u1", age: 30, gender: "F", country: "France", traffic_source: "Search", order_id: "o1", item_id: "i1", category: "Jeans", cost: 4, item_status: "Shipped", sale_price: 10.4, created_at: "2026-09-30T00:00:00+00:00" },
  { user_id: "u2", age: 41, gender: "M", country: "Brasil", traffic_source: "Email", order_id: "o1", item_id: "i2", category: "Swim", cost: 9, item_status: "Shipped", sale_price: 20, created_at: "2026-09-30T00:00:00+00:00" },
  { user_id: "u1", age: 30, gender: "F", country: "France", traffic_source: "Search", order_id: "o2", item_id: "i3", category: "Socks", cost: 2, item_status: "Complete", sale_price: 15, created_at: "2026-09-30T00:00:00+00:00" },
];

describe("getMetrics", () => {
  it("counts distinct orders and items, and rounds total sales", () => {
    expect(getMetrics(records)).toEqual({ num_orders: 2, num_order_items: 3, total_sales: 45 });
  });
});

describe("createMetricItems", () => {
  it("shows each metric with its change since the last update", () => {
    const items = createMetricItems(
      { num_orders: 5, num_order_items: 8, total_sales: 100 },
      { num_orders: 3, num_order_items: 8, total_sales: 120 },
    );
    expect(items.map((i) => i.delta)).toEqual([2, 0, -20]);
    expect(items.map((i) => i.is_currency)).toEqual([false, false, true]);
  });
});

describe("createOptionsItems", () => {
  it("charts revenue by country and by source, largest first", () => {
    const [country, source] = createOptionsItems(records);
    expect(country.title.text).toBe("Revenue by Country");
    expect(country.xAxis.data).toEqual(["France", "Brasil"]);
    expect(country.series[0].data).toEqual([25, 20]);
    expect(source.title.text).toBe("Revenue by Traffic Source");
  });
});
