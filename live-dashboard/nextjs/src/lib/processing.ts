// Turns the WebSocket's records into metric cards and chart options, with no React.
// The Streamlit dashboard does the same in sales/dashboard/metrics.py.
import { EChartsOption } from "echarts-for-react";

import { MetricProps } from "@/components/metric";

export interface Record {
  user_id: string;
  age: number;
  gender: string;
  country: string;
  traffic_source: string;
  order_id: string;
  item_id: string;
  category: string;
  cost: number;
  item_status: string;
  sale_price: number;
  created_at: string;
}

export interface Metrics {
  num_orders: number;
  num_order_items: number;
  total_sales: number;
}

const LABELS: { [K in keyof Metrics]: string } = {
  num_orders: "Number of Orders",
  num_order_items: "Number of Order Items",
  total_sales: "Total Sales",
};
const CHARTS = { country: "Country", traffic_source: "Traffic Source" }; // revenue grouped by

export const defaultMetrics: Metrics = { num_orders: 0, num_order_items: 0, total_sales: 0 };

export function getMetrics(records: Record[]): Metrics {
  return {
    num_orders: new Set(records.map((r) => r.order_id)).size,
    num_order_items: new Set(records.map((r) => r.item_id)).size,
    total_sales: Math.round(records.reduce((sum, r) => sum + r.sale_price, 0)),
  };
}

export function createMetricItems(current: Metrics, previous: Metrics): MetricProps[] {
  return (Object.keys(LABELS) as (keyof Metrics)[]).map((key) => ({
    label: LABELS[key],
    value: current[key],
    delta: current[key] - previous[key],
    is_currency: key === "total_sales",
  }));
}

export function createOptionsItems(records: Record[]): EChartsOption[] {
  return (Object.keys(CHARTS) as (keyof typeof CHARTS)[]).map((column) => {
    const revenue = new Map<string, number>();
    for (const r of records) revenue.set(r[column], (revenue.get(r[column]) ?? 0) + r.sale_price);
    const bars = [...revenue].sort((a, b) => b[1] - a[1]);
    return {
      title: { text: `Revenue by ${CHARTS[column]}` },
      grid: { containLabel: true }, // room for the rotated axis labels
      xAxis: { type: "category", data: bars.map(([name]) => name), axisLabel: { rotate: 75 } },
      yAxis: { type: "value" },
      series: [{ type: "bar", colorBy: "data", data: bars.map(([, value]) => Math.round(value)) }],
      tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
    };
  });
}
