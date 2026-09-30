import { useEffect, useRef, useState } from "react";
import { EChartsOption } from "echarts-for-react";
import useWebSocket from "react-use-websocket";

import { MetricProps } from "@/components/metric";
import { createMetricItems, createOptionsItems, defaultMetrics, getMetrics, Record } from "@/lib/processing";

// Follows the WebSocket while `connected`, and turns each message into cards and charts.
export default function useDashboard(url: string, connected: boolean) {
  const [metricItems, setMetricItems] = useState<MetricProps[]>(createMetricItems(defaultMetrics, defaultMetrics));
  const [chartOptions, setChartOptions] = useState<EChartsOption[]>([]);
  const previous = useRef(defaultMetrics);
  const { lastJsonMessage } = useWebSocket<Record[]>(url, { share: false, shouldReconnect: () => true }, connected);

  useEffect(() => {
    if (!lastJsonMessage) return;
    const metrics = getMetrics(lastJsonMessage);
    setMetricItems(createMetricItems(metrics, previous.current));
    setChartOptions(createOptionsItems(lastJsonMessage));
    previous.current = metrics;
  }, [lastJsonMessage]);

  return { metricItems, chartOptions };
}
