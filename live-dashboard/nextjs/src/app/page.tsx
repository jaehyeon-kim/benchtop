"use client";

import { useState } from "react";
import { Checkbox } from "@nextui-org/react";
import ReactECharts from "echarts-for-react";

import Metric from "@/components/metric";
import useDashboard from "@/lib/useDashboard";

export default function Home() {
  const [connected, setConnected] = useState(false);
  const { metricItems, chartOptions } = useDashboard("ws://127.0.0.1:8000/ws", connected);

  return (
    <div>
      <div className="mt-20">
        <div className="flex m-2 justify-between items-center">
          <h1 className="text-4xl font-bold">theLook eCommerce Dashboard</h1>
        </div>
        <div className="flex m-2 mt-5 justify-between items-center">
          <Checkbox color="primary" onChange={() => setConnected(!connected)}>
            Connect to WS Server
          </Checkbox>
        </div>
      </div>
      <div className="grid grid-cols-12 gap-4 mt-5">
        {metricItems.map((item, i) => (
          <Metric key={i} {...item} />
        ))}
      </div>
      <div className="grid grid-cols-12 gap-4 mt-5">
        {chartOptions.map((option, i) => (
          <ReactECharts key={i} className="col-span-12 md:col-span-6" option={option} style={{ height: "500px" }} />
        ))}
      </div>
    </div>
  );
}
