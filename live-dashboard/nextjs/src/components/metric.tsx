"use client";

import {
  Card,
  CardHeader,
  CardBody,
  Divider,
  CardFooter,
} from "@nextui-org/react";

export interface MetricProps {
  label: string;
  value: number;
  delta: number;
  is_currency: boolean;
}

export default function Metric({
  label,
  value,
  delta,
  is_currency,
}: MetricProps) {
  const formatted_value = is_currency
    ? "$ ".concat(value.toLocaleString())
    : value.toLocaleString();
  const arrowColor = delta == 0 ? "black" : delta > 0 ? "green" : "red";
  return (
    <div className="col-span-12 md:col-span-4">
      <Card>
        <CardHeader>{label}</CardHeader>
        <CardBody>
          <h1 className="text-4xl font-bold">{formatted_value}</h1>
        </CardBody>
        <Divider />
        <CardFooter>
          <svg
            height={25}
            viewBox="0 0 24 24"
            aria-hidden="true"
            focusable="false"
            fill={arrowColor}
            xmlns="http://www.w3.org/2000/svg"
            color="inherit"
          >
            <path fill="none" d="M0 0h24v24H0V0z"></path>
            <path d="M4 12l1.41 1.41L11 7.83V20h2V7.83l5.58 5.59L20 12l-8-8-8 8z"></path>
          </svg>
          <h1 className="text-xl">{delta.toLocaleString()}</h1>
        </CardFooter>
      </Card>
    </div>
  );
}
