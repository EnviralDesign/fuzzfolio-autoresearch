import {
  Area,
  AreaChart,
  CartesianGrid,
  ReferenceLine,
  XAxis,
  YAxis,
} from "recharts";

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from "@/components/ui/chart";
import type { BasketCurve } from "@/lib/types";
import { formatDateTime, formatInt, formatNumber } from "@/lib/utils";

const equityChartConfig = {
  equity_r: {
    label: "Equity",
    color: "hsl(142 72% 55%)",
  },
} satisfies ChartConfig;

const drawdownChartConfig = {
  drawdown_r: {
    label: "Drawdown",
    color: "hsl(0 84% 68%)",
  },
} satisfies ChartConfig;

type BasketCurvePanelProps = {
  title: string;
  description: string;
  curve?: BasketCurve | null;
};

export function BasketCurvePanel({ title, description, curve }: BasketCurvePanelProps) {
  const points = curve?.points || [];

  return (
    <Card className="border-border/60 bg-card/85 shadow-2xl shadow-black/25">
      <CardHeader>
        <CardTitle className="text-2xl tracking-tight">{title}</CardTitle>
        <CardDescription className="max-w-3xl text-sm leading-7">{description}</CardDescription>
      </CardHeader>
      <CardContent className="space-y-6">
        {points.length ? (
          <>
            <div className="overflow-hidden rounded-3xl border border-emerald-500/20 bg-background/50 p-4">
              <div className="mb-3 text-xs uppercase tracking-[0.18em] text-emerald-300/80">
                Basket equity
              </div>
              <ChartContainer config={equityChartConfig} className="h-72 w-full">
                <AreaChart data={points} margin={{ left: 8, right: 8, top: 8, bottom: 0 }}>
                  <CartesianGrid vertical={false} strokeDasharray="3 3" />
                  <XAxis
                    dataKey="time"
                    minTickGap={28}
                    tickFormatter={(value) => formatTickDate(value)}
                  />
                  <YAxis
                    width={72}
                    tickFormatter={(value) => `${formatCompactR(value)}R`}
                  />
                  <ReferenceLine y={0} stroke="hsl(var(--border))" strokeDasharray="4 4" />
                  <ChartTooltip
                    cursor={false}
                    content={
                      <ChartTooltipContent
                        labelFormatter={(_, payload) => formatTooltipDate(payload?.[0]?.payload?.time)}
                        formatter={(value) => (
                          <div className="flex min-w-40 items-center justify-between gap-4">
                            <span className="text-muted-foreground">Equity</span>
                            <span className="font-mono font-medium text-foreground">
                              {formatSignedR(Number(value))}
                            </span>
                          </div>
                        )}
                      />
                    }
                  />
                  <Area
                    type="monotone"
                    dataKey="equity_r"
                    stroke="var(--color-equity_r)"
                    fill="var(--color-equity_r)"
                    fillOpacity={0.18}
                    strokeWidth={2.5}
                    dot={false}
                    activeDot={{ r: 4, strokeWidth: 1.5 }}
                  />
                </AreaChart>
              </ChartContainer>
            </div>

            <div className="overflow-hidden rounded-3xl border border-rose-500/20 bg-background/50 p-4">
              <div className="mb-3 text-xs uppercase tracking-[0.18em] text-rose-300/80">
                Basket drawdown
              </div>
              <ChartContainer config={drawdownChartConfig} className="h-56 w-full">
                <AreaChart data={points} margin={{ left: 8, right: 8, top: 8, bottom: 0 }}>
                  <CartesianGrid vertical={false} strokeDasharray="3 3" />
                  <XAxis
                    dataKey="time"
                    minTickGap={28}
                    tickFormatter={(value) => formatTickDate(value)}
                  />
                  <YAxis
                    width={72}
                    tickFormatter={(value) => `${formatCompactR(value)}R`}
                  />
                  <ReferenceLine y={0} stroke="hsl(var(--border))" strokeDasharray="4 4" />
                  <ChartTooltip
                    cursor={false}
                    content={
                      <ChartTooltipContent
                        labelFormatter={(_, payload) => formatTooltipDate(payload?.[0]?.payload?.time)}
                        formatter={(_value, _name, _item, _index, payload) => {
                          const row = payload as
                            | {
                                payload?: {
                                  drawdown_r?: number;
                                  equity_r?: number;
                                  realized_r?: number;
                                  closed_trade_count?: number;
                                };
                              }
                            | undefined;
                          const point = row?.payload;
                          return (
                            <div className="grid min-w-44 gap-1">
                              <TooltipRow label="Equity" value={formatSignedR(point?.equity_r)} />
                              <TooltipRow label="Drawdown" value={formatSignedR(point?.drawdown_r)} />
                              <TooltipRow label="Realized" value={formatSignedR(point?.realized_r)} />
                              <TooltipRow
                                label="Closed trades"
                                value={formatInt(point?.closed_trade_count)}
                              />
                            </div>
                          );
                        }}
                      />
                    }
                  />
                  <Area
                    type="monotone"
                    dataKey="drawdown_r"
                    stroke="var(--color-drawdown_r)"
                    fill="var(--color-drawdown_r)"
                    fillOpacity={0.35}
                    strokeWidth={2}
                    dot={false}
                    activeDot={{ r: 4, strokeWidth: 1.5 }}
                  />
                </AreaChart>
              </ChartContainer>
            </div>
          </>
        ) : (
          <div className="flex min-h-72 items-center justify-center rounded-3xl border border-dashed border-border/60 bg-background/50 text-sm text-muted-foreground">
            Basket curve not available yet.
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function TooltipRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center justify-between gap-4">
      <span className="text-muted-foreground">{label}</span>
      <span className="font-mono font-medium text-foreground">{value}</span>
    </div>
  );
}

function formatTooltipDate(value: unknown) {
  if (typeof value !== "number") {
    return "—";
  }
  return formatDateTime(new Date(value * 1000).toISOString());
}

function formatTickDate(value: unknown) {
  if (typeof value !== "number") {
    return "";
  }
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
  }).format(new Date(value * 1000));
}

function formatCompactR(value: unknown) {
  const numeric = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(numeric)) {
    return "0";
  }
  return formatNumber(numeric, Math.abs(numeric) >= 100 ? 0 : 1);
}

function formatSignedR(value: unknown) {
  const numeric = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(numeric)) {
    return "—";
  }
  const abs = formatNumber(Math.abs(numeric), 2);
  return `${numeric >= 0 ? "+" : "-"}${abs}R`;
}
