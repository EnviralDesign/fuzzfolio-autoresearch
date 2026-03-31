import { useEffect, useRef, memo } from "react";
import { createChart, ColorType, LineSeries, type IChartApi, type Time } from "lightweight-charts";
import type { CurvePoint } from "../../lib/types";

interface LightweightChartProps {
  className?: string;
  height?: number;
  equityData?: CurvePoint[];
  hasFullBacktest?: boolean;
  onCalculate?: () => void;
  isCalculating?: boolean;
}

export const LightweightChart = memo(function LightweightChart({
  className,
  height = 320,
  equityData,
  hasFullBacktest = false,
  onCalculate,
  isCalculating,
}: LightweightChartProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);

  useEffect(() => {
    if (!containerRef.current || !hasFullBacktest) return;

    const chart = createChart(containerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: "transparent" },
        textColor: "#99abc4",
        fontFamily: "'Inter', 'Segoe UI', system-ui, sans-serif",
        fontSize: 11,
      },
      grid: {
        vertLines: { color: "rgba(147, 190, 255, 0.06)" },
        horzLines: { color: "rgba(147, 190, 255, 0.06)" },
      },
      width: containerRef.current.clientWidth,
      height,
      rightPriceScale: {
        borderColor: "rgba(147, 190, 255, 0.12)",
      },
      timeScale: {
        borderColor: "rgba(147, 190, 255, 0.12)",
        timeVisible: true,
      },
      crosshair: {
        vertLine: { color: "rgba(96, 214, 195, 0.3)" },
        horzLine: { color: "rgba(96, 214, 195, 0.3)" },
      },
    });

    chartRef.current = chart;

    const equitySeries = chart.addSeries(LineSeries, {
      color: "#60d6c3",
      lineWidth: 2,
      priceFormat: { type: "custom", formatter: (v: number) => v.toFixed(1) + "R" },
    });

    const drawdownSeries = chart.addSeries(LineSeries, {
      color: "#f97070",
      lineWidth: 1,
      priceFormat: { type: "custom", formatter: (v: number) => v.toFixed(1) + "R" },
    });

      if (equityData) {
      const equityPoints = equityData.map((p) => ({
        time: p.time as unknown as never,
        value: p.equity_r,
      }));
      const drawdownPoints = equityData.map((p) => ({
        time: p.time as unknown as never,
        value: p.equity_r - p.drawdown_r,
      }));
      equitySeries.setData(equityPoints);
      drawdownSeries.setData(drawdownPoints);

      const from = equityPoints[0].time as Time;
      const to = equityPoints[equityPoints.length - 1].time as Time;
      chart.timeScale().setVisibleRange({ from, to });
    } else {
      chart.timeScale().fitContent();
    }

    const handleResize = () => {
      if (containerRef.current) {
        chart.applyOptions({ width: containerRef.current.clientWidth });
      }
    };
    window.addEventListener("resize", handleResize);

    return () => {
      window.removeEventListener("resize", handleResize);
      chart.remove();
      chartRef.current = null;
    };
  }, [height, equityData, hasFullBacktest]);

  if (!hasFullBacktest) {
    return (
      <div className={className}>
        <div
          className="rounded-lg border border-border/50 bg-card/30 flex items-center justify-center"
          style={{ height }}
        >
          <div className="flex flex-col items-center gap-2">
            {onCalculate && (
              <button
                onClick={onCalculate}
                disabled={isCalculating}
                className="px-3 py-1.5 text-xs rounded-md bg-primary/10 hover:bg-primary/20 text-primary border border-primary/20 transition-colors disabled:opacity-50"
              >
                {isCalculating ? "Calculating 3yr backtest..." : "Calculate Full 3yr Backtest"}
              </button>
            )}
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className={className}>
      <div
        ref={containerRef}
        className="rounded-lg overflow-hidden border border-border/50"
      />
      <div className="flex items-center gap-4 mt-1.5 justify-center">
        <div className="flex items-center gap-1.5">
          <div className="w-3 h-0.5 rounded-full bg-[#60d6c3]" />
          <span className="text-[10px] text-muted-foreground">Equity (R)</span>
        </div>
        <div className="flex items-center gap-1.5">
          <div className="w-3 h-0.5 rounded-full bg-[#f97070]" />
          <span className="text-[10px] text-muted-foreground">Drawdown floor (R)</span>
        </div>
      </div>
    </div>
  );
});

export const LightweightChartPlaceholder = LightweightChart;
