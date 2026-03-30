import { useEffect, useRef, memo } from "react";
import { createChart, ColorType, LineSeries, type IChartApi } from "lightweight-charts";

interface LightweightChartProps {
  className?: string;
  height?: number;
}

/**
 * Placeholder Lightweight Chart component.
 * Shows a styled empty chart shell ready for equity curve / backtest data.
 * Will be wired to real CurvePoint[] data in a future iteration.
 */
export const LightweightChartPlaceholder = memo(function LightweightChartPlaceholder({
  className,
  height = 320,
}: LightweightChartProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);

  useEffect(() => {
    if (!containerRef.current) return;

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

    const lineSeries = chart.addSeries(LineSeries, {
      color: "#60d6c3",
      lineWidth: 2,
      priceFormat: { type: "custom", formatter: (v: number) => v.toFixed(1) + "R" },
    });

    // Generate some subtle placeholder data to show the chart is alive
    const now = Math.floor(Date.now() / 1000);
    const day = 86400;
    const placeholderData = Array.from({ length: 60 }, (_, i) => ({
      time: (now - (60 - i) * day) as unknown as never,
      value: 10 + Math.sin(i * 0.15) * 3 + i * 0.4 + Math.random() * 1.5,
    }));
    lineSeries.setData(placeholderData);

    chart.timeScale().fitContent();

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
  }, [height]);

  return (
    <div className={className}>
      <div
        ref={containerRef}
        className="rounded-lg overflow-hidden border border-border/50"
      />
      <p className="text-[10px] text-muted-foreground mt-1.5 opacity-60 text-center">
        Equity curve placeholder — will show real backtest data when wired up
      </p>
    </div>
  );
});
