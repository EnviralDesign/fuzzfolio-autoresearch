import { cn, formatNumber, scoreBg, scoreColor } from "@/lib/utils";

interface ScoreBadgeProps {
  score: number | null | undefined;
  digits?: number;
  className?: string;
}

export function ScoreBadge({ score, digits = 2, className }: ScoreBadgeProps) {
  return (
    <span
      className={cn(
        "inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-semibold border",
        scoreBg(score),
        scoreColor(score),
        className
      )}
    >
      {score !== null && score !== undefined ? formatNumber(score, digits) : "unscored"}
    </span>
  );
}
