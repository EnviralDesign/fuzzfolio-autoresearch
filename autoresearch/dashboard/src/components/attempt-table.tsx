import { Link } from "react-router-dom";

import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import type { AttemptCatalogRow } from "@/lib/types";
import {
  compactRunId,
  formatInt,
  formatNumber,
  scoreTone,
} from "@/lib/utils";

type AttemptTableProps = {
  rows: AttemptCatalogRow[];
  showSelectionFields?: boolean;
  caption?: string;
};

export function AttemptTable({
  rows,
  showSelectionFields = false,
  caption,
}: AttemptTableProps) {
  return (
    <div className="overflow-hidden rounded-3xl border border-border/60 bg-background/50">
      <Table>
        <TableHeader>
          <TableRow className="border-border/60">
            <TableHead className="min-w-64">Candidate</TableHead>
            <TableHead>Score</TableHead>
            <TableHead>Trades / mo</TableHead>
            <TableHead>Drawdown</TableHead>
            <TableHead>Strategy Key</TableHead>
            <TableHead>Run</TableHead>
            {showSelectionFields ? <TableHead>Utility</TableHead> : null}
            {showSelectionFields ? <TableHead>Sameness</TableHead> : null}
          </TableRow>
        </TableHeader>
        <TableBody>
          {rows.map((row) => {
            const runId = String(row.run_id || "");
            return (
              <TableRow key={String(row.attempt_id)} className="border-border/50 align-top">
                <TableCell>
                  <div className="space-y-1">
                    <div className="font-medium text-foreground">
                      {String(row.candidate_name || row.attempt_id)}
                    </div>
                    <div className="text-xs text-muted-foreground">
                      {String(row.attempt_id)}
                    </div>
                  </div>
                </TableCell>
                <TableCell className={scoreTone(row.score_36m ?? null)}>
                  {formatNumber(row.score_36m ?? null, 2)}
                </TableCell>
                <TableCell>{formatNumber(row.trades_per_month_36m ?? null, 2)}</TableCell>
                <TableCell>{formatNumber(row.max_drawdown_r_36m ?? null, 2)}R</TableCell>
                <TableCell className="max-w-52 truncate text-muted-foreground">
                  {String(row.strategy_key_36m || "—")}
                </TableCell>
                <TableCell>
                  <Link
                    to={`/runs/${encodeURIComponent(runId)}`}
                    className="text-sm text-muted-foreground transition hover:text-foreground"
                  >
                    {compactRunId(runId)}
                  </Link>
                </TableCell>
                {showSelectionFields ? (
                  <TableCell>{formatNumber(row.selection_utility ?? null, 2)}</TableCell>
                ) : null}
                {showSelectionFields ? (
                  <TableCell>{formatNumber((row.max_sameness_to_selected ?? row.max_sameness_to_board) as number | null, 3)}</TableCell>
                ) : null}
              </TableRow>
            );
          })}
          {rows.length === 0 ? (
            <TableRow>
              <TableCell
                colSpan={showSelectionFields ? 8 : 6}
                className="py-12 text-center text-sm text-muted-foreground"
              >
                No rows match the current view.
              </TableCell>
            </TableRow>
          ) : null}
        </TableBody>
      </Table>
      {caption ? (
        <div className="border-t border-border/60 px-4 py-3 text-xs text-muted-foreground">
          {caption} Showing {formatInt(rows.length)} rows.
        </div>
      ) : null}
    </div>
  );
}
