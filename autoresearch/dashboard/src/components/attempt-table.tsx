import type { AttemptCatalogRow } from "@/lib/types";
import {
  formatDateTime,
  formatNumber,
  scoreTone,
} from "@/lib/utils";
import type { ColumnDef } from "@tanstack/react-table";
import { DataTable } from "./data-table";

type AttemptTableProps = {
  rows: AttemptCatalogRow[];
  showSelectionFields?: boolean;
  caption?: string;
  onAttemptClick?: (attempt: AttemptCatalogRow) => void;
};

const baseColumns: ColumnDef<AttemptCatalogRow, unknown>[] = [
  {
    accessorKey: "candidate_name",
    header: "Candidate",
    cell: ({ row }) => {
      const r = row.original;
      const role = String(r.attempt_role || r.play_hand_role || "").trim();
      const decision = String(r.attempt_decision || "").trim();
      return (
        <div className="space-y-1">
          <div className="font-medium text-foreground">
            {String(r.candidate_name || r.attempt_id)}
          </div>
          {role || decision || r.is_canonical_playhand_attempt ? (
            <div className="flex flex-wrap gap-1">
              {r.is_canonical_playhand_attempt ? (
                <span className="rounded border border-emerald-400/40 bg-emerald-400/10 px-1.5 py-0.5 text-[0.66rem] uppercase tracking-wide text-emerald-200">
                  Canonical
                </span>
              ) : null}
              {role ? (
                <span className="rounded border border-border/70 bg-background/45 px-1.5 py-0.5 text-[0.66rem] uppercase tracking-wide text-muted-foreground">
                  {role.replaceAll("_", " ")}
                </span>
              ) : null}
              {decision && decision !== "canonical" ? (
                <span className="rounded border border-border/70 bg-background/45 px-1.5 py-0.5 text-[0.66rem] uppercase tracking-wide text-muted-foreground">
                  {decision.replaceAll("_", " ")}
                </span>
              ) : null}
            </div>
          ) : null}
          <div className="text-xs text-muted-foreground">
            {String(r.attempt_id)}
          </div>
        </div>
      );
    },
    enableSorting: true,
  },
  {
    accessorKey: "score_36m",
    header: "Score Lab",
    cell: ({ row }) => (
      <span className={scoreTone(row.original.score_36m ?? null)}>
        {formatNumber(row.original.score_36m ?? null, 2)}
      </span>
    ),
    enableSorting: true,
  },
  {
    accessorKey: "legacy_quality_score_36m",
    header: "Legacy",
    cell: ({ row }) => formatNumber(row.original.legacy_quality_score_36m ?? null, 2),
    enableSorting: true,
  },
  {
    accessorKey: "trades_per_month_36m",
    header: "Trades / mo",
    cell: ({ row }) =>
      formatNumber(row.original.trades_per_month_36m ?? null, 2),
    enableSorting: true,
  },
  {
    accessorKey: "max_drawdown_r_36m",
    header: "Drawdown",
    cell: ({ row }) =>
      `${formatNumber(row.original.max_drawdown_r_36m ?? null, 2)}R`,
    enableSorting: true,
  },
  {
    accessorKey: "created_at",
    header: "Created",
    cell: ({ row }) => formatDateTime(row.original.created_at ?? null),
    enableSorting: true,
  },
];

const selectionColumns: ColumnDef<AttemptCatalogRow, unknown>[] = [
  {
    accessorKey: "selection_utility",
    header: "Utility",
    cell: ({ row }) =>
      formatNumber(row.original.selection_utility ?? null, 2),
    enableSorting: true,
  },
  {
    accessorKey: "max_sameness_to_selected",
    header: "Sameness",
    cell: ({ row }) =>
      formatNumber(
        (row.original.max_sameness_to_selected ??
          row.original.max_sameness_to_board) as number | null,
        3
      ),
    enableSorting: true,
  },
];

export function AttemptTable({
  rows,
  showSelectionFields = false,
  caption,
  onAttemptClick,
}: AttemptTableProps) {
  const columns = showSelectionFields
    ? [...baseColumns, ...selectionColumns]
    : baseColumns;

  return (
    <DataTable
      columns={columns}
      data={rows}
      caption={caption}
      emptyMessage="No rows match the current view."
      onRowClick={onAttemptClick}
    />
  );
}
