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
      return (
        <div className="space-y-1">
          <div className="font-medium text-foreground">
            {String(r.candidate_name || r.attempt_id)}
          </div>
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
    header: "Score",
    cell: ({ row }) => (
      <span className={scoreTone(row.original.score_36m ?? null)}>
        {formatNumber(row.original.score_36m ?? null, 2)}
      </span>
    ),
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