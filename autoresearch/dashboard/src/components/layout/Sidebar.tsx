import { NavLink } from "react-router-dom";
import {
  LayoutDashboard,
  Trophy,
  Cpu,
  ShieldCheck,
  Fingerprint,
  ArrowLeftRight,
  FolderOpen,
  CandlestickChart,
} from "lucide-react";
import { cn } from "@/lib/utils";

const NAV_ITEMS = [
  { to: "/", icon: LayoutDashboard, label: "Overview" },
  { to: "/leaderboard", icon: Trophy, label: "Leaderboard" },
  { to: "/models", icon: Cpu, label: "Models" },
  { to: "/validation", icon: ShieldCheck, label: "Validation" },
  { to: "/similarity", icon: Fingerprint, label: "Similarity" },
  { to: "/tradeoff", icon: ArrowLeftRight, label: "Tradeoff" },
  { to: "/runs", icon: FolderOpen, label: "Runs" },
] as const;

export function Sidebar() {
  return (
    <aside className="flex flex-col w-56 shrink-0 border-r border-border bg-card/50 h-screen sticky top-0">
      {/* Logo */}
      <div className="flex items-center gap-2.5 px-5 py-5 border-b border-border">
        <CandlestickChart className="w-6 h-6 text-primary" />
        <span className="font-semibold text-sm tracking-tight">Autoresearch</span>
      </div>

      {/* Navigation */}
      <nav className="flex-1 py-3 px-3 space-y-0.5 overflow-y-auto">
        {NAV_ITEMS.map(({ to, icon: Icon, label }) => (
          <NavLink
            key={to}
            to={to}
            end={to === "/"}
            className={({ isActive }) =>
              cn(
                "flex items-center gap-2.5 px-3 py-2 rounded-lg text-sm font-medium transition-colors",
                isActive
                  ? "bg-primary/10 text-primary border border-primary/20"
                  : "text-muted-foreground hover:text-foreground hover:bg-surface-hover border border-transparent"
              )
            }
          >
            <Icon className="w-4 h-4" />
            {label}
          </NavLink>
        ))}
      </nav>

      {/* Footer */}
      <div className="px-5 py-4 border-t border-border text-xs text-muted-foreground">
        Fuzzfolio Autoresearch
      </div>
    </aside>
  );
}
