import type { ReactNode } from "react";
import { NavLink, useLocation } from "react-router-dom";
import {
  Blocks,
  GalleryVerticalEnd,
  GitBranch,
  LayoutDashboard,
  Telescope,
} from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarInset,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarProvider,
  SidebarSeparator,
  SidebarTrigger,
} from "@/components/ui/sidebar";
import { useViewerState } from "@/hooks/use-viewer-data";
import { formatDateTime, formatInt } from "@/lib/utils";

type AppShellProps = {
  children: ReactNode;
};

const NAV_ITEMS = [
  { to: "/", label: "Corpus", icon: LayoutDashboard },
  { to: "/shortlist", label: "Shortlist", icon: GalleryVerticalEnd },
  { to: "/promotion", label: "Promotion", icon: GitBranch },
  { to: "/catalog", label: "Catalog", icon: Blocks },
  { to: "/runs", label: "Runs", icon: Telescope },
];

export function AppShell({ children }: AppShellProps) {
  const location = useLocation();
  const { data } = useViewerState();

  return (
    <SidebarProvider defaultOpen>
      <Sidebar collapsible="icon" variant="inset">
        <SidebarHeader className="gap-4 px-3 pt-4">
          <div className="space-y-2 rounded-3xl border border-sidebar-border/70 bg-sidebar-primary/10 p-4">
            <div className="text-[0.7rem] uppercase tracking-[0.24em] text-sidebar-foreground/60">
              Autoresearch
            </div>
            <div className="space-y-1">
              <div className="text-lg font-semibold tracking-tight text-sidebar-foreground">
                Corpus Viewer
              </div>
              <p className="text-sm leading-6 text-sidebar-foreground/70">
                A read-only workstation for the story from raw corpus to shortlist to promotion.
              </p>
            </div>
          </div>
        </SidebarHeader>
        <SidebarContent>
          <SidebarGroup>
            <SidebarGroupLabel>Navigation</SidebarGroupLabel>
            <SidebarGroupContent>
              <SidebarMenu>
                {NAV_ITEMS.map((item) => {
                  const Icon = item.icon;
                  const active =
                    item.to === "/"
                      ? location.pathname === "/"
                      : location.pathname.startsWith(item.to);
                  return (
                    <SidebarMenuItem key={item.to}>
                      <SidebarMenuButton asChild isActive={active} tooltip={item.label}>
                        <NavLink to={item.to}>
                          <Icon className="h-4 w-4" />
                          <span>{item.label}</span>
                        </NavLink>
                      </SidebarMenuButton>
                    </SidebarMenuItem>
                  );
                })}
              </SidebarMenu>
            </SidebarGroupContent>
          </SidebarGroup>

          <SidebarSeparator />

          <SidebarGroup>
            <SidebarGroupLabel>Current Corpus</SidebarGroupLabel>
            <SidebarGroupContent>
              <div className="space-y-3 px-2 text-sm text-sidebar-foreground/75">
                <div className="rounded-2xl border border-sidebar-border/70 bg-sidebar-accent/30 p-3">
                  <div className="text-[0.7rem] uppercase tracking-[0.18em] text-sidebar-foreground/55">
                    Valid 36mo
                  </div>
                  <div className="mt-1 text-2xl font-semibold">
                    {formatInt(data?.corpus_summary?.attempts_with_valid_full_backtest_36m)}
                  </div>
                </div>
                <div className="rounded-2xl border border-sidebar-border/70 bg-sidebar-accent/20 p-3">
                  <div className="text-[0.7rem] uppercase tracking-[0.18em] text-sidebar-foreground/55">
                    Score 40+
                  </div>
                  <div className="mt-1 text-2xl font-semibold">
                    {formatInt(data?.corpus_summary?.score_36m_ge_40)}
                  </div>
                </div>
              </div>
            </SidebarGroupContent>
          </SidebarGroup>
        </SidebarContent>
        <SidebarFooter className="p-3">
          <div className="rounded-3xl border border-sidebar-border/70 bg-sidebar-accent/20 p-4 text-sm text-sidebar-foreground/70">
            <div className="flex items-center justify-between gap-3">
              <span>Viewer status</span>
              <Badge variant={data?.audit?.status === "ready_for_review" ? "default" : "secondary"}>
                {data?.audit?.status || "loading"}
              </Badge>
            </div>
            <div className="mt-3 text-xs leading-5 text-sidebar-foreground/55">
              Updated {formatDateTime(data?.generated_at)}
            </div>
          </div>
        </SidebarFooter>
      </Sidebar>
      <SidebarInset className="bg-transparent">
        <div className="sticky top-0 z-20 border-b border-border/50 bg-background/70 backdrop-blur-xl">
          <div className="flex items-center justify-between gap-4 px-4 py-3 md:px-6">
            <div className="flex items-center gap-3">
              <SidebarTrigger />
              <div>
                <div className="text-[0.72rem] uppercase tracking-[0.2em] text-muted-foreground">
                  Derived Artifacts Only
                </div>
                <div className="text-sm text-foreground/80">
                  The dashboard is now a viewer. CLI builds the evidence, this UI reads it.
                </div>
              </div>
            </div>
            <div className="hidden items-center gap-2 md:flex">
              <Button asChild variant="outline" size="sm">
                <a href="/api/state" target="_blank" rel="noreferrer">
                  JSON state
                </a>
              </Button>
              <Button asChild variant="outline" size="sm">
                <a href="/api/catalog" target="_blank" rel="noreferrer">
                  Catalog
                </a>
              </Button>
            </div>
          </div>
        </div>
        <div className="mx-auto flex w-full max-w-[1680px] flex-1 flex-col px-4 py-6 md:px-6">
          {children}
        </div>
      </SidebarInset>
    </SidebarProvider>
  );
}
