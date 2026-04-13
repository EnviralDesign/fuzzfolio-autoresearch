import { useState } from "react";

import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { ProfileDropModal } from "@/components/profile-drop-modal";
import type { ShortlistProfileDrop } from "@/lib/types";
import { formatInt } from "@/lib/utils";

type ProfileDropGridProps = {
  items: ShortlistProfileDrop[];
};

export function ProfileDropGrid({ items }: ProfileDropGridProps) {
  const [selectedItem, setSelectedItem] = useState<ShortlistProfileDrop | null>(null);

  if (items.length === 0) {
    return (
      <div className="flex min-h-64 items-center justify-center rounded-3xl border border-dashed border-border/60 bg-background/40 text-sm text-muted-foreground">
        No profile drops have been rendered for the current shortlist.
      </div>
    );
  }

  return (
    <>
      <div className="grid gap-5 md:grid-cols-2 xl:grid-cols-3">
        {items.map((item, index) => (
          <Card
            key={item.attempt_id}
            className="border-border/60 bg-card/80 shadow-2xl shadow-black/20 cursor-pointer transition-opacity hover:opacity-80"
            onClick={() => setSelectedItem(item)}
          >
            <CardHeader className="gap-3">
              <div className="flex items-center justify-between gap-3">
                <Badge variant="secondary">#{formatInt(index + 1)}</Badge>
                <Badge variant={item.status === "rendered" ? "default" : "outline"}>
                  {item.status || "unknown"}
                </Badge>
              </div>
              <div className="space-y-1">
                <CardTitle className="text-base leading-6">
                  {item.display_name || item.candidate_name || item.attempt_id}
                </CardTitle>
                <CardDescription className="text-xs">
                  {item.tagline || item.short_description || item.run_id}
                </CardDescription>
              </div>
            </CardHeader>
            <CardContent className="space-y-4">
              {item.png_url ? (
                <div className="overflow-hidden rounded-3xl border border-border/60 bg-background/70">
                  <img
                    src={item.png_url}
                    alt={item.display_name || item.candidate_name || item.attempt_id}
                    className="h-auto w-full object-cover"
                  />
                </div>
              ) : (
                <div className="flex min-h-64 items-center justify-center rounded-3xl border border-dashed border-border/60 bg-background/50 text-sm text-muted-foreground">
                  PNG missing.
                </div>
              )}
            </CardContent>
          </Card>
        ))}
      </div>
      <ProfileDropModal
        isOpen={selectedItem !== null}
        onClose={() => setSelectedItem(null)}
        profilePathUrl={selectedItem?.png_url ?? null}
        candidateName={
          selectedItem?.display_name || selectedItem?.candidate_name || selectedItem?.attempt_id || ""
        }
      />
    </>
  );
}
