import { Route, Routes } from "react-router-dom";

import { AppShell } from "@/components/app-shell";
import { CatalogPage } from "@/pages/CatalogPage";
import { CorpusPage } from "@/pages/CorpusPage";
import { PromotionPage } from "@/pages/PromotionPage";
import { RunDetailPage } from "@/pages/RunDetailPage";
import { RunsPage } from "@/pages/RunsPage";
import { ShortlistPage } from "@/pages/ShortlistPage";

export default function App() {
  return (
    <AppShell>
      <Routes>
        <Route path="/" element={<CorpusPage />} />
        <Route path="/shortlist" element={<ShortlistPage />} />
        <Route path="/promotion" element={<PromotionPage />} />
        <Route path="/catalog" element={<CatalogPage />} />
        <Route path="/runs" element={<RunsPage />} />
        <Route path="/runs/:runId" element={<RunDetailPage />} />
      </Routes>
    </AppShell>
  );
}
