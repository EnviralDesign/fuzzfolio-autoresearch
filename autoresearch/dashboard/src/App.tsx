import { BrowserRouter, Routes, Route } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { Sidebar } from "@/components/layout/Sidebar";
import { TopBar } from "@/components/layout/TopBar";
import { OverviewPage } from "@/pages/OverviewPage";
import { LeaderboardPage } from "@/pages/LeaderboardPage";
import { ModelsPage } from "@/pages/ModelsPage";
import { ValidationPage } from "@/pages/ValidationPage";
import { SimilarityPage } from "@/pages/SimilarityPage";
import { TradeoffPage } from "@/pages/TradeoffPage";
import { RunsPage } from "@/pages/RunsPage";
import { RunDetailPage } from "@/pages/RunDetailPage";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
    },
  },
});

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <div className="flex h-screen overflow-hidden">
          <Sidebar />
          <div className="flex flex-col flex-1 min-w-0 overflow-hidden">
            <TopBar />
            <main className="flex-1 overflow-y-auto">
              <Routes>
                <Route path="/" element={<OverviewPage />} />
                <Route path="/leaderboard" element={<LeaderboardPage />} />
                <Route path="/models" element={<ModelsPage />} />
                <Route path="/validation" element={<ValidationPage />} />
                <Route path="/similarity" element={<SimilarityPage />} />
                <Route path="/tradeoff" element={<TradeoffPage />} />
                <Route path="/runs" element={<RunsPage />} />
                <Route path="/runs/:runId" element={<RunDetailPage />} />
              </Routes>
            </main>
          </div>
        </div>
      </BrowserRouter>
    </QueryClientProvider>
  );
}
