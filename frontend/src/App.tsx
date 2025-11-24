import { Suspense, lazy } from 'react';
import { BrowserRouter as Router, Routes, Route, Link } from 'react-router-dom';
import './App.css';

const Dashboard = lazy(() => import('./pages/Dashboard'));
const CandidateDetail = lazy(() => import('./pages/CandidateDetail'));
const DonorAnalysis = lazy(() => import('./pages/DonorAnalysis'));
const RaceAnalysis = lazy(() => import('./pages/RaceAnalysis'));
const BulkDataManagement = lazy(() => import('./pages/BulkDataManagement'));
const IndependentExpenditures = lazy(() => import('./pages/IndependentExpenditures'));
const SavedSearches = lazy(() => import('./pages/SavedSearches'));
const TrendAnalysis = lazy(() => import('./pages/TrendAnalysis'));
const CommitteeDetail = lazy(() => import('./pages/CommitteeDetail'));
const Committees = lazy(() => import('./pages/Committees'));
const Settings = lazy(() => import('./pages/Settings'));

function PageSkeleton({ title }: { title: string }) {
  return (
    <div className="p-6">
      <div className="h-6 w-48 bg-gray-200 rounded animate-pulse" aria-hidden="true" />
      <p className="mt-4 text-gray-500">Loading {title}...</p>
    </div>
  );
}

function App() {
  const workflowGroups = [
    {
      title: 'Follow the candidate',
      items: [
        { to: '/', label: 'Candidates', helper: 'Start a search, then open the detail view' },
        { to: '/committees', label: 'Committees', helper: 'Review affiliated committees and leadership PACs' },
        { to: '/independent-expenditures', label: 'Independent Expenditures', helper: 'Scan outside spend tied to the race' },
      ],
    },
    {
      title: 'Deeper analysis',
      items: [
        { to: '/donor-analysis', label: 'Donor Analysis', helper: 'Identify top donors and geographic clusters' },
        { to: '/trends', label: 'Trends', helper: 'Compare cycle-to-cycle velocity and receipts' },
        { to: '/saved-searches', label: 'Saved Searches', helper: 'Jump back into prior investigations' },
      ],
    },
  ];

  return (
    <Router
      future={{
        v7_startTransition: true,
        v7_relativeSplatPath: true,
      }}
    >
      <div className="min-h-screen bg-gray-50">
        <nav className="bg-white shadow-sm">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
            <div className="flex flex-col gap-3 py-4">
              <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
                <Link to="/" className="text-xl font-bold text-gray-900 hover:text-blue-600">
                  FEC Campaign Finance Analysis
                </Link>
                <div className="flex flex-wrap gap-2" title="Jump straight into high-signal views used most often by investigators.">
                  <Link
                    to="/independent-expenditures"
                    className="px-3 py-2 bg-indigo-600 text-white rounded-md text-sm font-medium hover:bg-indigo-700"
                  >
                    High-spend PACs
                  </Link>
                  <Link
                    to="/donor-analysis"
                    className="px-3 py-2 bg-emerald-600 text-white rounded-md text-sm font-medium hover:bg-emerald-700"
                  >
                    Donor hotspots
                  </Link>
                  <Link
                    to="/bulk-data"
                    className="px-3 py-2 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700"
                  >
                    Bulk ingest
                  </Link>
                </div>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {workflowGroups.map((group) => (
                  <div key={group.title} className="bg-gray-50 border border-gray-200 rounded-lg p-3">
                    <div className="text-xs uppercase text-gray-500 mb-2">{group.title}</div>
                    <div className="flex flex-wrap gap-2">
                      {group.items.map((item) => (
                        <Link
                          key={item.to}
                          to={item.to}
                          className="px-3 py-2 bg-white border border-gray-200 rounded-md text-sm text-gray-700 hover:border-blue-400 hover:text-blue-700"
                          title={item.helper}
                        >
                          {item.label}
                        </Link>
                      ))}
                    </div>
                  </div>
                ))}
                <div className="bg-white border border-gray-200 rounded-lg p-3 flex flex-col gap-2" title="Open the race and committee maps directly from navigation.">
                  <div className="text-xs uppercase text-gray-500">Context views</div>
                  <div className="flex flex-wrap gap-2">
                    <Link to="/race" className="px-3 py-2 bg-white border border-gray-200 rounded-md text-sm text-gray-700 hover:border-blue-400 hover:text-blue-700">
                      Race Analysis
                    </Link>
                    <Link to="/committees" className="px-3 py-2 bg-white border border-gray-200 rounded-md text-sm text-gray-700 hover:border-blue-400 hover:text-blue-700">
                      Committee Explorer
                    </Link>
                    <Link to="/settings" className="px-3 py-2 bg-white border border-gray-200 rounded-md text-sm text-gray-700 hover:border-blue-400 hover:text-blue-700">
                      Settings
                    </Link>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </nav>
        <Routes>
          <Route
            path="/"
            element={
              <Suspense fallback={<PageSkeleton title="Dashboard" />}>
                <Dashboard />
              </Suspense>
            }
          />
          <Route
            path="/candidate/:candidateId"
            element={
              <Suspense fallback={<PageSkeleton title="Candidate Details" />}>
                <CandidateDetail />
              </Suspense>
            }
          />
          <Route
            path="/race"
            element={
              <Suspense fallback={<PageSkeleton title="Race Analysis" />}>
                <RaceAnalysis />
              </Suspense>
            }
          />
          <Route
            path="/donor-analysis"
            element={
              <Suspense fallback={<PageSkeleton title="Donor Analysis" />}>
                <DonorAnalysis />
              </Suspense>
            }
          />
          <Route
            path="/bulk-data"
            element={
              <Suspense fallback={<PageSkeleton title="Bulk Data" />}>
                <BulkDataManagement />
              </Suspense>
            }
          />
          <Route
            path="/independent-expenditures"
            element={
              <Suspense fallback={<PageSkeleton title="Independent Expenditures" />}>
                <IndependentExpenditures />
              </Suspense>
            }
          />
          <Route
            path="/trends"
            element={
              <Suspense fallback={<PageSkeleton title="Trend Analysis" />}>
                <TrendAnalysis />
              </Suspense>
            }
          />
          <Route
            path="/saved-searches"
            element={
              <Suspense fallback={<PageSkeleton title="Saved Searches" />}>
                <SavedSearches />
              </Suspense>
            }
          />
          <Route
            path="/committee/:committeeId"
            element={
              <Suspense fallback={<PageSkeleton title="Committee Details" />}>
                <CommitteeDetail />
              </Suspense>
            }
          />
          <Route
            path="/committees"
            element={
              <Suspense fallback={<PageSkeleton title="Committees" />}>
                <Committees />
              </Suspense>
            }
          />
          <Route
            path="/settings"
            element={
              <Suspense fallback={<PageSkeleton title="Settings" />}>
                <Settings />
              </Suspense>
            }
          />
        </Routes>
      </div>
    </Router>
  );
}

export default App;

