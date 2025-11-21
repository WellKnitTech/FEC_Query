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
            <div className="flex justify-between h-16">
              <div className="flex items-center space-x-8">
                <Link to="/" className="text-xl font-bold text-gray-900 hover:text-blue-600">
                  FEC Campaign Finance Analysis
                </Link>
                <div className="flex space-x-4">
                  <Link
                    to="/"
                    className="text-gray-600 hover:text-gray-900 px-3 py-2 rounded-md text-sm font-medium"
                  >
                    Search
                  </Link>
                  <Link
                    to="/race"
                    className="text-gray-600 hover:text-gray-900 px-3 py-2 rounded-md text-sm font-medium"
                  >
                    Race Analysis
                  </Link>
                  <Link
                    to="/donor-analysis"
                    className="text-gray-600 hover:text-gray-900 px-3 py-2 rounded-md text-sm font-medium"
                  >
                    Donor Analysis
                  </Link>
                  <Link
                    to="/bulk-data"
                    className="text-gray-600 hover:text-gray-900 px-3 py-2 rounded-md text-sm font-medium"
                  >
                    Bulk Data
                  </Link>
                  <Link
                    to="/independent-expenditures"
                    className="text-gray-600 hover:text-gray-900 px-3 py-2 rounded-md text-sm font-medium"
                  >
                    Independent Expenditures
                  </Link>
                  <Link
                    to="/trends"
                    className="text-gray-600 hover:text-gray-900 px-3 py-2 rounded-md text-sm font-medium"
                  >
                    Trends
                  </Link>
                  <Link
                    to="/saved-searches"
                    className="text-gray-600 hover:text-gray-900 px-3 py-2 rounded-md text-sm font-medium"
                  >
                    Saved Searches
                  </Link>
                  <Link
                    to="/committees"
                    className="text-gray-600 hover:text-gray-900 px-3 py-2 rounded-md text-sm font-medium"
                  >
                    Committees
                  </Link>
                  <Link
                    to="/settings"
                    className="text-gray-600 hover:text-gray-900 px-3 py-2 rounded-md text-sm font-medium"
                  >
                    Settings
                  </Link>
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

