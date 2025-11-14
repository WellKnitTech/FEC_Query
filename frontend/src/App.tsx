import { BrowserRouter as Router, Routes, Route, Link } from 'react-router-dom';
import Dashboard from './pages/Dashboard';
import CandidateDetail from './pages/CandidateDetail';
import DonorAnalysis from './pages/DonorAnalysis';
import RaceAnalysis from './pages/RaceAnalysis';
import BulkDataManagement from './pages/BulkDataManagement';
import IndependentExpenditures from './pages/IndependentExpenditures';
import SavedSearches from './pages/SavedSearches';
import TrendAnalysis from './pages/TrendAnalysis';
import CommitteeDetail from './pages/CommitteeDetail';
import Committees from './pages/Committees';
import Settings from './pages/Settings';
import './App.css';

function App() {
  return (
    <Router>
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
          <Route path="/" element={<Dashboard />} />
          <Route path="/candidate/:candidateId" element={<CandidateDetail />} />
          <Route path="/race" element={<RaceAnalysis />} />
          <Route path="/donor-analysis" element={<DonorAnalysis />} />
          <Route path="/bulk-data" element={<BulkDataManagement />} />
          <Route path="/independent-expenditures" element={<IndependentExpenditures />} />
          <Route path="/trends" element={<TrendAnalysis />} />
          <Route path="/saved-searches" element={<SavedSearches />} />
          <Route path="/committee/:committeeId" element={<CommitteeDetail />} />
          <Route path="/committees" element={<Committees />} />
          <Route path="/settings" element={<Settings />} />
        </Routes>
      </div>
    </Router>
  );
}

export default App;

