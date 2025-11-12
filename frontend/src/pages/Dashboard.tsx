import CandidateSearch from '../components/CandidateSearch';

export default function Dashboard() {
  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-gray-900 mb-2">
          FEC Campaign Finance Analysis
        </h1>
        <p className="text-gray-600">
          Search for federal candidates and analyze their campaign finance data, track money flows, and detect potential fraud patterns.
        </p>
      </div>
      <CandidateSearch />
    </div>
  );
}

