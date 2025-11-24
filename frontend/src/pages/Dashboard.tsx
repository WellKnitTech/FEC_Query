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
        <div className="mt-4 grid grid-cols-1 md:grid-cols-3 gap-3">
          <div className="bg-white border border-gray-200 rounded-lg p-3" title="Begin with a candidate lookup, then pivot to committees and spend.">
            <div className="text-sm font-semibold text-gray-900">Investigative workflow</div>
            <p className="text-xs text-gray-600">1) Open candidate → 2) Committees → 3) Independent expenditures</p>
          </div>
          <div className="bg-white border border-gray-200 rounded-lg p-3" title="Use quick filters when you know what you need to pull.">
            <div className="text-sm font-semibold text-gray-900">Quick access</div>
            <p className="text-xs text-gray-600">Links in the nav jump straight to donor hotspots and high-spend PACs.</p>
          </div>
          <div className="bg-white border border-gray-200 rounded-lg p-3" title="Avoid re-running the same searches—save them for the next audit.">
            <div className="text-sm font-semibold text-gray-900">Stay organized</div>
            <p className="text-xs text-gray-600">Save searches so your next review starts from the last checkpoint.</p>
          </div>
        </div>
      </div>
      <CandidateSearch />
    </div>
  );
}

