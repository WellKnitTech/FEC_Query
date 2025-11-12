import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { committeeApi, CommitteeSummary } from '../services/api';

export default function CommitteeSearch() {
  const navigate = useNavigate();
  const [name, setName] = useState('');
  const [committeeType, setCommitteeType] = useState('');
  const [state, setState] = useState('');
  const [committees, setCommittees] = useState<CommitteeSummary[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSearch = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError(null);

    try {
      const results = await committeeApi.search({
        name: name || undefined,
        committee_type: committeeType || undefined,
        state: state || undefined,
        limit: 50,
      });
      setCommittees(results);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to search committees');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="bg-white rounded-lg shadow p-6">
      <h2 className="text-xl font-semibold mb-4">Search Committees</h2>
      <form onSubmit={handleSearch} className="mb-4">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Committee Name</label>
            <input
              type="text"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="Search by name..."
              className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Committee Type</label>
            <input
              type="text"
              value={committeeType}
              onChange={(e) => setCommitteeType(e.target.value)}
              placeholder="e.g., N, Q, O"
              className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">State</label>
            <input
              type="text"
              value={state}
              onChange={(e) => setState(e.target.value.toUpperCase())}
              placeholder="TX"
              maxLength={2}
              className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"
            />
          </div>
        </div>
        <button
          type="submit"
          disabled={loading}
          className="mt-4 px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50"
        >
          {loading ? 'Searching...' : 'Search'}
        </button>
      </form>

      {error && (
        <div className="mb-4 p-4 bg-red-100 border border-red-400 text-red-700 rounded">
          {error}
        </div>
      )}

      {committees.length > 0 && (
        <div className="mt-6">
          <h3 className="text-lg font-semibold mb-4">Results ({committees.length})</h3>
          <div className="space-y-2">
            {committees.map((committee) => (
              <div
                key={committee.committee_id}
                className="p-4 border border-gray-200 rounded-lg hover:bg-gray-50 cursor-pointer"
                onClick={() => navigate(`/committee/${committee.committee_id}`)}
              >
                <div className="flex justify-between items-start">
                  <div>
                    <h4 className="font-semibold text-gray-900">{committee.name}</h4>
                    <p className="text-sm text-gray-600">
                      {committee.committee_type_full || committee.committee_type} • {committee.state || 'N/A'}
                    </p>
                    <p className="text-xs text-gray-500 mt-1">ID: {committee.committee_id}</p>
                  </div>
                  <button className="text-blue-600 hover:text-blue-800 text-sm font-medium">
                    View Details →
                  </button>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

