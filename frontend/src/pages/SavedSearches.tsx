import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { savedSearchApi, SavedSearch } from '../services/api';

export default function SavedSearches() {
  const navigate = useNavigate();
  const [searches, setSearches] = useState<SavedSearch[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [filterType, setFilterType] = useState<string>('');

  useEffect(() => {
    const abortController = new AbortController();
    
    const loadSearches = async () => {
      setLoading(true);
      setError(null);
      try {
        const data = await savedSearchApi.list(filterType || undefined, abortController.signal);
        if (!abortController.signal.aborted) {
          setSearches(data);
        }
      } catch (err: any) {
        // Don't set error if request was aborted
        if (err.name === 'AbortError' || abortController.signal.aborted) {
          return;
        }
        if (!abortController.signal.aborted) {
          setError(err?.response?.data?.detail || err?.message || 'Failed to load saved searches');
        }
      } finally {
        if (!abortController.signal.aborted) {
          setLoading(false);
        }
      }
    };
    
    loadSearches();
    
    return () => {
      abortController.abort();
    };
  }, [filterType]);

  const handleDelete = async (searchId: number) => {
    if (!confirm('Are you sure you want to delete this saved search?')) return;

    try {
      await savedSearchApi.delete(searchId);
      await loadSearches();
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to delete saved search');
    }
  };

  const handleRunSearch = (search: SavedSearch) => {
    const params = search.search_params;
    
    switch (search.search_type) {
      case 'candidate':
        if (params.candidate_id) {
          navigate(`/candidate/${params.candidate_id}`);
        } else {
          navigate(`/?name=${params.name || ''}`);
        }
        break;
      case 'committee':
        if (params.committee_id) {
          navigate(`/committee/${params.committee_id}`);
        } else {
          navigate(`/committees?name=${params.name || ''}`);
        }
        break;
      case 'race':
        navigate(`/race?office=${params.office || ''}&state=${params.state || ''}&district=${params.district || ''}&year=${params.year || ''}`);
        break;
      case 'donor':
        navigate(`/donor-analysis?contributorName=${params.contributor_name || ''}`);
        break;
      case 'independent_expenditure':
        navigate(`/independent-expenditures?candidate_id=${params.candidate_id || ''}&committee_id=${params.committee_id || ''}`);
        break;
      default:
        break;
    }
  };

  const groupedSearches = searches.reduce((acc, search) => {
    if (!acc[search.search_type]) {
      acc[search.search_type] = [];
    }
    acc[search.search_type].push(search);
    return acc;
  }, {} as Record<string, SavedSearch[]>);

  const searchTypeLabels: Record<string, string> = {
    candidate: 'Candidates',
    committee: 'Committees',
    race: 'Races',
    donor: 'Donors',
    independent_expenditure: 'Independent Expenditures',
  };

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <h1 className="text-3xl font-bold text-gray-900 mb-6">Saved Searches & Bookmarks</h1>

      <div className="bg-white rounded-lg shadow p-6 mb-6">
        <div className="flex justify-between items-center mb-4">
          <h2 className="text-xl font-semibold">All Saved Searches</h2>
          <select
            value={filterType}
            onChange={(e) => setFilterType(e.target.value)}
            className="px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"
          >
            <option value="">All Types</option>
            <option value="candidate">Candidates</option>
            <option value="committee">Committees</option>
            <option value="race">Races</option>
            <option value="donor">Donors</option>
            <option value="independent_expenditure">Independent Expenditures</option>
          </select>
        </div>

        {error && (
          <div className="mb-4 p-4 bg-red-100 border border-red-400 text-red-700 rounded">
            {error}
          </div>
        )}

        {loading ? (
          <div className="animate-pulse">
            <div className="h-32 bg-gray-200 rounded"></div>
          </div>
        ) : searches.length === 0 ? (
          <p className="text-gray-500">No saved searches yet. Save searches from other pages to see them here.</p>
        ) : (
          <div className="space-y-6">
            {Object.entries(groupedSearches).map(([type, typeSearches]) => (
              <div key={type}>
                <h3 className="text-lg font-semibold mb-3">{searchTypeLabels[type] || type}</h3>
                <div className="space-y-2">
                  {typeSearches.map((search) => (
                    <div
                      key={search.id}
                      className="p-4 border border-gray-200 rounded-lg hover:bg-gray-50 flex justify-between items-center"
                    >
                      <div className="flex-1">
                        <h4 className="font-semibold text-gray-900">{search.name}</h4>
                        <p className="text-sm text-gray-600 mt-1">
                          Created: {search.created_at ? new Date(search.created_at).toLocaleDateString() : 'N/A'}
                        </p>
                        <div className="text-xs text-gray-500 mt-1">
                          {Object.entries(search.search_params || {})
                            .filter(([_, v]) => v !== null && v !== undefined && v !== '')
                            .map(([k, v]) => `${k}: ${v}`)
                            .join(', ')}
                        </div>
                      </div>
                      <div className="flex gap-2">
                        <button
                          onClick={() => handleRunSearch(search)}
                          className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 text-sm"
                        >
                          Run Search
                        </button>
                        <button
                          onClick={() => handleDelete(search.id!)}
                          className="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 text-sm"
                        >
                          Delete
                        </button>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

