import { useEffect, useRef, useState } from 'react';
import { candidateApi, Candidate } from '../services/api';
import { useNavigate } from 'react-router-dom';

export default function CandidateSearch() {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<Candidate[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const navigate = useNavigate();
  const searchAbortController = useRef<AbortController | null>(null);
  const prefetchAbortController = useRef<AbortController | null>(null);
  const debounceTimeout = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Helper function to extract last name for sorting
  const getLastName = (name: string): string => {
    if (!name) return '';
    const parts = name.trim().split(/\s+/);
    // Return the last word (last name), handling suffixes like Jr., Sr., III, etc.
    return parts[parts.length - 1] || '';
  };

  // Sort candidates alphabetically by last name
  const sortCandidatesByLastName = (candidates: Candidate[]): Candidate[] => {
    return [...candidates].sort((a, b) => {
      const lastNameA = getLastName(a.name || '').toLowerCase();
      const lastNameB = getLastName(b.name || '').toLowerCase();
      return lastNameA.localeCompare(lastNameB);
    });
  };

  const prefetchCandidateDetail = async (candidateId: string) => {
    if (!candidateId) return;
    if (prefetchAbortController.current) {
      prefetchAbortController.current.abort();
    }

    const controller = new AbortController();
    prefetchAbortController.current = controller;

    try {
      await candidateApi.getById(candidateId, controller.signal);
    } catch (err: any) {
      if (err?.code === 'ERR_CANCELED' || err?.name === 'CanceledError') {
        return;
      }
      // Prefetch failures should not block the main search flow
      console.error('Failed to prefetch candidate details', err);
    }
  };

  const performSearch = async (searchTerm: string) => {
    if (!searchTerm.trim()) {
      setResults([]);
      setError(null);
      return;
    }

    if (searchAbortController.current) {
      searchAbortController.current.abort();
    }

    const controller = new AbortController();
    searchAbortController.current = controller;

    setLoading(true);
    setError(null);
    try {
      const candidates = await candidateApi.search({ name: searchTerm, limit: 20 }, controller.signal);
      // Sort candidates by last name
      const sortedCandidates = sortCandidatesByLastName(candidates);
      setResults(sortedCandidates);
      if (sortedCandidates.length === 0) {
        setError('No candidates found. Try a different search term.');
      } else {
        prefetchCandidateDetail(sortedCandidates[0].candidate_id);
      }
    } catch (err: any) {
      if (err?.code === 'ERR_CANCELED' || err?.name === 'CanceledError') {
        return;
      }
      let errorMessage = 'Failed to search candidates. Please try again.';
      if (err?.response?.data?.detail) {
        errorMessage = err.response.data.detail;
      } else if (err?.message) {
        errorMessage = err.message;
      }
      setError(errorMessage);
    } finally {
      setLoading(false);
    }
  };

  const handleSearch = async (e: React.FormEvent) => {
    e.preventDefault();
    await performSearch(query);
  };

  useEffect(() => {
    if (debounceTimeout.current) {
      clearTimeout(debounceTimeout.current);
    }

    debounceTimeout.current = setTimeout(() => {
      performSearch(query);
    }, 400);

    return () => {
      if (debounceTimeout.current) {
        clearTimeout(debounceTimeout.current);
      }
    };
  }, [query]);

  useEffect(() => {
    return () => {
      if (searchAbortController.current) {
        searchAbortController.current.abort();
      }
      if (prefetchAbortController.current) {
        prefetchAbortController.current.abort();
      }
      if (debounceTimeout.current) {
        clearTimeout(debounceTimeout.current);
      }
    };
  }, []);

  const handleCandidateClick = (candidateId: string) => {
    navigate(`/candidate/${candidateId}`);
  };

  return (
    <div className="w-full max-w-4xl mx-auto">
      <form onSubmit={handleSearch} className="mb-6">
        <div className="flex gap-2">
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search for a candidate by name..."
            className="flex-1 px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          />
          <button
            type="submit"
            disabled={loading}
            className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {loading ? 'Searching...' : 'Search'}
          </button>
        </div>
      </form>

      {error && (
        <div className="mb-4 p-4 bg-red-100 border border-red-400 text-red-700 rounded">
          {error}
        </div>
      )}

      {results.length > 0 && (
        <div className="bg-white rounded-lg shadow">
          <div className="px-6 py-4 border-b border-gray-200">
            <h2 className="text-lg font-semibold text-gray-900">
              Search Results ({results.length})
            </h2>
          </div>
          <div className="divide-y divide-gray-200">
            {results.map((candidate) => (
              <div
                key={candidate.candidate_id}
                onClick={() => handleCandidateClick(candidate.candidate_id)}
                className="px-6 py-4 hover:bg-gray-50 cursor-pointer transition-colors"
              >
                <div className="flex justify-between items-start">
                  <div>
                    <h3 className="text-lg font-medium text-gray-900">
                      {candidate.name}
                    </h3>
                    <div className="mt-1 text-sm text-gray-600">
                      {candidate.office && (
                        <span className="mr-4">
                          Office: {candidate.office}
                          {candidate.state && ` (${candidate.state})`}
                        </span>
                      )}
                      {candidate.party && (
                        <span className="mr-4">Party: {candidate.party}</span>
                      )}
                      {candidate.election_years && candidate.election_years.length > 0 && (
                        <span>
                          Elections: {candidate.election_years.join(', ')}
                        </span>
                      )}
                    </div>
                  </div>
                  <button className="text-blue-600 hover:text-blue-800 font-medium">
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

