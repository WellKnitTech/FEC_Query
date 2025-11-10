import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { candidateApi, Candidate, FinancialSummary } from '../services/api';
import { Bar } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js';
import ExportButton from '../components/ExportButton';

ChartJS.register(CategoryScale, LinearScale, BarElement, Title, Tooltip, Legend);

export default function RaceAnalysis() {
  const navigate = useNavigate();
  const [office, setOffice] = useState('H');
  const [state, setState] = useState('TX');
  const [district, setDistrict] = useState('21');
  const [year, setYear] = useState<number | undefined>(undefined);
  const [candidates, setCandidates] = useState<Candidate[]>([]);
  const [financials, setFinancials] = useState<Record<string, FinancialSummary[]>>({});
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedCandidates, setSelectedCandidates] = useState<Set<string>>(new Set());
  const [searchQuery, setSearchQuery] = useState('');
  const [sortColumn, setSortColumn] = useState<string | null>(null);
  const [sortDirection, setSortDirection] = useState<'asc' | 'desc'>('asc');

  const officeNames: Record<string, string> = {
    P: 'President',
    S: 'Senate',
    H: 'House of Representatives',
  };

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

  const handleSearch = async () => {
    setLoading(true);
    setError(null);
    setFinancials({});
    setSelectedCandidates(new Set());

    try {
      const results = await candidateApi.getRaceCandidates({
        office,
        state,
        district: office === 'H' ? district : undefined,
        year,
        limit: 100,
      });
      // Sort candidates by last name
      const sortedCandidates = sortCandidatesByLastName(results);
      setCandidates(sortedCandidates);

      // Fetch financials for all candidates using batch endpoint
      try {
        const candidateIds = sortedCandidates.map(c => c.candidate_id);
        const financialsMap = await candidateApi.getBatchFinancials(candidateIds);
        setFinancials(financialsMap);
      } catch (err) {
        console.error('Failed to fetch batch financials, falling back to individual calls:', err);
        // Fallback to individual calls if batch fails
        const financialPromises = sortedCandidates.map(async (candidate) => {
          try {
            const candidateFinancials = await candidateApi.getFinancials(candidate.candidate_id);
            return { candidateId: candidate.candidate_id, financials: candidateFinancials };
          } catch (err) {
            console.error(`Failed to fetch financials for ${candidate.candidate_id}:`, err);
            return { candidateId: candidate.candidate_id, financials: [] };
          }
        });

        const financialResults = await Promise.all(financialPromises);
        const financialsMap: Record<string, FinancialSummary[]> = {};
        financialResults.forEach(({ candidateId, financials: fs }) => {
          financialsMap[candidateId] = fs;
        });
        setFinancials(financialsMap);
      }
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to load race candidates');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const toggleCandidateSelection = (candidateId: string) => {
    const newSelected = new Set(selectedCandidates);
    if (newSelected.has(candidateId)) {
      newSelected.delete(candidateId);
    } else {
      newSelected.add(candidateId);
    }
    setSelectedCandidates(newSelected);
  };

  const getLatestFinancials = (candidateId: string): FinancialSummary | null => {
    const candidateFinancials = financials[candidateId];
    if (!candidateFinancials || candidateFinancials.length === 0) {
      return null;
    }
    // Return the most recent cycle
    return candidateFinancials.sort((a, b) => b.cycle - a.cycle)[0];
  };

  // Filter and sort candidates
  const getFilteredAndSortedCandidates = (): Candidate[] => {
    let filtered = candidates;

    // Apply search filter
    if (searchQuery.trim()) {
      const query = searchQuery.toLowerCase();
      filtered = filtered.filter((candidate) => {
        const name = (candidate.name || '').toLowerCase();
        const party = (candidate.party || '').toLowerCase();
        return name.includes(query) || party.includes(query);
      });
    }

    // Apply sorting
    if (sortColumn) {
      filtered = [...filtered].sort((a, b) => {
        let aValue: any;
        let bValue: any;

        switch (sortColumn) {
          case 'name':
            aValue = getLastName(a.name || '').toLowerCase();
            bValue = getLastName(b.name || '').toLowerCase();
            break;
          case 'party':
            aValue = (a.party || '').toLowerCase();
            bValue = (b.party || '').toLowerCase();
            break;
          case 'total_receipts':
            aValue = getLatestFinancials(a.candidate_id)?.total_receipts || 0;
            bValue = getLatestFinancials(b.candidate_id)?.total_receipts || 0;
            break;
          case 'cash_on_hand':
            aValue = getLatestFinancials(a.candidate_id)?.cash_on_hand || 0;
            bValue = getLatestFinancials(b.candidate_id)?.cash_on_hand || 0;
            break;
          default:
            return 0;
        }

        if (aValue < bValue) return sortDirection === 'asc' ? -1 : 1;
        if (aValue > bValue) return sortDirection === 'asc' ? 1 : -1;
        return 0;
      });
    }

    return filtered;
  };

  const handleSort = (column: string) => {
    if (sortColumn === column) {
      // Toggle direction if clicking the same column
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      // Set new column and default to ascending
      setSortColumn(column);
      setSortDirection('asc');
    }
  };

  const getSortIcon = (column: string) => {
    if (sortColumn !== column) {
      return (
        <span className="ml-1 text-gray-400">
          <svg className="w-4 h-4 inline" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 16V4m0 0L3 8m4-4l4 4m6 0v12m0 0l4-4m-4 4l-4-4" />
          </svg>
        </span>
      );
    }
    return (
      <span className="ml-1 text-blue-600">
        {sortDirection === 'asc' ? (
          <svg className="w-4 h-4 inline" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 15l7-7 7 7" />
          </svg>
        ) : (
          <svg className="w-4 h-4 inline" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
          </svg>
        )}
      </span>
    );
  };

  const filteredCandidates = getFilteredAndSortedCandidates();

  useEffect(() => {
    // Auto-search on mount with default values
    handleSearch();
  }, []);

  const selectedFinancials = Array.from(selectedCandidates)
    .map((id) => {
      const candidate = candidates.find((c) => c.candidate_id === id);
      const financial = getLatestFinancials(id);
      return { candidate, financial };
    })
    .filter((item) => item.candidate && item.financial);

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <h1 className="text-3xl font-bold text-gray-900 mb-6">Race Analysis</h1>

      <div className="bg-white rounded-lg shadow p-6 mb-6">
        <h2 className="text-xl font-semibold mb-4">Search Race</h2>
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Office</label>
            <select
              value={office}
              onChange={(e) => setOffice(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"
            >
              <option value="P">President</option>
              <option value="S">Senate</option>
              <option value="H">House of Representatives</option>
            </select>
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
          {office === 'H' && (
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">District</label>
              <input
                type="text"
                value={district}
                onChange={(e) => setDistrict(e.target.value)}
                placeholder="21"
                className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"
              />
            </div>
          )}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Year (Optional)</label>
            <input
              type="number"
              value={year || ''}
              onChange={(e) => setYear(e.target.value ? parseInt(e.target.value) : undefined)}
              placeholder="2024"
              className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"
            />
          </div>
        </div>
        <button
          onClick={handleSearch}
          disabled={loading}
          className="mt-4 px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50"
        >
          {loading ? 'Loading...' : 'Search Race'}
        </button>
      </div>

      {error && (
        <div className="mb-4 p-4 bg-red-100 border border-red-400 text-red-700 rounded">
          {error}
        </div>
      )}

      {candidates.length > 0 && (
        <div className="space-y-6">
          <div className="bg-white rounded-lg shadow">
            <div className="px-6 py-4 border-b border-gray-200 flex justify-between items-center">
              <div className="flex-1">
                <h2 className="text-xl font-semibold">
                  {officeNames[office]} - {state}
                  {office === 'H' && ` District ${district}`}
                  {year && ` (${year})`}
                </h2>
                <div className="text-sm text-gray-600 mt-1">
                  {filteredCandidates.length} of {candidates.length} candidate{candidates.length !== 1 ? 's' : ''} shown
                </div>
              </div>
              <div className="ml-4">
                <ExportButton
                  candidateIds={candidates.map(c => c.candidate_id)}
                  office={office}
                  state={state}
                  district={office === 'H' ? district : undefined}
                  year={year}
                  isRace={true}
                />
              </div>
            </div>
            <div className="px-6 py-4 border-b border-gray-200">
              <div className="max-w-md">
                <label htmlFor="search" className="block text-sm font-medium text-gray-700 mb-2">
                  Search Candidates
                </label>
                <input
                  id="search"
                  type="text"
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  placeholder="Search by name or party..."
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                />
              </div>
            </div>
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Select
                    </th>
                    <th 
                      className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100 select-none"
                      onClick={() => handleSort('name')}
                    >
                      <div className="flex items-center">
                        Name
                        {getSortIcon('name')}
                      </div>
                    </th>
                    <th 
                      className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100 select-none"
                      onClick={() => handleSort('party')}
                    >
                      <div className="flex items-center">
                        Party
                        {getSortIcon('party')}
                      </div>
                    </th>
                    <th 
                      className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100 select-none"
                      onClick={() => handleSort('total_receipts')}
                    >
                      <div className="flex items-center">
                        Total Receipts
                        {getSortIcon('total_receipts')}
                      </div>
                    </th>
                    <th 
                      className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100 select-none"
                      onClick={() => handleSort('cash_on_hand')}
                    >
                      <div className="flex items-center">
                        Cash on Hand
                        {getSortIcon('cash_on_hand')}
                      </div>
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Actions
                    </th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {filteredCandidates.length === 0 ? (
                    <tr>
                      <td colSpan={6} className="px-6 py-8 text-center text-gray-500">
                        {searchQuery.trim() 
                          ? `No candidates match "${searchQuery}"`
                          : 'No candidates found'}
                      </td>
                    </tr>
                  ) : (
                    filteredCandidates.map((candidate) => {
                      const financial = getLatestFinancials(candidate.candidate_id);
                      return (
                        <tr key={candidate.candidate_id} className="hover:bg-gray-50">
                          <td className="px-6 py-4 whitespace-nowrap">
                            <input
                              type="checkbox"
                              checked={selectedCandidates.has(candidate.candidate_id)}
                              onChange={() => toggleCandidateSelection(candidate.candidate_id)}
                              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                            />
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap">
                            <div className="text-sm font-medium text-gray-900">{candidate.name}</div>
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap">
                            <div className="text-sm text-gray-500">{candidate.party || 'N/A'}</div>
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap">
                            <div className="text-sm text-gray-900">
                              {financial
                                ? `$${(financial.total_receipts / 1000).toFixed(1)}K`
                                : 'Loading...'}
                            </div>
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap">
                            <div className="text-sm text-gray-900">
                              {financial
                                ? `$${(financial.cash_on_hand / 1000).toFixed(1)}K`
                                : 'Loading...'}
                            </div>
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm font-medium">
                            <button
                              onClick={() => navigate(`/candidate/${candidate.candidate_id}`)}
                              className="text-blue-600 hover:text-blue-900"
                            >
                              View Details →
                            </button>
                          </td>
                        </tr>
                      );
                    })
                  )}
                </tbody>
              </table>
            </div>
          </div>

          {selectedFinancials.length > 0 && (
            <div className="space-y-6">
              <div className="bg-white rounded-lg shadow p-6">
                <h2 className="text-xl font-semibold mb-4">
                  Comparison ({selectedFinancials.length} selected)
                </h2>
                <div className="overflow-x-auto">
                  <table className="min-w-full divide-y divide-gray-200">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                          Candidate
                        </th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                          Total Receipts
                        </th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                          Total Disbursements
                        </th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                          Cash on Hand
                        </th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                          Individual Contributions
                        </th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                          PAC Contributions
                        </th>
                      </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                      {selectedFinancials.map(({ candidate, financial }) => (
                        <tr key={candidate!.candidate_id} className="hover:bg-gray-50">
                          <td className="px-4 py-3 whitespace-nowrap">
                            <div className="text-sm font-medium text-gray-900">{candidate!.name}</div>
                            <div className="text-xs text-gray-500">{candidate!.party || 'N/A'}</div>
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-900">
                            ${(financial!.total_receipts / 1000).toFixed(1)}K
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-900">
                            ${(financial!.total_disbursements / 1000).toFixed(1)}K
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-900">
                            ${(financial!.cash_on_hand / 1000).toFixed(1)}K
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-900">
                            ${(financial!.individual_contributions / 1000).toFixed(1)}K
                          </td>
                          <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-900">
                            ${(financial!.pac_contributions / 1000).toFixed(1)}K
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

              {/* Comparison Charts */}
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                <div className="bg-white rounded-lg shadow p-6">
                  <h3 className="text-lg font-semibold mb-4">Total Receipts Comparison</h3>
                  <Bar
                    data={{
                      labels: selectedFinancials.map(({ candidate }) => candidate!.name),
                      datasets: [
                        {
                          label: 'Total Receipts',
                          data: selectedFinancials.map(({ financial }) => financial!.total_receipts),
                          backgroundColor: 'rgba(59, 130, 246, 0.5)',
                          borderColor: 'rgba(59, 130, 246, 1)',
                          borderWidth: 1,
                        },
                      ],
                    }}
                    options={{
                      responsive: true,
                      maintainAspectRatio: true,
                      plugins: {
                        tooltip: {
                          callbacks: {
                            label: (context) => `$${((context.parsed.y || 0) / 1000).toFixed(1)}K`,
                          },
                        },
                      },
                      scales: {
                        y: {
                          ticks: {
                            callback: (value) => `$${(Number(value) / 1000).toFixed(0)}K`,
                          },
                        },
                      },
                    }}
                  />
                </div>

                <div className="bg-white rounded-lg shadow p-6">
                  <h3 className="text-lg font-semibold mb-4">Cash on Hand Comparison</h3>
                  <Bar
                    data={{
                      labels: selectedFinancials.map(({ candidate }) => candidate!.name),
                      datasets: [
                        {
                          label: 'Cash on Hand',
                          data: selectedFinancials.map(({ financial }) => financial!.cash_on_hand),
                          backgroundColor: 'rgba(16, 185, 129, 0.5)',
                          borderColor: 'rgba(16, 185, 129, 1)',
                          borderWidth: 1,
                        },
                      ],
                    }}
                    options={{
                      responsive: true,
                      maintainAspectRatio: true,
                      plugins: {
                        tooltip: {
                          callbacks: {
                            label: (context) => `$${((context.parsed.y || 0) / 1000).toFixed(1)}K`,
                          },
                        },
                      },
                      scales: {
                        y: {
                          ticks: {
                            callback: (value) => `$${(Number(value) / 1000).toFixed(0)}K`,
                          },
                        },
                      },
                    }}
                  />
                </div>

                <div className="bg-white rounded-lg shadow p-6">
                  <h3 className="text-lg font-semibold mb-4">Individual vs PAC Contributions</h3>
                  <Bar
                    data={{
                      labels: selectedFinancials.map(({ candidate }) => candidate!.name),
                      datasets: [
                        {
                          label: 'Individual Contributions',
                          data: selectedFinancials.map(({ financial }) => financial!.individual_contributions),
                          backgroundColor: 'rgba(59, 130, 246, 0.5)',
                          borderColor: 'rgba(59, 130, 246, 1)',
                          borderWidth: 1,
                        },
                        {
                          label: 'PAC Contributions',
                          data: selectedFinancials.map(({ financial }) => financial!.pac_contributions),
                          backgroundColor: 'rgba(245, 158, 11, 0.5)',
                          borderColor: 'rgba(245, 158, 11, 1)',
                          borderWidth: 1,
                        },
                      ],
                    }}
                    options={{
                      responsive: true,
                      maintainAspectRatio: true,
                      plugins: {
                        tooltip: {
                          callbacks: {
                            label: (context) => `$${((context.parsed.y || 0) / 1000).toFixed(1)}K`,
                          },
                        },
                      },
                      scales: {
                        y: {
                          ticks: {
                            callback: (value) => `$${(Number(value) / 1000).toFixed(0)}K`,
                          },
                        },
                      },
                    }}
                  />
                </div>

                <div className="bg-white rounded-lg shadow p-6">
                  <h3 className="text-lg font-semibold mb-4">Total Disbursements Comparison</h3>
                  <Bar
                    data={{
                      labels: selectedFinancials.map(({ candidate }) => candidate!.name),
                      datasets: [
                        {
                          label: 'Total Disbursements',
                          data: selectedFinancials.map(({ financial }) => financial!.total_disbursements),
                          backgroundColor: 'rgba(239, 68, 68, 0.5)',
                          borderColor: 'rgba(239, 68, 68, 1)',
                          borderWidth: 1,
                        },
                      ],
                    }}
                    options={{
                      responsive: true,
                      maintainAspectRatio: true,
                      plugins: {
                        tooltip: {
                          callbacks: {
                            label: (context) => `$${((context.parsed.y || 0) / 1000).toFixed(1)}K`,
                          },
                        },
                      },
                      scales: {
                        y: {
                          ticks: {
                            callback: (value) => `$${(Number(value) / 1000).toFixed(0)}K`,
                          },
                        },
                      },
                    }}
                  />
                </div>
              </div>
            </div>
          )}
        </div>
      )}

      {!loading && candidates.length === 0 && !error && (
        <div className="bg-white rounded-lg shadow p-6 text-center text-gray-600">
          <p>No candidates found for this race. Try adjusting your search criteria.</p>
        </div>
      )}
    </div>
  );
}

