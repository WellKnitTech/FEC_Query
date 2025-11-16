import { useState, useEffect } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { trendApi, candidateApi, TrendAnalysis as TrendAnalysisType, Candidate } from '../services/api';
import { Line } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js';

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Title, Tooltip, Legend);

export default function TrendAnalysis() {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const [candidateId, setCandidateId] = useState(searchParams.get('candidateId') || '');
  const [candidate, setCandidate] = useState<Candidate | null>(null);
  const [trends, setTrends] = useState<TrendAnalysisType | null>(null);
  const [minCycle, setMinCycle] = useState<number | undefined>(undefined);
  const [maxCycle, setMaxCycle] = useState<number | undefined>(undefined);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [comparisonMode, setComparisonMode] = useState(false);
  const [comparisonCandidateIds, setComparisonCandidateIds] = useState<string[]>([]);
  const [raceTrends, setRaceTrends] = useState<any>(null);

  useEffect(() => {
    // Auto-search if candidateId is in URL
    if (candidateId) {
      const abortController = new AbortController();
      
      const handleSearch = async () => {
        if (!candidateId.trim()) return;

        setLoading(true);
        setError(null);

        try {
          const [candidateData, trendsData] = await Promise.all([
            candidateApi.getById(candidateId, abortController.signal),
            trendApi.getCandidateTrends(candidateId, minCycle, maxCycle, abortController.signal),
          ]);

          if (!abortController.signal.aborted) {
            setCandidate(candidateData);
            setTrends(trendsData);
          }
        } catch (err: any) {
          // Don't set error if request was aborted
          if (err.name === 'AbortError' || abortController.signal.aborted) {
            return;
          }
          if (!abortController.signal.aborted) {
            setError(err?.response?.data?.detail || err?.message || 'Failed to load trends');
          }
        } finally {
          if (!abortController.signal.aborted) {
            setLoading(false);
          }
        }
      };
      
      handleSearch();
      
      return () => {
        abortController.abort();
      };
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const handleSearch = async () => {
    if (!candidateId.trim()) return;

    setLoading(true);
    setError(null);

    try {
      const [candidateData, trendsData] = await Promise.all([
        candidateApi.getById(candidateId),
        trendApi.getCandidateTrends(candidateId, minCycle, maxCycle),
      ]);

      setCandidate(candidateData);
      setTrends(trendsData);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to load trends');
    } finally {
      setLoading(false);
    }
  };

  const handleCompare = async () => {
    if (comparisonCandidateIds.length === 0) return;

    setLoading(true);
    setError(null);

    try {
      const allIds = candidateId ? [candidateId, ...comparisonCandidateIds] : comparisonCandidateIds;
      const raceData = await trendApi.getRaceTrends(allIds, minCycle, maxCycle);
      setRaceTrends(raceData);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to load race trends');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <h1 className="text-3xl font-bold text-gray-900 mb-6">Historical Trend Analysis</h1>

      <div className="bg-white rounded-lg shadow p-6 mb-6">
        <h2 className="text-xl font-semibold mb-4">Search</h2>
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Candidate ID</label>
            <input
              type="text"
              value={candidateId}
              onChange={(e) => setCandidateId(e.target.value)}
              placeholder="P00003392"
              className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Min Cycle</label>
            <input
              type="number"
              value={minCycle || ''}
              onChange={(e) => setMinCycle(e.target.value ? parseInt(e.target.value) : undefined)}
              placeholder="2020"
              className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Max Cycle</label>
            <input
              type="number"
              value={maxCycle || ''}
              onChange={(e) => setMaxCycle(e.target.value ? parseInt(e.target.value) : undefined)}
              placeholder="2024"
              className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"
            />
          </div>
          <div className="flex items-end">
            <button
              onClick={handleSearch}
              disabled={loading || !candidateId}
              className="w-full px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50"
            >
              {loading ? 'Loading...' : 'Analyze Trends'}
            </button>
          </div>
        </div>

        <div className="mt-4 flex items-center">
          <input
            type="checkbox"
            id="comparison"
            checked={comparisonMode}
            onChange={(e) => setComparisonMode(e.target.checked)}
            className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
          />
          <label htmlFor="comparison" className="ml-2 text-sm text-gray-700">
            Comparison Mode
          </label>
        </div>
      </div>

      {error && (
        <div className="mb-4 p-4 bg-red-100 border border-red-400 text-red-700 rounded">
          {error}
        </div>
      )}

      {trends && trends.trends.length > 0 && (
        <div className="space-y-6">
          {candidate && (
            <div className="bg-white rounded-lg shadow p-6">
              <h2 className="text-xl font-semibold mb-2">{candidate.name}</h2>
              <p className="text-gray-600">
                {candidate.office} • {candidate.state} • {candidate.party || 'N/A'}
              </p>
            </div>
          )}

          <div className="bg-white rounded-lg shadow p-6">
            <h3 className="text-lg font-semibold mb-4">Financial Trends Over Time</h3>
            <Line
              data={{
                labels: trends.trends.map((t) => t.cycle.toString()),
                datasets: [
                  {
                    label: 'Total Receipts',
                    data: trends.trends.map((t) => t.total_receipts),
                    borderColor: 'rgb(59, 130, 246)',
                    backgroundColor: 'rgba(59, 130, 246, 0.1)',
                  },
                  {
                    label: 'Total Disbursements',
                    data: trends.trends.map((t) => t.total_disbursements),
                    borderColor: 'rgb(239, 68, 68)',
                    backgroundColor: 'rgba(239, 68, 68, 0.1)',
                  },
                  {
                    label: 'Cash on Hand',
                    data: trends.trends.map((t) => t.cash_on_hand),
                    borderColor: 'rgb(16, 185, 129)',
                    backgroundColor: 'rgba(16, 185, 129, 0.1)',
                  },
                ],
              }}
              options={{
                responsive: true,
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
            <h3 className="text-lg font-semibold mb-4">Contribution Trends</h3>
            <Line
              data={{
                labels: trends.trends.map((t) => t.cycle.toString()),
                datasets: [
                  {
                    label: 'Total Contributions',
                    data: trends.trends.map((t) => t.total_contributions),
                    borderColor: 'rgb(59, 130, 246)',
                    backgroundColor: 'rgba(59, 130, 246, 0.1)',
                  },
                  {
                    label: 'Individual Contributions',
                    data: trends.trends.map((t) => t.individual_contributions),
                    borderColor: 'rgb(16, 185, 129)',
                    backgroundColor: 'rgba(16, 185, 129, 0.1)',
                  },
                  {
                    label: 'PAC Contributions',
                    data: trends.trends.map((t) => t.pac_contributions),
                    borderColor: 'rgb(245, 158, 11)',
                    backgroundColor: 'rgba(245, 158, 11, 0.1)',
                  },
                ],
              }}
              options={{
                responsive: true,
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
            <h3 className="text-lg font-semibold mb-4">Trend Data</h3>
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Cycle</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Receipts</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Disbursements</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Cash on Hand</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Growth</th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {trends.trends.map((trend) => (
                    <tr key={trend.cycle}>
                      <td className="px-6 py-4 whitespace-nowrap text-sm">{trend.cycle}</td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm">${(trend.total_receipts / 1000).toFixed(1)}K</td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm">${(trend.total_disbursements / 1000).toFixed(1)}K</td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm">${(trend.cash_on_hand / 1000).toFixed(1)}K</td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm">
                        {trend.receipts_growth !== undefined && trend.receipts_growth !== null ? (
                          <span className={trend.receipts_growth >= 0 ? 'text-green-600' : 'text-red-600'}>
                            {trend.receipts_growth >= 0 ? '+' : ''}{trend.receipts_growth.toFixed(1)}%
                          </span>
                        ) : (
                          'N/A'
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

