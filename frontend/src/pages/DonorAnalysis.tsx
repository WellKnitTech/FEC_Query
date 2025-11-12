import { useState, useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import { contributionApi, Contribution, exportApi } from '../services/api';
import { Line, Bar } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Title,
  Tooltip,
  Legend
);

export default function DonorAnalysis() {
  const navigate = useNavigate();
  const [contributorName, setContributorName] = useState('');
  const [contributions, setContributions] = useState<Contribution[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSearch = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!contributorName.trim()) return;

    setLoading(true);
    setError(null);
    try {
      const data = await contributionApi.get({
        contributor_name: contributorName,
        limit: 1000,
      });
      setContributions(data);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to search contributions');
    } finally {
      setLoading(false);
    }
  };

  const handleExport = async (format: 'csv' | 'excel') => {
    try {
      await exportApi.exportContributions(format, {
        contributor_name: contributorName,
      });
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to export contributions');
    }
  };

  // Calculate summary statistics
  const totalAmount = contributions.reduce((sum, c) => sum + c.contribution_amount, 0);
  const uniqueCandidates = new Set(contributions.map((c) => c.candidate_id).filter(Boolean)).size;
  const uniqueCommittees = new Set(contributions.map((c) => c.committee_id).filter(Boolean)).size;
  const averageContribution = contributions.length > 0 ? totalAmount / contributions.length : 0;

  // Process data for visualizations
  const chartData = useMemo(() => {
    if (contributions.length === 0) return null;

    // Group by month
    const byMonth: Record<string, number> = {};
    contributions.forEach((c) => {
      if (c.contribution_date) {
        const date = new Date(c.contribution_date);
        const monthKey = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
        byMonth[monthKey] = (byMonth[monthKey] || 0) + c.contribution_amount;
      }
    });

    const sortedMonths = Object.keys(byMonth).sort();
    return {
      labels: sortedMonths,
      datasets: [
        {
          label: 'Contributions by Month',
          data: sortedMonths.map((m) => byMonth[m]),
          borderColor: 'rgb(59, 130, 246)',
          backgroundColor: 'rgba(59, 130, 246, 0.1)',
          tension: 0.1,
        },
      ],
    };
  }, [contributions]);

  // Geographic breakdown
  const contributionsByState = useMemo(() => {
    const byState: Record<string, { amount: number; count: number }> = {};
    contributions.forEach((c) => {
      if (c.contributor_state) {
        const state = c.contributor_state;
        if (!byState[state]) {
          byState[state] = { amount: 0, count: 0 };
        }
        byState[state].amount += c.contribution_amount;
        byState[state].count += 1;
      }
    });
    return byState;
  }, [contributions]);

  // Top candidates
  const topCandidates = useMemo(() => {
    const byCandidate: Record<string, { amount: number; count: number; candidateId: string }> = {};
    contributions.forEach((c) => {
      if (c.candidate_id) {
        if (!byCandidate[c.candidate_id]) {
          byCandidate[c.candidate_id] = { amount: 0, count: 0, candidateId: c.candidate_id };
        }
        byCandidate[c.candidate_id].amount += c.contribution_amount;
        byCandidate[c.candidate_id].count += 1;
      }
    });
    return Object.values(byCandidate)
      .sort((a, b) => b.amount - a.amount)
      .slice(0, 10);
  }, [contributions]);

  // Top committees
  const topCommittees = useMemo(() => {
    const byCommittee: Record<string, { amount: number; count: number; committeeId: string }> = {};
    contributions.forEach((c) => {
      if (c.committee_id) {
        if (!byCommittee[c.committee_id]) {
          byCommittee[c.committee_id] = { amount: 0, count: 0, committeeId: c.committee_id };
        }
        byCommittee[c.committee_id].amount += c.contribution_amount;
        byCommittee[c.committee_id].count += 1;
      }
    });
    return Object.values(byCommittee)
      .sort((a, b) => b.amount - a.amount)
      .slice(0, 10);
  }, [contributions]);

  // Contribution frequency analysis
  const contributionFrequency = useMemo(() => {
    if (contributions.length === 0) return null;
    const dates = contributions
      .map((c) => c.contribution_date)
      .filter(Boolean)
      .map((d) => new Date(d!));
    if (dates.length === 0) return null;

    const firstDate = new Date(Math.min(...dates.map((d) => d.getTime())));
    const lastDate = new Date(Math.max(...dates.map((d) => d.getTime())));
    const daysDiff = Math.ceil((lastDate.getTime() - firstDate.getTime()) / (1000 * 60 * 60 * 24));
    return daysDiff > 0 ? contributions.length / daysDiff : 0;
  }, [contributions]);

  // Amount distribution (histogram data)
  const amountDistribution = useMemo(() => {
    const ranges = [
      { label: '$0-$100', min: 0, max: 100, count: 0 },
      { label: '$100-$500', min: 100, max: 500, count: 0 },
      { label: '$500-$1,000', min: 500, max: 1000, count: 0 },
      { label: '$1,000-$5,000', min: 1000, max: 5000, count: 0 },
      { label: '$5,000+', min: 5000, max: Infinity, count: 0 },
    ];

    contributions.forEach((c) => {
      const amount = c.contribution_amount;
      for (const range of ranges) {
        if (amount >= range.min && amount < range.max) {
          range.count++;
          break;
        }
      }
    });

    return {
      labels: ranges.map((r) => r.label),
      datasets: [
        {
          label: 'Number of Contributions',
          data: ranges.map((r) => r.count),
          backgroundColor: 'rgba(59, 130, 246, 0.6)',
        },
      ],
    };
  }, [contributions]);

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <h1 className="text-3xl font-bold text-gray-900 mb-6">Donor Analysis</h1>

      <form onSubmit={handleSearch} className="mb-6">
        <div className="flex gap-2">
          <input
            type="text"
            value={contributorName}
            onChange={(e) => setContributorName(e.target.value)}
            placeholder="Enter contributor name..."
            className="flex-1 px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          />
          <button
            type="submit"
            disabled={loading}
            className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50"
          >
            {loading ? 'Searching...' : 'Search'}
          </button>
          {contributions.length > 0 && (
            <>
              <button
                type="button"
                onClick={() => handleExport('csv')}
                className="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700"
              >
                Export CSV
              </button>
              <button
                type="button"
                onClick={() => handleExport('excel')}
                className="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700"
              >
                Export Excel
              </button>
            </>
          )}
        </div>
      </form>

      {error && (
        <div className="mb-4 p-4 bg-red-100 border border-red-400 text-red-700 rounded">
          {error}
        </div>
      )}

      {contributions.length > 0 && (
        <div className="space-y-6">
          <div className="bg-white rounded-lg shadow p-6">
            <h2 className="text-xl font-semibold mb-4">Summary</h2>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
              <div>
                <div className="text-sm text-gray-600">Total Contributions</div>
                <div className="text-2xl font-bold">${totalAmount.toLocaleString()}</div>
              </div>
              <div>
                <div className="text-sm text-gray-600">Number of Contributions</div>
                <div className="text-2xl font-bold">{contributions.length}</div>
              </div>
              <div>
                <div className="text-sm text-gray-600">Candidates Supported</div>
                <div className="text-2xl font-bold">{uniqueCandidates}</div>
              </div>
              <div>
                <div className="text-sm text-gray-600">Average Contribution</div>
                <div className="text-2xl font-bold">${averageContribution.toFixed(2)}</div>
              </div>
            </div>
          </div>

          {/* Time Series Chart */}
          {chartData && (
            <div className="bg-white rounded-lg shadow p-6">
              <h2 className="text-xl font-semibold mb-4">Contributions Over Time</h2>
              <div className="h-64">
                <Line
                  data={chartData}
                  options={{
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                      legend: { display: true },
                      title: { display: false },
                    },
                    scales: {
                      y: {
                        beginAtZero: true,
                        ticks: {
                          callback: function (value) {
                            return '$' + value.toLocaleString();
                          },
                        },
                      },
                    },
                  }}
                />
              </div>
            </div>
          )}

          {/* Geographic Breakdown */}
          {Object.keys(contributionsByState).length > 0 && (
            <div className="bg-white rounded-lg shadow p-6">
              <h2 className="text-xl font-semibold mb-4">Contributions by State</h2>
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                {Object.entries(contributionsByState)
                  .sort((a, b) => b[1].amount - a[1].amount)
                  .slice(0, 12)
                  .map(([state, data]) => (
                    <div key={state} className="p-4 border border-gray-200 rounded-lg">
                      <div className="font-semibold text-gray-900">{state}</div>
                      <div className="text-2xl font-bold text-blue-600 mt-1">
                        ${data.amount.toLocaleString()}
                      </div>
                      <div className="text-sm text-gray-600 mt-1">{data.count} contributions</div>
                    </div>
                  ))}
              </div>
            </div>
          )}

          {/* Top Candidates and Committees */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            {topCandidates.length > 0 && (
              <div className="bg-white rounded-lg shadow p-6">
                <h2 className="text-xl font-semibold mb-4">Top Candidates Supported</h2>
                <div className="space-y-2">
                  {topCandidates.map((candidate, idx) => (
                    <div
                      key={candidate.candidateId}
                      className="flex justify-between items-center p-3 border border-gray-200 rounded-lg hover:bg-gray-50 cursor-pointer"
                      onClick={() => navigate(`/candidate/${candidate.candidateId}`)}
                    >
                      <div>
                        <div className="font-medium">#{idx + 1} {candidate.candidateId}</div>
                        <div className="text-sm text-gray-600">{candidate.count} contributions</div>
                      </div>
                      <div className="text-lg font-bold text-blue-600">
                        ${candidate.amount.toLocaleString()}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {topCommittees.length > 0 && (
              <div className="bg-white rounded-lg shadow p-6">
                <h2 className="text-xl font-semibold mb-4">Top Committees Supported</h2>
                <div className="space-y-2">
                  {topCommittees.map((committee, idx) => (
                    <div
                      key={committee.committeeId}
                      className="flex justify-between items-center p-3 border border-gray-200 rounded-lg hover:bg-gray-50 cursor-pointer"
                      onClick={() => navigate(`/committee/${committee.committeeId}`)}
                    >
                      <div>
                        <div className="font-medium">#{idx + 1} {committee.committeeId}</div>
                        <div className="text-sm text-gray-600">{committee.count} contributions</div>
                      </div>
                      <div className="text-lg font-bold text-blue-600">
                        ${committee.amount.toLocaleString()}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>

          {/* Contribution Pattern Analysis */}
          <div className="bg-white rounded-lg shadow p-6">
            <h2 className="text-xl font-semibold mb-4">Contribution Patterns</h2>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
              <div>
                <div className="text-sm text-gray-600 mb-2">Contribution Frequency</div>
                <div className="text-2xl font-bold">
                  {contributionFrequency ? contributionFrequency.toFixed(2) : '0'} per day
                </div>
                <div className="text-xs text-gray-500 mt-1">
                  Average contributions per day over period
                </div>
              </div>
              <div>
                <div className="text-sm text-gray-600 mb-2">Average Contribution Size</div>
                <div className="text-2xl font-bold">${averageContribution.toFixed(2)}</div>
                <div className="text-xs text-gray-500 mt-1">Mean contribution amount</div>
              </div>
              <div>
                <div className="text-sm text-gray-600 mb-2">Committees Supported</div>
                <div className="text-2xl font-bold">{uniqueCommittees}</div>
                <div className="text-xs text-gray-500 mt-1">Unique committees</div>
              </div>
            </div>
            {amountDistribution && (
              <div className="mt-6">
                <h3 className="text-lg font-semibold mb-4">Contribution Amount Distribution</h3>
                <div className="h-48">
                  <Bar
                    data={amountDistribution}
                    options={{
                      responsive: true,
                      maintainAspectRatio: false,
                      plugins: {
                        legend: { display: false },
                      },
                      scales: {
                        y: {
                          beginAtZero: true,
                        },
                      },
                    }}
                  />
                </div>
              </div>
            )}
          </div>

          <div className="bg-white rounded-lg shadow">
            <div className="px-6 py-4 border-b border-gray-200">
              <h2 className="text-lg font-semibold">Contributions ({contributions.length})</h2>
            </div>
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Date
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Amount
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Candidate ID
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Committee ID
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Location
                    </th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {contributions.map((contrib, idx) => (
                    <tr key={idx} className="hover:bg-gray-50">
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                        {contrib.contribution_date || 'N/A'}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                        ${contrib.contribution_amount.toLocaleString()}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                        {contrib.candidate_id || 'N/A'}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                        {contrib.committee_id || 'N/A'}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                        {contrib.contributor_city && contrib.contributor_state
                          ? `${contrib.contributor_city}, ${contrib.contributor_state}`
                          : 'N/A'}
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

