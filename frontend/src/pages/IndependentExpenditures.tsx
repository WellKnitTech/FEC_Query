import { useState, useEffect } from 'react';
import { independentExpenditureApi, IndependentExpenditure, IndependentExpenditureAnalysis } from '../services/api';
import SaveSearchButton from '../components/SaveSearchButton';
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

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, BarElement, Title, Tooltip, Legend);

export default function IndependentExpenditures() {
  const [candidateId, setCandidateId] = useState('');
  const [committeeId, setCommitteeId] = useState('');
  const [supportOppose, setSupportOppose] = useState<string>('');
  const [minDate, setMinDate] = useState('');
  const [maxDate, setMaxDate] = useState('');
  const [expenditures, setExpenditures] = useState<IndependentExpenditure[]>([]);
  const [analysis, setAnalysis] = useState<IndependentExpenditureAnalysis | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSearch = async () => {
    setLoading(true);
    setError(null);

    try {
      const [expData, analysisData] = await Promise.all([
        independentExpenditureApi.get({
          candidate_id: candidateId || undefined,
          committee_id: committeeId || undefined,
          support_oppose: supportOppose || undefined,
          min_date: minDate || undefined,
          max_date: maxDate || undefined,
          limit: 1000,
        }),
        independentExpenditureApi.analyze({
          candidate_id: candidateId || undefined,
          committee_id: committeeId || undefined,
          min_date: minDate || undefined,
          max_date: maxDate || undefined,
        }),
      ]);

      setExpenditures(expData);
      setAnalysis(analysisData);
    } catch (err: any) {
      setError(err?.response?.data?.detail || err?.message || 'Failed to load independent expenditures');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    // Auto-search on mount with empty filters to show recent data
    handleSearch();
  }, []);

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <h1 className="text-3xl font-bold text-gray-900 mb-6">Independent Expenditures Analysis</h1>

      <div className="bg-white rounded-lg shadow p-6 mb-6">
        <div className="flex justify-between items-center mb-4">
          <h2 className="text-xl font-semibold">Search & Filters</h2>
          {(candidateId || committeeId) && (
            <SaveSearchButton
              searchType="independent_expenditure"
              searchParams={{
                candidate_id: candidateId || undefined,
                committee_id: committeeId || undefined,
                support_oppose: supportOppose || undefined,
                min_date: minDate || undefined,
                max_date: maxDate || undefined,
              }}
            />
          )}
        </div>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
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
            <label className="block text-sm font-medium text-gray-700 mb-2">Committee ID</label>
            <input
              type="text"
              value={committeeId}
              onChange={(e) => setCommitteeId(e.target.value)}
              placeholder="C00000042"
              className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Support/Oppose</label>
            <select
              value={supportOppose}
              onChange={(e) => setSupportOppose(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"
            >
              <option value="">All</option>
              <option value="S">Support</option>
              <option value="O">Oppose</option>
            </select>
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Start Date</label>
            <input
              type="date"
              value={minDate}
              onChange={(e) => setMinDate(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">End Date</label>
            <input
              type="date"
              value={maxDate}
              onChange={(e) => setMaxDate(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500"
            />
          </div>
        </div>
        <button
          onClick={handleSearch}
          disabled={loading}
          className="mt-4 px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50"
        >
          {loading ? 'Loading...' : 'Search'}
        </button>
      </div>

      {error && (
        <div className="mb-4 p-4 bg-red-100 border border-red-400 text-red-700 rounded">
          {error}
        </div>
      )}

      {analysis && (
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
          <div className="bg-white rounded-lg shadow p-6">
            <div className="text-sm text-gray-600">Total Expenditures</div>
            <div className="text-2xl font-bold">${(analysis.total_expenditures / 1000000).toFixed(2)}M</div>
          </div>
          <div className="bg-white rounded-lg shadow p-6">
            <div className="text-sm text-gray-600">Support</div>
            <div className="text-2xl font-bold text-green-600">${(analysis.total_support / 1000000).toFixed(2)}M</div>
          </div>
          <div className="bg-white rounded-lg shadow p-6">
            <div className="text-sm text-gray-600">Oppose</div>
            <div className="text-2xl font-bold text-red-600">${(analysis.total_oppose / 1000000).toFixed(2)}M</div>
          </div>
          <div className="bg-white rounded-lg shadow p-6">
            <div className="text-sm text-gray-600">Total Transactions</div>
            <div className="text-2xl font-bold">{analysis.total_transactions.toLocaleString()}</div>
          </div>
        </div>
      )}

      {analysis && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
          <div className="bg-white rounded-lg shadow p-6">
            <h3 className="text-lg font-semibold mb-4">Support vs Oppose Over Time</h3>
            {Object.keys(analysis.expenditures_by_date).length > 0 ? (
              <Line
                data={{
                  labels: Object.keys(analysis.expenditures_by_date).sort(),
                  datasets: [
                    {
                      label: 'Total',
                      data: Object.keys(analysis.expenditures_by_date)
                        .sort()
                        .map((date) => analysis.expenditures_by_date[date]),
                      borderColor: 'rgb(59, 130, 246)',
                      backgroundColor: 'rgba(59, 130, 246, 0.1)',
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
            ) : (
              <p className="text-gray-500">No data available</p>
            )}
          </div>

          <div className="bg-white rounded-lg shadow p-6">
            <h3 className="text-lg font-semibold mb-4">Top Committees</h3>
            {analysis.top_committees.length > 0 ? (
              <Bar
                data={{
                  labels: analysis.top_committees.slice(0, 10).map((c) => c.committee_id),
                  datasets: [
                    {
                      label: 'Amount',
                      data: analysis.top_committees.slice(0, 10).map((c) => c.total_amount),
                      backgroundColor: 'rgba(16, 185, 129, 0.5)',
                      borderColor: 'rgba(16, 185, 129, 1)',
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
            ) : (
              <p className="text-gray-500">No data available</p>
            )}
          </div>
        </div>
      )}

      {expenditures.length > 0 && (
        <div className="bg-white rounded-lg shadow">
          <div className="px-6 py-4 border-b border-gray-200">
            <h2 className="text-lg font-semibold">Expenditures ({expenditures.length})</h2>
          </div>
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Date</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Amount</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Support/Oppose</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Candidate</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Committee</th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">Payee</th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {expenditures.map((exp, idx) => (
                  <tr key={idx} className="hover:bg-gray-50">
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                      {exp.expenditure_date || 'N/A'}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                      ${exp.expenditure_amount.toLocaleString()}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm">
                      {exp.support_oppose_indicator === 'S' ? (
                        <span className="text-green-600 font-semibold">Support</span>
                      ) : exp.support_oppose_indicator === 'O' ? (
                        <span className="text-red-600 font-semibold">Oppose</span>
                      ) : (
                        'N/A'
                      )}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {exp.candidate_name || exp.candidate_id || 'N/A'}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {exp.committee_id || 'N/A'}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                      {exp.payee_name || 'N/A'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}

