import { useEffect, useState } from 'react';
import { analysisApi, EmployerAnalysis } from '../services/api';
import { Bar, Pie } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  ArcElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js';

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  ArcElement,
  Title,
  Tooltip,
  Legend
);

interface EmployerTreemapProps {
  candidateId?: string;
  committeeId?: string;
  minDate?: string;
  maxDate?: string;
}

export default function EmployerTreemap({
  candidateId,
  committeeId,
  minDate,
  maxDate,
}: EmployerTreemapProps) {
  const [analysis, setAnalysis] = useState<EmployerAnalysis | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchAnalysis = async () => {
      setLoading(true);
      setError(null);
      try {
        const data = await analysisApi.getEmployerBreakdown({
          candidate_id: candidateId,
          committee_id: committeeId,
          min_date: minDate,
          max_date: maxDate,
        });
        setAnalysis(data);
      } catch (err) {
        setError('Failed to load employer analysis');
        console.error(err);
      } finally {
        setLoading(false);
      }
    };

    if (candidateId || committeeId) {
      fetchAnalysis();
    }
  }, [candidateId, committeeId, minDate, maxDate]);

  if (loading) {
    return (
      <div className="bg-white rounded-lg shadow p-6">
        <div className="animate-pulse">
          <div className="h-4 bg-gray-200 rounded w-3/4 mb-4"></div>
          <div className="h-64 bg-gray-200 rounded"></div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-white rounded-lg shadow p-6">
        <div className="text-red-600">{error}</div>
      </div>
    );
  }

  if (!analysis) {
    return null;
  }

  if (analysis.top_employers.length === 0) {
    return (
      <div className="bg-white rounded-lg shadow p-6">
        <h2 className="text-xl font-semibold mb-4">Employer/Industry Analysis</h2>
        <p className="text-gray-600">No employer data available for contributions</p>
      </div>
    );
  }

  // Prepare chart data
  const topEmployers = analysis.top_employers.slice(0, 20);
  const employerLabels = topEmployers.map((e) => e.employer.length > 40 ? e.employer.substring(0, 40) + '...' : e.employer);
  const employerAmounts = topEmployers.map((e) => e.total);
  const employerCounts = topEmployers.map((e) => e.count);

  const top10Employers = analysis.top_employers.slice(0, 10);
  const pieLabels = top10Employers.map((e) => e.employer.length > 30 ? e.employer.substring(0, 30) + '...' : e.employer);
  const pieData = top10Employers.map((e) => e.total);

  const barData = {
    labels: employerLabels,
    datasets: [
      {
        label: 'Total Contributions',
        data: employerAmounts,
        backgroundColor: 'rgba(59, 130, 246, 0.5)',
        borderColor: 'rgba(59, 130, 246, 1)',
        borderWidth: 1,
      },
    ],
  };

  const pieChartData = {
    labels: pieLabels,
    datasets: [
      {
        data: pieData,
        backgroundColor: [
          'rgba(239, 68, 68, 0.5)',
          'rgba(245, 158, 11, 0.5)',
          'rgba(251, 191, 36, 0.5)',
          'rgba(34, 197, 94, 0.5)',
          'rgba(59, 130, 246, 0.5)',
          'rgba(139, 92, 246, 0.5)',
          'rgba(236, 72, 153, 0.5)',
          'rgba(20, 184, 166, 0.5)',
          'rgba(168, 85, 247, 0.5)',
          'rgba(251, 146, 60, 0.5)',
        ],
        borderWidth: 1,
      },
    ],
  };

  const totalFromEmployers = analysis.top_employers.reduce((sum, e) => sum + e.total, 0);
  const percentageWithEmployer = analysis.total_contributions > 0
    ? (totalFromEmployers / analysis.total_contributions) * 100
    : 0;

  return (
    <div className="space-y-6">
      <div className="bg-white rounded-lg shadow p-6">
        <h2 className="text-xl font-semibold mb-4">Employer/Industry Analysis</h2>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
          <div>
            <div className="text-sm text-gray-600">Unique Employers</div>
            <div className="text-2xl font-bold">{analysis.employer_count.toLocaleString()}</div>
          </div>
          <div>
            <div className="text-sm text-gray-600">Total from Employers</div>
            <div className="text-2xl font-bold">${(totalFromEmployers / 1000).toFixed(1)}K</div>
          </div>
          <div>
            <div className="text-sm text-gray-600">% with Employer Data</div>
            <div className="text-2xl font-bold">{percentageWithEmployer.toFixed(1)}%</div>
          </div>
          <div>
            <div className="text-sm text-gray-600">Total Contributions</div>
            <div className="text-2xl font-bold">${(analysis.total_contributions / 1000).toFixed(1)}K</div>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="bg-white rounded-lg shadow p-6">
          <h3 className="text-lg font-semibold mb-4">Top 10 Employers (Pie Chart)</h3>
          <Pie data={pieChartData} options={{ responsive: true, maintainAspectRatio: true }} />
        </div>

        <div className="bg-white rounded-lg shadow p-6">
          <h3 className="text-lg font-semibold mb-4">Top 20 Employers (Bar Chart)</h3>
          <Bar
            data={barData}
            options={{
              responsive: true,
              maintainAspectRatio: true,
              indexAxis: 'y',
              plugins: {
                tooltip: {
                  callbacks: {
                    label: (context) => {
                      const index = context.dataIndex;
                      const employer = topEmployers[index];
                      return [
                        `Total: $${employer.total.toLocaleString()}`,
                        `Contributions: ${employer.count}`,
                        `Average: $${(employer.total / employer.count).toFixed(2)}`,
                      ];
                    },
                  },
                },
              },
            }}
          />
        </div>
      </div>

      <div className="bg-white rounded-lg shadow p-6">
        <h3 className="text-lg font-semibold mb-4">Top Employers Table</h3>
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Rank
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Employer
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Total Contributions
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  # of Contributions
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Average
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {analysis.top_employers.slice(0, 30).map((employer, idx) => (
                <tr key={idx} className="hover:bg-gray-50">
                  <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                    {idx + 1}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    {employer.employer}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                    ${employer.total.toLocaleString()}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {employer.count}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    ${(employer.total / employer.count).toFixed(2)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

