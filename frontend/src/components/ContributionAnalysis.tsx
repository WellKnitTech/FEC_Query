import { useEffect, useState } from 'react';
import { contributionApi, ContributionAnalysis as ContributionAnalysisType } from '../services/api';
import { Line, Bar, Doughnut } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  ArcElement,
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
  ArcElement,
  Title,
  Tooltip,
  Legend
);

interface ContributionAnalysisProps {
  candidateId?: string;
  committeeId?: string;
  minDate?: string;
  maxDate?: string;
}

export default function ContributionAnalysis({
  candidateId,
  committeeId,
  minDate,
  maxDate,
}: ContributionAnalysisProps) {
  const [analysis, setAnalysis] = useState<ContributionAnalysisType | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchAnalysis = async () => {
      setLoading(true);
      setError(null);
      try {
        const data = await contributionApi.analyze({
          candidate_id: candidateId,
          committee_id: committeeId,
          min_date: minDate,
          max_date: maxDate,
        });
        setAnalysis(data);
      } catch (err) {
        setError('Failed to load contribution analysis');
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

  // Prepare chart data
  const dateLabels = Object.keys(analysis.contributions_by_date).sort();
  const dateData = dateLabels.map((date) => analysis.contributions_by_date[date]);

  const stateLabels = Object.keys(analysis.contributions_by_state);
  const stateData = stateLabels.map((state) => analysis.contributions_by_state[state]);

  const distributionLabels = Object.keys(analysis.contribution_distribution);
  const distributionData = Object.values(analysis.contribution_distribution);

  const topDonorsData = {
    labels: analysis.top_donors.slice(0, 10).map((d) => d.name),
    datasets: [
      {
        label: 'Total Contributions',
        data: analysis.top_donors.slice(0, 10).map((d) => d.total),
        backgroundColor: 'rgba(59, 130, 246, 0.5)',
        borderColor: 'rgba(59, 130, 246, 1)',
        borderWidth: 1,
      },
    ],
  };

  const dateChartData = {
    labels: dateLabels,
    datasets: [
      {
        label: 'Contributions by Date',
        data: dateData,
        borderColor: 'rgba(59, 130, 246, 1)',
        backgroundColor: 'rgba(59, 130, 246, 0.1)',
        tension: 0.1,
      },
    ],
  };

  const stateChartData = {
    labels: stateLabels.slice(0, 10),
    datasets: [
      {
        label: 'Contributions by State',
        data: stateData.slice(0, 10),
        backgroundColor: 'rgba(16, 185, 129, 0.5)',
        borderColor: 'rgba(16, 185, 129, 1)',
        borderWidth: 1,
      },
    ],
  };

  const distributionChartData = {
    labels: distributionLabels,
    datasets: [
      {
        data: distributionData,
        backgroundColor: [
          'rgba(239, 68, 68, 0.5)',
          'rgba(245, 158, 11, 0.5)',
          'rgba(251, 191, 36, 0.5)',
          'rgba(34, 197, 94, 0.5)',
          'rgba(59, 130, 246, 0.5)',
          'rgba(139, 92, 246, 0.5)',
          'rgba(236, 72, 153, 0.5)',
        ],
        borderWidth: 1,
      },
    ],
  };

  return (
    <div className="space-y-6">
      <div className="bg-white rounded-lg shadow p-6">
        <h2 className="text-xl font-semibold mb-4">Contribution Analysis</h2>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
          <div>
            <div className="text-sm text-gray-600">Total Contributions</div>
            <div className="text-2xl font-bold">
              ${(analysis.total_contributions / 1000).toFixed(1)}K
            </div>
          </div>
          <div>
            <div className="text-sm text-gray-600">Total Contributors</div>
            <div className="text-2xl font-bold">{analysis.total_contributors.toLocaleString()}</div>
          </div>
          <div>
            <div className="text-sm text-gray-600">Average Contribution</div>
            <div className="text-2xl font-bold">${analysis.average_contribution.toFixed(2)}</div>
          </div>
          <div>
            <div className="text-sm text-gray-600">Top Donors</div>
            <div className="text-2xl font-bold">{analysis.top_donors.length}</div>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="bg-white rounded-lg shadow p-6">
          <h3 className="text-lg font-semibold mb-4">Contributions Over Time</h3>
          <Line data={dateChartData} options={{ responsive: true, maintainAspectRatio: true }} />
        </div>

        <div className="bg-white rounded-lg shadow p-6">
          <h3 className="text-lg font-semibold mb-4">Top States</h3>
          <Bar data={stateChartData} options={{ responsive: true, maintainAspectRatio: true }} />
        </div>

        <div className="bg-white rounded-lg shadow p-6">
          <h3 className="text-lg font-semibold mb-4">Top Donors</h3>
          <Bar data={topDonorsData} options={{ responsive: true, maintainAspectRatio: true, indexAxis: 'y' }} />
        </div>

        <div className="bg-white rounded-lg shadow p-6">
          <h3 className="text-lg font-semibold mb-4">Contribution Distribution</h3>
          <Doughnut data={distributionChartData} options={{ responsive: true, maintainAspectRatio: true }} />
        </div>
      </div>
    </div>
  );
}

