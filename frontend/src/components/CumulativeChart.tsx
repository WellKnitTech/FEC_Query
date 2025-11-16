import { useEffect, useState } from 'react';
import { contributionApi, Contribution } from '../services/api';
import { Line } from 'react-chartjs-2';
import { getDateTimestamp } from '../utils/dateUtils';
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

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend
);

interface CumulativeChartProps {
  candidateId?: string;
  committeeId?: string;
  minDate?: string;
  maxDate?: string;
}

export default function CumulativeChart({
  candidateId,
  committeeId,
  minDate,
  maxDate,
}: CumulativeChartProps) {
  const [contributions, setContributions] = useState<Contribution[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchContributions = async () => {
      setLoading(true);
      setError(null);
      try {
        const data = await contributionApi.get({
          candidate_id: candidateId,
          committee_id: committeeId,
          min_date: minDate,
          max_date: maxDate,
          limit: 10000,
        });
        setContributions(data);
      } catch (err: any) {
        const errorMessage = err?.response?.data?.detail || err?.message || 'Failed to load contributions';
        setError(errorMessage);
        console.error('Error loading contributions:', err);
      } finally {
        setLoading(false);
      }
    };

    if (candidateId || committeeId) {
      fetchContributions();
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
        <h2 className="text-xl font-semibold mb-4">Cumulative Contributions</h2>
        <div className="bg-red-50 border border-red-200 rounded-lg p-4">
          <p className="text-red-800">{error}</p>
        </div>
      </div>
    );
  }

  if (contributions.length === 0) {
    return (
      <div className="bg-white rounded-lg shadow p-6">
        <h2 className="text-xl font-semibold mb-4">Cumulative Contributions</h2>
        <p className="text-gray-600">No contribution data available</p>
      </div>
    );
  }

  // Sort contributions by date
  const sortedContributions = [...contributions].sort((a, b) => {
    const dateA = getDateTimestamp(a.contribution_date);
    const dateB = getDateTimestamp(b.contribution_date);
    return dateA - dateB;
  });

  // Calculate cumulative totals
  let cumulativeTotal = 0;
  const cumulativeData: { date: string; total: number }[] = [];

  for (const contrib of sortedContributions) {
    if (contrib.contribution_date) {
      cumulativeTotal += contrib.contribution_amount || 0;
      cumulativeData.push({
        date: contrib.contribution_date,
        total: cumulativeTotal,
      });
    }
  }

  // Group by date to avoid duplicate dates
  const dateMap = new Map<string, number>();
  for (const item of cumulativeData) {
    dateMap.set(item.date, item.total);
  }

  const labels = Array.from(dateMap.keys()).sort();
  const data = labels.map((date) => dateMap.get(date) || 0);

  const chartData = {
    labels: labels,
    datasets: [
      {
        label: 'Cumulative Contributions',
        data: data,
        borderColor: 'rgba(59, 130, 246, 1)',
        backgroundColor: 'rgba(59, 130, 246, 0.1)',
        tension: 0.1,
        fill: true,
      },
    ],
  };

  const totalAmount = cumulativeTotal;
  const firstDate = labels[0] || 'N/A';
  const lastDate = labels[labels.length - 1] || 'N/A';

  return (
    <div className="bg-white rounded-lg shadow p-6">
      <h2 className="text-xl font-semibold mb-4">Cumulative Contributions</h2>
      <div className="grid grid-cols-3 gap-4 mb-6">
        <div>
          <div className="text-sm text-gray-600">Total Amount</div>
          <div className="text-2xl font-bold">${(totalAmount / 1000).toFixed(1)}K</div>
        </div>
        <div>
          <div className="text-sm text-gray-600">Start Date</div>
          <div className="text-lg font-semibold">{firstDate}</div>
        </div>
        <div>
          <div className="text-sm text-gray-600">End Date</div>
          <div className="text-lg font-semibold">{lastDate}</div>
        </div>
      </div>
      <Line
        data={chartData}
        options={{
          responsive: true,
          maintainAspectRatio: true,
          plugins: {
            tooltip: {
              callbacks: {
                label: (context) => {
                  return `Total: $${context.parsed.y.toLocaleString()}`;
                },
              },
            },
          },
          scales: {
            y: {
              beginAtZero: true,
              ticks: {
                callback: (value) => `$${(Number(value) / 1000).toFixed(0)}K`,
              },
            },
          },
        }}
      />
    </div>
  );
}

