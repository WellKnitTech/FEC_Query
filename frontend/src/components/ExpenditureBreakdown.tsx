import { useEffect, useState } from 'react';
import { analysisApi, ExpenditureBreakdown } from '../services/api';
import { Pie, Bar, Line } from 'react-chartjs-2';
import AnalysisSection from './candidate/AnalysisSection';
import { formatCurrency } from '../utils/candidateCalculations';
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

interface ExpenditureBreakdownProps {
  candidateId?: string;
  committeeId?: string;
  minDate?: string;
  maxDate?: string;
  cycle?: number;
}

export default function ExpenditureBreakdownComponent({
  candidateId,
  committeeId,
  minDate,
  maxDate,
  cycle,
}: ExpenditureBreakdownProps) {
  const [breakdown, setBreakdown] = useState<ExpenditureBreakdown | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!candidateId && !committeeId) {
      return;
    }

    const abortController = new AbortController();
    
    const fetchBreakdown = async () => {
      setLoading(true);
      setError(null);
      try {
        const data = await analysisApi.getExpenditureBreakdown({
          candidate_id: candidateId,
          committee_id: committeeId,
          min_date: minDate,
          max_date: maxDate,
        }, abortController.signal);
        if (!abortController.signal.aborted) {
          setBreakdown(data);
        }
      } catch (err: any) {
        // Don't set error if request was aborted
        if (err.name === 'AbortError' || abortController.signal.aborted) {
          return;
        }
        const errorMessage = err?.response?.data?.detail || err?.message || 'Failed to load expenditure breakdown';
        if (!abortController.signal.aborted) {
          setError(errorMessage);
        }
      } finally {
        if (!abortController.signal.aborted) {
          setLoading(false);
        }
      }
    };

    fetchBreakdown();
    
    return () => {
      abortController.abort();
    };
  }, [candidateId, committeeId, minDate, maxDate, cycle]);

  const refresh = async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await analysisApi.getExpenditureBreakdown({
        candidate_id: candidateId,
        committee_id: committeeId,
        min_date: minDate,
        max_date: maxDate,
      });
      setBreakdown(data);
    } catch (err: any) {
      const errorMessage = err?.response?.data?.detail || err?.message || 'Failed to load expenditure breakdown';
      setError(errorMessage);
    } finally {
      setLoading(false);
    }
  };

  if (!breakdown && !loading && !error) {
    return null;
  }

  if (!breakdown) {
    return (
      <AnalysisSection
        title="Expenditure Analysis"
        loading={loading}
        error={error}
        onRetry={refresh}
      >
        <p className="text-gray-600">No expenditure data available</p>
      </AnalysisSection>
    );
  }

  // Prepare chart data
  const categoryLabels = Object.keys(breakdown.expenditures_by_category || {});
  const categoryData = Object.values(breakdown.expenditures_by_category || {});

  const dateLabels = Object.keys(breakdown.expenditures_by_date || {}).sort();
  const dateData = dateLabels.map((date) => breakdown.expenditures_by_date[date] || 0);

  const topRecipients = breakdown.top_recipients || [];
  const topRecipientsData = {
    labels: topRecipients.slice(0, 10).map((r) => r.name || 'Unknown'),
    datasets: [
      {
        label: 'Total Expenditures',
        data: topRecipients.slice(0, 10).map((r) => r.total || 0),
        backgroundColor: 'rgba(239, 68, 68, 0.5)',
        borderColor: 'rgba(239, 68, 68, 1)',
        borderWidth: 1,
      },
    ],
  };

  const categoryChartData = {
    labels: categoryLabels,
    datasets: [
      {
        data: categoryData,
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
        ],
        borderWidth: 1,
      },
    ],
  };

  const dateChartData = {
    labels: dateLabels,
    datasets: [
      {
        label: 'Expenditures by Date',
        data: dateData,
        borderColor: 'rgba(239, 68, 68, 1)',
        backgroundColor: 'rgba(239, 68, 68, 0.1)',
        tension: 0.1,
      },
    ],
  };

  return (
    <AnalysisSection
      title="Expenditure Analysis"
      loading={loading}
      error={error}
      onRetry={refresh}
    >
      <div className="space-y-6">
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
          <div>
            <div className="text-sm text-gray-600">Total Expenditures</div>
            <div className="text-2xl font-bold">
              {formatCurrency(breakdown.total_expenditures)}
            </div>
          </div>
          <div>
            <div className="text-sm text-gray-600">Total Transactions</div>
            <div className="text-2xl font-bold">{breakdown.total_transactions.toLocaleString()}</div>
          </div>
          <div>
            <div className="text-sm text-gray-600">Average Expenditure</div>
            <div className="text-2xl font-bold">${breakdown.average_expenditure.toFixed(2)}</div>
          </div>
          <div>
            <div className="text-sm text-gray-600">Categories</div>
            <div className="text-2xl font-bold">{categoryLabels.length}</div>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="bg-white rounded-lg shadow p-6">
          <h3 className="text-lg font-semibold mb-4">Expenditures by Category</h3>
          <Pie data={categoryChartData} options={{ responsive: true, maintainAspectRatio: true }} />
        </div>

        <div className="bg-white rounded-lg shadow p-6">
          <h3 className="text-lg font-semibold mb-4">Expenditures Over Time</h3>
          <Line data={dateChartData} options={{ responsive: true, maintainAspectRatio: true }} />
        </div>

        <div className="bg-white rounded-lg shadow p-6 lg:col-span-2">
          <h3 className="text-lg font-semibold mb-4">Top Recipients</h3>
          <Bar
            data={topRecipientsData}
            options={{ responsive: true, maintainAspectRatio: true, indexAxis: 'y' }}
          />
        </div>
      </div>
    </AnalysisSection>
  );
}

