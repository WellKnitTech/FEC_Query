import { useEffect, useState } from 'react';
import { analysisApi, CumulativeTotals } from '../services/api';
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
  Filler,
} from 'chart.js';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler
);

import AnalysisSection from './candidate/AnalysisSection';
import { formatCurrency } from '../utils/candidateCalculations';

interface CumulativeChartProps {
  candidateId?: string;
  committeeId?: string;
  minDate?: string;
  maxDate?: string;
  cycle?: number;
}

export default function CumulativeChart({
  candidateId,
  committeeId,
  minDate,
  maxDate,
  cycle,
}: CumulativeChartProps) {
  const [cumulativeTotals, setCumulativeTotals] = useState<CumulativeTotals | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!candidateId && !committeeId) {
      return;
    }

    const abortController = new AbortController();
    
    const fetchTotals = async () => {
      setLoading(true);
      setError(null);
      try {
        const data = await analysisApi.getCumulativeTotals({
          candidate_id: candidateId,
          committee_id: committeeId,
          min_date: minDate,
          max_date: maxDate,
          cycle: cycle,
        }, abortController.signal);
        if (!abortController.signal.aborted) {
          setCumulativeTotals(data);
        }
      } catch (err: any) {
        // Don't set error if request was aborted
        if (err.name === 'AbortError' || abortController.signal.aborted) {
          return;
        }
        const errorMessage = err?.response?.data?.detail || err?.message || 'Failed to load cumulative totals';
        if (!abortController.signal.aborted) {
          setError(errorMessage);
        }
      } finally {
        if (!abortController.signal.aborted) {
          setLoading(false);
        }
      }
    };

    fetchTotals();
    
    return () => {
      abortController.abort();
    };
  }, [candidateId, committeeId, minDate, maxDate, cycle]);

  const refresh = async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await analysisApi.getCumulativeTotals({
        candidate_id: candidateId,
        committee_id: committeeId,
        min_date: minDate,
        max_date: maxDate,
        cycle: cycle,
      });
      setCumulativeTotals(data);
    } catch (err: any) {
      const errorMessage = err?.response?.data?.detail || err?.message || 'Failed to load cumulative totals';
      setError(errorMessage);
    } finally {
      setLoading(false);
    }
  };

  return (
    <AnalysisSection
      title="Cumulative Contributions"
      loading={loading}
      error={error}
      onRetry={refresh}
    >
      {!cumulativeTotals || Object.keys(cumulativeTotals.totals_by_date).length === 0 ? (
        <p className="text-gray-600">No contribution data available</p>
      ) : (() => {
        // Use pre-aggregated data from backend
        const labels = Object.keys(cumulativeTotals.totals_by_date).sort();
        const data = labels.map((date) => cumulativeTotals.totals_by_date[date] || 0);

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

        const totalAmount = cumulativeTotals.total_amount;
        const firstDate = cumulativeTotals.first_date || labels[0] || 'N/A';
        const lastDate = cumulativeTotals.last_date || labels[labels.length - 1] || 'N/A';

        return (
          <>
            <div className="grid grid-cols-3 gap-4 mb-6">
              <div>
                <div className="text-sm text-gray-600">Total Amount</div>
                <div className="text-2xl font-bold">{formatCurrency(totalAmount)}</div>
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
                        const value = context.parsed.y ?? 0;
                        return `Total: $${value.toLocaleString()}`;
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
          </>
        );
      })()}
    </AnalysisSection>
  );
}

