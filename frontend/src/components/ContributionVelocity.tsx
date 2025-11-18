import { useEffect, useState } from 'react';
import { analysisApi, ContributionVelocity } from '../services/api';
import { Line, Bar } from 'react-chartjs-2';
import AnalysisSection from './candidate/AnalysisSection';
import { formatCurrency } from '../utils/candidateCalculations';
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
  Filler,
} from 'chart.js';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Title,
  Tooltip,
  Legend,
  Filler
);

interface ContributionVelocityProps {
  candidateId?: string;
  committeeId?: string;
  minDate?: string;
  maxDate?: string;
  cycle?: number;
}

export default function ContributionVelocityComponent({
  candidateId,
  committeeId,
  minDate,
  maxDate,
  cycle,
}: ContributionVelocityProps) {
  const [velocity, setVelocity] = useState<ContributionVelocity | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!candidateId && !committeeId) {
      return;
    }

    const abortController = new AbortController();
    
    const fetchVelocity = async () => {
      setLoading(true);
      setError(null);
      try {
        const data = await analysisApi.getVelocity({
          candidate_id: candidateId,
          committee_id: committeeId,
          min_date: minDate,
          max_date: maxDate,
        }, abortController.signal);
        if (!abortController.signal.aborted) {
          setVelocity(data);
        }
      } catch (err: any) {
        // Don't set error if request was aborted
        if (err.name === 'AbortError' || abortController.signal.aborted) {
          return;
        }
        const errorMessage = err?.response?.data?.detail || err?.message || 'Failed to load contribution velocity';
        if (!abortController.signal.aborted) {
          setError(errorMessage);
          console.error('Error loading contribution velocity:', err);
        }
      } finally {
        if (!abortController.signal.aborted) {
          setLoading(false);
        }
      }
    };

    fetchVelocity();
    
    return () => {
      abortController.abort();
    };
  }, [candidateId, committeeId, minDate, maxDate, cycle]);

  const refresh = async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await analysisApi.getVelocity({
        candidate_id: candidateId,
        committee_id: committeeId,
        min_date: minDate,
        max_date: maxDate,
      });
      setVelocity(data);
    } catch (err: any) {
      const errorMessage = err?.response?.data?.detail || err?.message || 'Failed to load contribution velocity';
      setError(errorMessage);
      console.error('Error loading contribution velocity:', err);
    } finally {
      setLoading(false);
    }
  };

  if (!velocity && !loading && !error) {
    return null;
  }

  if (!velocity) {
    return (
      <AnalysisSection
        title="Contribution Velocity"
        loading={loading}
        error={error}
        onRetry={refresh}
      >
        <p className="text-gray-600">No velocity data available</p>
      </AnalysisSection>
    );
  }

  // Prepare chart data
  const dateLabels = Object.keys(velocity.velocity_by_date || {}).sort();
  const dateData = dateLabels.map((date) => velocity.velocity_by_date[date] || 0);

  const weekLabels = Object.keys(velocity.velocity_by_week || {}).sort();
  const weekData = weekLabels.map((week) => velocity.velocity_by_week[week] || 0);

  const peakDays = velocity.peak_days || [];
  const peakDaysData = {
    labels: peakDays.map((d) => d.date || 'Unknown'),
    datasets: [
      {
        label: 'Contributions',
        data: peakDays.map((d) => d.count || 0),
        backgroundColor: 'rgba(59, 130, 246, 0.5)',
        borderColor: 'rgba(59, 130, 246, 1)',
        borderWidth: 1,
      },
    ],
  };

  const dailyVelocityData = {
    labels: dateLabels,
    datasets: [
      {
        label: 'Contributions per Day',
        data: dateData,
        borderColor: 'rgba(16, 185, 129, 1)',
        backgroundColor: 'rgba(16, 185, 129, 0.1)',
        tension: 0.1,
        fill: true,
      },
    ],
  };

  const weeklyVelocityData = {
    labels: weekLabels,
    datasets: [
      {
        label: 'Contributions per Week',
        data: weekData,
        borderColor: 'rgba(139, 92, 246, 1)',
        backgroundColor: 'rgba(139, 92, 246, 0.1)',
        tension: 0.1,
        fill: true,
      },
    ],
  };

  return (
    <AnalysisSection
      title="Contribution Velocity"
      loading={loading}
      error={error}
      onRetry={refresh}
    >
      <div className="space-y-6">
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
          <div>
            <div className="text-sm text-gray-600">Average Daily Velocity</div>
            <div className="text-2xl font-bold">{velocity.average_daily_velocity.toFixed(1)}</div>
            <div className="text-xs text-gray-500">contributions/day</div>
          </div>
          <div>
            <div className="text-sm text-gray-600">Days Tracked</div>
            <div className="text-2xl font-bold">{dateLabels.length}</div>
          </div>
          <div>
            <div className="text-sm text-gray-600">Weeks Tracked</div>
            <div className="text-2xl font-bold">{weekLabels.length}</div>
          </div>
          <div>
            <div className="text-sm text-gray-600">Peak Days</div>
            <div className="text-2xl font-bold">{velocity.peak_days.length}</div>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="bg-white rounded-lg shadow p-6">
          <h3 className="text-lg font-semibold mb-4">Daily Velocity</h3>
          <Line data={dailyVelocityData} options={{ responsive: true, maintainAspectRatio: true }} />
        </div>

        <div className="bg-white rounded-lg shadow p-6">
          <h3 className="text-lg font-semibold mb-4">Weekly Velocity</h3>
          <Line data={weeklyVelocityData} options={{ responsive: true, maintainAspectRatio: true }} />
        </div>

        <div className="bg-white rounded-lg shadow p-6 lg:col-span-2">
          <h3 className="text-lg font-semibold mb-4">Peak Contribution Days</h3>
          <Bar data={peakDaysData} options={{ responsive: true, maintainAspectRatio: true }} />
          <div className="mt-4 text-sm text-gray-600">
            <p className="font-semibold mb-2">Top Peak Days:</p>
            <div className="grid grid-cols-2 md:grid-cols-5 gap-2">
              {peakDays.slice(0, 10).map((day, idx) => (
                <div key={idx} className="p-2 bg-gray-50 rounded">
                  <div className="font-medium text-xs">{day.date || 'Unknown'}</div>
                  <div className="text-xs text-gray-600">
                    {day.count || 0} contributions
                    <br />
                    {formatCurrency(day.amount || 0)}
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </AnalysisSection>
  );
}

