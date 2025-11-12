import { useEffect, useState } from 'react';
import { analysisApi, ContributionVelocity } from '../services/api';
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

interface ContributionVelocityProps {
  candidateId?: string;
  committeeId?: string;
  minDate?: string;
  maxDate?: string;
}

export default function ContributionVelocityComponent({
  candidateId,
  committeeId,
  minDate,
  maxDate,
}: ContributionVelocityProps) {
  const [velocity, setVelocity] = useState<ContributionVelocity | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchVelocity = async () => {
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
      } catch (err) {
        setError('Failed to load contribution velocity');
        console.error(err);
      } finally {
        setLoading(false);
      }
    };

    if (candidateId || committeeId) {
      fetchVelocity();
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

  if (!velocity) {
    return null;
  }

  // Prepare chart data
  const dateLabels = Object.keys(velocity.velocity_by_date).sort();
  const dateData = dateLabels.map((date) => velocity.velocity_by_date[date]);

  const weekLabels = Object.keys(velocity.velocity_by_week).sort();
  const weekData = weekLabels.map((week) => velocity.velocity_by_week[week]);

  const peakDaysData = {
    labels: velocity.peak_days.map((d) => d.date),
    datasets: [
      {
        label: 'Contributions',
        data: velocity.peak_days.map((d) => d.count),
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
    <div className="space-y-6">
      <div className="bg-white rounded-lg shadow p-6">
        <h2 className="text-xl font-semibold mb-4">Contribution Velocity</h2>
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
              {velocity.peak_days.slice(0, 10).map((day, idx) => (
                <div key={idx} className="p-2 bg-gray-50 rounded">
                  <div className="font-medium text-xs">{day.date}</div>
                  <div className="text-xs text-gray-600">
                    {day.count} contributions
                    <br />
                    ${(day.amount / 1000).toFixed(1)}K
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

