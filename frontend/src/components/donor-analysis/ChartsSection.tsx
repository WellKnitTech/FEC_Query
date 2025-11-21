import { Contribution } from '../../services/api';
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
import {
  ChartData,
  ContributionsByState,
  AmountDistribution,
} from '../../utils/donorAnalysisUtils';

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

interface ChartsSectionProps {
  contributions: Contribution[];
  contributionsByState: ContributionsByState;
  chartData: ChartData | null;
  amountDistribution: AmountDistribution | null;
  contributionFrequency: number | null;
  averageContribution: number;
  uniqueCommittees: number;
  totalAmount: number;
  viewAggregated: boolean;
}

export default function ChartsSection({
  contributions,
  contributionsByState,
  chartData,
  amountDistribution,
  contributionFrequency,
  averageContribution,
  uniqueCommittees,
  totalAmount,
  viewAggregated,
}: ChartsSectionProps) {
  // Process employer breakdown
  const topEmployers = Object.entries(
    contributions.reduce((acc, c) => {
      if (c.contributor_employer) {
        const employer = c.contributor_employer;
        if (!acc[employer]) {
          acc[employer] = { amount: 0, count: 0 };
        }
        acc[employer].amount += c.contribution_amount || 0;
        acc[employer].count += 1;
      }
      return acc;
    }, {} as Record<string, { amount: number; count: number }>)
  )
    .map(([employer, data]) => ({ employer, ...data }))
    .sort((a, b) => b.amount - a.amount)
    .slice(0, 10);

  // Process velocity data
  const velocityData = contributions.reduce((acc, c) => {
    if (c.contribution_date) {
      const date = c.contribution_date.split('T')[0];
      if (!acc[date]) acc[date] = 0;
      acc[date] += c.contribution_amount || 0;
    }
    return acc;
  }, {} as Record<string, number>);

  const velocityLabels = Object.keys(velocityData).sort();
  const velocityValues = velocityLabels.map((date) => velocityData[date]);

  return (
    <div className="space-y-6">
      {/* Time Series Chart - Only show for individual contributions */}
      {!viewAggregated && chartData && (
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

      {/* Employer Breakdown */}
      {topEmployers.length > 0 && (
        <div className="bg-white rounded-lg shadow p-6">
          <h2 className="text-xl font-semibold mb-4">Top Employers</h2>
          <div className="space-y-2">
            {topEmployers.map((item, idx) => (
              <div
                key={item.employer}
                className="flex justify-between items-center p-3 border border-gray-200 rounded-lg"
              >
                <div>
                  <div className="font-medium">
                    #{idx + 1} {item.employer}
                  </div>
                  <div className="text-sm text-gray-600">
                    {item.count} contribution{item.count !== 1 ? 's' : ''}
                  </div>
                </div>
                <div className="text-lg font-bold text-blue-600">
                  ${item.amount.toLocaleString()}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Geographic Breakdown */}
      {Object.keys(contributionsByState).length > 0 && (
        <div className="bg-white rounded-lg shadow p-6">
          <h2 className="text-xl font-semibold mb-4">Contributions by State</h2>
          <div className="mb-4">
            <Bar
              data={{
                labels: Object.entries(contributionsByState)
                  .sort((a, b) => b[1].amount - a[1].amount)
                  .slice(0, 15)
                  .map(([state]) => state),
                datasets: [
                  {
                    label: 'Total Contributions',
                    data: Object.entries(contributionsByState)
                      .sort((a, b) => b[1].amount - a[1].amount)
                      .slice(0, 15)
                      .map(([, data]) => data.amount),
                    backgroundColor: 'rgba(59, 130, 246, 0.6)',
                  },
                ],
              }}
              options={{
                responsive: true,
                plugins: {
                  legend: { display: false },
                  tooltip: {
                    callbacks: {
                      label: (context) => {
                        const state = context.label;
                        const data = contributionsByState[state];
                        return [
                          `State: ${state}`,
                          `Amount: $${data.amount.toLocaleString()}`,
                          `Count: ${data.count} contributions`,
                          `Percentage: ${((data.amount / totalAmount) * 100).toFixed(1)}%`,
                        ];
                      },
                    },
                  },
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
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {Object.entries(contributionsByState)
              .sort((a, b) => b[1].amount - a[1].amount)
              .slice(0, 12)
              .map(([state, data]) => (
                <div
                  key={state}
                  className="p-4 border border-gray-200 rounded-lg hover:bg-gray-50 cursor-pointer"
                >
                  <div className="font-semibold text-gray-900">{state}</div>
                  <div className="text-2xl font-bold text-blue-600 mt-1">
                    ${data.amount.toLocaleString()}
                  </div>
                  <div className="text-sm text-gray-600 mt-1">
                    {data.count} contribution{data.count !== 1 ? 's' : ''}
                  </div>
                  <div className="text-xs text-gray-500 mt-1">
                    {((data.amount / totalAmount) * 100).toFixed(1)}% of total
                  </div>
                </div>
              ))}
          </div>
        </div>
      )}

      {/* Contribution Velocity - Only show for individual contributions */}
      {!viewAggregated && contributions.length > 0 && velocityLabels.length > 0 && (
        <div className="bg-white rounded-lg shadow p-6">
          <h2 className="text-xl font-semibold mb-4">Contribution Velocity</h2>
          <div className="h-64">
            <Line
              data={{
                labels: velocityLabels,
                datasets: [
                  {
                    label: 'Daily Contributions',
                    data: velocityValues,
                    borderColor: 'rgb(16, 185, 129)',
                    backgroundColor: 'rgba(16, 185, 129, 0.1)',
                    tension: 0.1,
                  },
                ],
              }}
              options={{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                  legend: { display: true },
                  tooltip: {
                    callbacks: {
                      label: (context) => `$${(context.parsed.y || 0).toLocaleString()}`,
                    },
                  },
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
    </div>
  );
}

