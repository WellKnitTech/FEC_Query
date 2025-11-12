import { useEffect, useState } from 'react';
import { contributionApi, Contribution } from '../services/api';
import { Scatter } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  LinearScale,
  PointElement,
  LineElement,
  Tooltip,
  Legend,
} from 'chart.js';

ChartJS.register(LinearScale, PointElement, LineElement, Tooltip, Legend);

interface SmurfingScatterProps {
  candidateId?: string;
  committeeId?: string;
  minDate?: string;
  maxDate?: string;
}

export default function SmurfingScatter({
  candidateId,
  committeeId,
  minDate,
  maxDate,
}: SmurfingScatterProps) {
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
      } catch (err) {
        setError('Failed to load contributions');
        console.error(err);
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
        <div className="text-red-600">{error}</div>
      </div>
    );
  }

  if (contributions.length === 0) {
    return (
      <div className="bg-white rounded-lg shadow p-6">
        <h2 className="text-xl font-semibold mb-4">Smurfing Detection Visualization</h2>
        <p className="text-gray-600">No contribution data available</p>
      </div>
    );
  }

  // Filter contributions in smurfing range (just under $200 threshold)
  const smurfingThreshold = 200;
  const smurfingMin = 190;
  const suspiciousContributions = contributions.filter(
    (c) => c.contribution_amount >= smurfingMin && c.contribution_amount < smurfingThreshold
  );

  // Calculate similarity groups (simplified - group by similar names)
  const similarityGroups: Map<string, Contribution[]> = new Map();
  const processed = new Set<number>();

  for (let i = 0; i < suspiciousContributions.length; i++) {
    if (processed.has(i)) continue;
    const contrib = suspiciousContributions[i];
    const key = contrib.contributor_name?.toLowerCase().trim() || 'unknown';
    
    if (!similarityGroups.has(key)) {
      similarityGroups.set(key, []);
    }
    similarityGroups.get(key)!.push(contrib);
    processed.add(i);

    // Find similar contributions
    for (let j = i + 1; j < suspiciousContributions.length; j++) {
      if (processed.has(j)) continue;
      const other = suspiciousContributions[j];
      const otherKey = other.contributor_name?.toLowerCase().trim() || 'unknown';
      
      // Simple similarity check (same name or very similar)
      if (key === otherKey || (key.length > 5 && otherKey.length > 5 && 
          (key.includes(otherKey.substring(0, 5)) || otherKey.includes(key.substring(0, 5))))) {
        similarityGroups.get(key)!.push(other);
        processed.add(j);
      }
    }
  }

  // Prepare scatter plot data
  const scatterData: { x: number; y: number; label: string; group: string }[] = [];
  const colors = [
    'rgba(239, 68, 68, 0.6)',
    'rgba(245, 158, 11, 0.6)',
    'rgba(251, 191, 36, 0.6)',
    'rgba(34, 197, 94, 0.6)',
    'rgba(59, 130, 246, 0.6)',
    'rgba(139, 92, 246, 0.6)',
    'rgba(236, 72, 153, 0.6)',
  ];

  let colorIndex = 0;
  const groupColors: Map<string, string> = new Map();

  for (const [groupName, groupContribs] of similarityGroups.entries()) {
    if (groupContribs.length >= 3) {
      // Only show groups with 3+ contributions (potential smurfing)
      if (!groupColors.has(groupName)) {
        groupColors.set(groupName, colors[colorIndex % colors.length]);
        colorIndex++;
      }
      const color = groupColors.get(groupName)!;

      for (const contrib of groupContribs) {
        if (contrib.contribution_date) {
          const date = new Date(contrib.contribution_date);
          scatterData.push({
            x: date.getTime(),
            y: contrib.contribution_amount,
            label: contrib.contributor_name || 'Unknown',
            group: groupName,
          });
        }
      }
    }
  }

  // Create datasets for each similarity group
  const datasets = Array.from(similarityGroups.entries())
    .filter(([_, contribs]) => contribs.length >= 3)
    .map(([groupName, contribs]) => {
      const color = groupColors.get(groupName) || 'rgba(128, 128, 128, 0.6)';
      const data = contribs
        .filter((c) => c.contribution_date)
        .map((c) => ({
          x: new Date(c.contribution_date!).getTime(),
          y: c.contribution_amount,
        }));

      return {
        label: `${groupName} (${contribs.length} contributions)`,
        data: data,
        backgroundColor: color,
        borderColor: color.replace('0.6', '1'),
        pointRadius: 6,
        pointHoverRadius: 8,
      };
    });

  const chartData = {
    datasets: datasets,
  };

  const smurfingCount = suspiciousContributions.length;
  const groupCount = Array.from(similarityGroups.values()).filter((g) => g.length >= 3).length;

  return (
    <div className="bg-white rounded-lg shadow p-6">
      <h2 className="text-xl font-semibold mb-4">Smurfing Detection Visualization</h2>
      <div className="mb-4">
        <div className="grid grid-cols-3 gap-4 mb-4">
          <div>
            <div className="text-sm text-gray-600">Contributions $190-$199</div>
            <div className="text-2xl font-bold">{smurfingCount}</div>
          </div>
          <div>
            <div className="text-sm text-gray-600">Suspicious Groups</div>
            <div className="text-2xl font-bold">{groupCount}</div>
            <div className="text-xs text-gray-500">(3+ contributions)</div>
          </div>
          <div>
            <div className="text-sm text-gray-600">Total Suspicious Amount</div>
            <div className="text-2xl font-bold">
              ${(suspiciousContributions.reduce((sum, c) => sum + c.contribution_amount, 0) / 1000).toFixed(1)}K
            </div>
          </div>
        </div>
        <div className="p-3 bg-yellow-50 border border-yellow-200 rounded text-sm text-yellow-800">
          <strong>Note:</strong> This visualization shows contributions just under the $200 reporting threshold.
          Groups with 3+ similar contributions may indicate smurfing patterns.
        </div>
      </div>
      {datasets.length > 0 ? (
        <Scatter
          data={chartData}
          options={{
            responsive: true,
            maintainAspectRatio: true,
            scales: {
              x: {
                type: 'linear',
                position: 'bottom',
                title: {
                  display: true,
                  text: 'Date',
                },
                ticks: {
                  callback: (value) => {
                    const date = new Date(value as number);
                    return date.toLocaleDateString();
                  },
                },
              },
              y: {
                title: {
                  display: true,
                  text: 'Contribution Amount ($)',
                },
                min: smurfingMin,
                max: smurfingThreshold,
              },
            },
            plugins: {
              tooltip: {
                callbacks: {
                  label: (context) => {
                    const point = context.raw as { x: number; y: number };
                    const date = new Date(point.x);
                    return [
                      `Date: ${date.toLocaleDateString()}`,
                      `Amount: $${point.y.toFixed(2)}`,
                      `Group: ${context.dataset.label}`,
                    ];
                  },
                },
              },
            },
          }}
        />
      ) : (
        <div className="text-center py-8 text-gray-600">
          <p>No suspicious smurfing patterns detected (no groups with 3+ similar contributions)</p>
        </div>
      )}
    </div>
  );
}

